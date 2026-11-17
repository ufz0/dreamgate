use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{MatchedPath, Request, State as AxumState};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::db::{self, Probe};
use crate::registry;
use crate::shutdown::Phase;
use crate::state::State;
use crate::usage::UsageRow;

// The type an answer was given, set by `api::error` on the response itself so that what counts the answers
// does not have to read the body back to know why a request was refused.
#[derive(Clone)]
pub struct ErrorType(pub String);

// Upper bounds, in seconds, of the buckets a recorded request's latency is counted in. A single answer is
// usually under a second and a long stream can run for minutes, so the buckets span both.
const BUCKETS: [f64; 11] = [
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

// What the gateway counts about itself for a scraper, in the process and not in the database: it is a
// reading of this run, starts at zero on a restart the way a Prometheus counter is expected to, and costs a
// few atomic additions per request. The two maps are behind plain mutexes, held for one insert or for one
// read of the counts, and never across an await. Nothing here is labelled by key or account: names are shown
// to an admin alone, and a label per key would grow without bound.
#[derive(Default)]
pub struct Metrics {
    // The token a scrape has to send. Without one the route is not there at all, because the gateway binds
    // every interface by default and counts per connection are not for anyone who can reach it.
    token: Option<String>,
    responses: Mutex<BTreeMap<(String, u16, String), u64>>,
    upstream_failures: Mutex<BTreeMap<String, u64>>,
    // Input and output, each as the upstream reported it or as the gateway estimated it, kept apart so a
    // dashboard never adds an estimate to a reported count without saying so.
    tokens: [[AtomicU64; 2]; 2],
    usage_write_failures: AtomicU64,
    stream_stalls: AtomicU64,
    // The latency of each recorded request, single answers and streams apart.
    latency: [Histogram; 2],
    // When the health check first found every pooled connection in use, in the run of checks that have
    // found nothing else since. Held for one read and one write, never across an await.
    busy_since: Mutex<Option<Instant>>,
}

// Each request is counted in the one bucket it falls in, the last being everything above the largest bound,
// and the cumulative counts are summed when a scrape is written. Counted straight into cumulative buckets,
// a scrape that read them while a request was being counted could show a bucket above the next one.
#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS.len() + 1],
    sum_ms: AtomicU64,
}

impl Metrics {
    // A token is read as it is set, without the line break or spaces a secret file or an environment file
    // tends to leave around it; one that is empty once they are gone is no token.
    pub fn new(token: Option<String>) -> Metrics {
        Metrics {
            token: token
                .map(|token| token.trim().to_string())
                .filter(|token| !token.is_empty()),
            ..Default::default()
        }
    }

    pub fn response(&self, route: &str, status: u16, kind: &str) {
        let mut responses = self.responses.lock().expect("response counts");
        *responses
            .entry((route.to_string(), status, kind.to_string()))
            .or_default() += 1;
    }

    pub fn upstream_failure(&self, connection: &str) {
        let mut failures = self.upstream_failures.lock().expect("upstream failures");
        *failures.entry(connection.to_string()).or_default() += 1;
    }

    pub fn stream_stall(&self) {
        self.stream_stalls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn usage_write_failure(&self) {
        self.usage_write_failures.fetch_add(1, Ordering::Relaxed);
    }

    // Counted whether or not the row could be written, because the tokens were used either way; a row that
    // was lost is counted on its own as well.
    pub fn recorded(&self, row: &UsageRow) {
        let source = usize::from(row.estimated);
        self.tokens[0][source].fetch_add(row.prompt_tokens.max(0) as u64, Ordering::Relaxed);
        self.tokens[1][source].fetch_add(row.completion_tokens.max(0) as u64, Ordering::Relaxed);
        let histogram = &self.latency[usize::from(row.streamed)];
        let seconds = row.latency_ms.max(0) as f64 / 1000.0;
        let bucket = BUCKETS
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(BUCKETS.len());
        histogram.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        histogram
            .sum_ms
            .fetch_add(row.latency_ms.max(0) as u64, Ordering::Relaxed);
    }
}

// Counts every answer the gateway endpoints give, by route, status and the type an error carried, so a
// refusal that is deliberately not a usage row (a limit, a full connection, a key that does not exist) is
// still counted somewhere. A stream is counted when its head is sent; how it ended is in the stall count and
// the recorded rows.
pub async fn count(AxumState(st): AxumState<State>, request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let response = next.run(request).await;
    let kind = response
        .extensions()
        .get::<ErrorType>()
        .map(|kind| kind.0.as_str())
        .unwrap_or("");
    st.metrics
        .response(&route, response.status().as_u16(), kind);
    response
}

// One line per request, written as a single event of its own target, `dreamgate::access`, so a `RUST_LOG`
// that filters other targets cannot leave the line without its method and path, and the line can be turned
// off on its own (`dreamgate::access=off`). Nothing from the headers is written, so a key or a session
// cookie never reaches the log, and the path is written without its query. For a stream the time is the time
// to its head, because that is when the answer is handed on; the stream's own length is in its usage row.
pub async fn access_log(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let started = Instant::now();
    let response = next.run(request).await;
    tracing::info!(
        target: "dreamgate::access",
        method = %method,
        path = %path,
        status = response.status().as_u16(),
        latency_ms = started.elapsed().as_millis() as u64,
        "request"
    );
    response
}

pub async fn scrape(AxumState(st): AxumState<State>, headers: HeaderMap) -> Response {
    let Some(token) = st.metrics.token.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let sent = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);
    if !sent.is_some_and(|sent| same(sent, token)) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let mut out = render(&st);
    // The connections' health is a reading the checks keep in the database, so it is read for the scrape.
    // A database that does not answer within a second leaves those families out rather than holding the
    // scrape past a scraper's own timeout, which would lose every other count at the moment they matter.
    match tokio::time::timeout(Duration::from_secs(1), registry::health_readings(&st)).await {
        Ok(Ok(readings)) => connection_families(&mut out, &st, &readings),
        Ok(Err(err)) => {
            tracing::warn!(
                "a scrape left the connections out, because they could not be read: {err}"
            )
        }
        Err(_) => tracing::warn!(
            "a scrape left the connections out, because the database did not answer within a second"
        ),
    }
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        Body::from(out),
    )
        .into_response()
}

// Compared without stopping at the first byte that differs, so the time an answer takes does not say how
// much of a guessed token was right.
fn same(sent: &str, token: &str) -> bool {
    sent.len() == token.len()
        && sent
            .bytes()
            .zip(token.bytes())
            .fold(0u8, |differ, (a, b)| differ | (a ^ b))
            == 0
}

fn connection_families(out: &mut String, st: &State, readings: &[(i64, String, String)]) {
    family(
        out,
        "dreamgate_connection_up",
        "gauge",
        "1 when the last health check reached an enabled connection, 0 when it failed; a connection no check has reached yet is left out",
    );
    for (_, name, health) in readings {
        let up = match health.as_str() {
            "ok" => 1,
            "down" => 0,
            _ => continue,
        };
        let _ = writeln!(
            out,
            "dreamgate_connection_up{{connection=\"{}\"}} {up}",
            escape(name)
        );
    }
    family(
        out,
        "dreamgate_connection_in_flight",
        "gauge",
        "Completion requests an enabled connection is working on now",
    );
    for (id, name, _) in readings {
        if let Some(in_flight) = st.limits.connection_in_flight(*id) {
            let _ = writeln!(
                out,
                "dreamgate_connection_in_flight{{connection=\"{}\"}} {in_flight}",
                escape(name)
            );
        }
    }
}

fn render(st: &State) -> String {
    let m = &st.metrics;
    let mut out = String::new();

    family(
        &mut out,
        "dreamgate_responses_total",
        "counter",
        "Answers given by the gateway endpoints, by route, status and the type of error they carried",
    );
    for ((route, status, kind), n) in m.responses.lock().expect("response counts").iter() {
        let _ = writeln!(
            out,
            "dreamgate_responses_total{{route=\"{}\",status=\"{status}\",type=\"{}\"}} {n}",
            escape(route),
            escape(kind)
        );
    }

    family(
        &mut out,
        "dreamgate_upstream_failures_total",
        "counter",
        "Requests to a connection that failed before or while it answered, a stream cut midway included, by connection",
    );
    for (connection, n) in m
        .upstream_failures
        .lock()
        .expect("upstream failures")
        .iter()
    {
        let _ = writeln!(
            out,
            "dreamgate_upstream_failures_total{{connection=\"{}\"}} {n}",
            escape(connection)
        );
    }

    family(
        &mut out,
        "dreamgate_tokens_total",
        "counter",
        "Tokens in recorded requests, by side and by whether the upstream reported them or the gateway estimated them",
    );
    for (d, direction) in ["input", "output"].iter().enumerate() {
        for (s, source) in ["reported", "estimated"].iter().enumerate() {
            let _ = writeln!(
                out,
                "dreamgate_tokens_total{{direction=\"{direction}\",source=\"{source}\"}} {}",
                m.tokens[d][s].load(Ordering::Relaxed)
            );
        }
    }

    family(
        &mut out,
        "dreamgate_request_duration_seconds",
        "histogram",
        "Latency of recorded requests, single answers and streams apart",
    );
    for (streamed, histogram) in m.latency.iter().enumerate() {
        let streamed = streamed == 1;
        // The counts are read once each and summed here, so the buckets, `+Inf` and `_count` of one scrape
        // agree with each other however many requests are being counted while it is written.
        let counts: Vec<u64> = histogram
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        let mut cumulative = 0;
        for (count, bound) in counts.iter().zip(BUCKETS) {
            cumulative += count;
            let _ = writeln!(
                out,
                "dreamgate_request_duration_seconds_bucket{{streamed=\"{streamed}\",le=\"{bound}\"}} {cumulative}"
            );
        }
        let total: u64 = counts.iter().sum();
        let _ = writeln!(
            out,
            "dreamgate_request_duration_seconds_bucket{{streamed=\"{streamed}\",le=\"+Inf\"}} {total}"
        );
        let _ = writeln!(
            out,
            "dreamgate_request_duration_seconds_sum{{streamed=\"{streamed}\"}} {}",
            histogram.sum_ms.load(Ordering::Relaxed) as f64 / 1000.0
        );
        let _ = writeln!(
            out,
            "dreamgate_request_duration_seconds_count{{streamed=\"{streamed}\"}} {total}"
        );
    }

    family(
        &mut out,
        "dreamgate_usage_write_failures_total",
        "counter",
        "Usage rows that could not be written, which the key caps and daily limits then do not count",
    );
    let _ = writeln!(
        out,
        "dreamgate_usage_write_failures_total {}",
        m.usage_write_failures.load(Ordering::Relaxed)
    );

    family(
        &mut out,
        "dreamgate_stream_stalls_total",
        "counter",
        "Streams closed because the upstream went quiet for the stall window",
    );
    let _ = writeln!(
        out,
        "dreamgate_stream_stalls_total {}",
        m.stream_stalls.load(Ordering::Relaxed)
    );

    family(
        &mut out,
        "dreamgate_in_flight",
        "gauge",
        "Completion requests the gateway has admitted and not yet finished, across every connection",
    );
    let _ = writeln!(out, "dreamgate_in_flight {}", st.limits.in_flight());
    family(
        &mut out,
        "dreamgate_in_flight_limit",
        "gauge",
        "How many completion requests the gateway admits at once",
    );
    let _ = writeln!(
        out,
        "dreamgate_in_flight_limit {}",
        st.limits.global_limit()
    );
    out
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

// A label value is written between double quotes, so a quote, a backslash or a line break in a connection's
// name would end it early; the exposition format escapes those three.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

// The answer for a load balancer or an orchestrator: whether this process can do its work, which needs the
// database. It does not ask the connections, because an upstream that is down is not a reason to restart the
// gateway in front of it. A pool whose every connection is in use is a burst being served, not a database
// that has stopped, so it answers `busy` with a 200 rather than a 503 that would get a working gateway
// restarted in the middle of its streams; but only for as long as a query waits for a connection. A pool
// that has given none for longer is one whose connections are stuck, on a disk that hangs for instance, and
// the gateway's own requests are already failing with it, so the check fails as well.
pub async fn health(AxumState(st): AxumState<State>) -> Response {
    // A process that is stopping says so first, so a load balancer stops sending it requests it would
    // turn away.
    if st.shutdown.phase() != Phase::Running {
        return (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response();
    }
    let probe = db::probe(&st.pool).await;
    let busy_for = {
        let mut since = st.metrics.busy_since.lock().expect("busy since");
        match probe {
            Probe::Busy => Some(since.get_or_insert_with(Instant::now).elapsed()),
            _ => {
                *since = None;
                None
            }
        }
    };
    match probe {
        Probe::Answered => (StatusCode::OK, "ok").into_response(),
        Probe::Busy => {
            let busy_for = busy_for.unwrap_or_default();
            if busy_for < st.pool.options().get_acquire_timeout() {
                return (StatusCode::OK, "busy").into_response();
            }
            tracing::error!(
                "health check: every connection of the pool has been in use for {} seconds",
                busy_for.as_secs()
            );
            (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response()
        }
        Probe::Failed(err) => {
            tracing::error!("health check could not read the database: {err}");
            (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response()
        }
        Probe::TimedOut => {
            tracing::error!("health check: the database did not answer within a second");
            (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response()
        }
    }
}
