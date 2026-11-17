use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{MatchedPath, Request, State as AxumState};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use sqlx::Row;

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
// few atomic additions per request. The two maps are behind plain mutexes, held for one insert and never
// across an await. Nothing here is labelled by key or account: names are shown to an admin alone, and a
// label per key would grow without bound.
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
}

#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    sum_ms: AtomicU64,
}

impl Metrics {
    pub fn new(token: Option<String>) -> Metrics {
        Metrics {
            token: token.filter(|token| !token.trim().is_empty()),
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
        for (bucket, bound) in histogram.buckets.iter().zip(BUCKETS) {
            if seconds <= bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        histogram.count.fetch_add(1, Ordering::Relaxed);
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

pub async fn scrape(AxumState(st): AxumState<State>, headers: HeaderMap) -> Response {
    let Some(token) = st.metrics.token.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let sent = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if sent.map(str::trim) != Some(token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let connections = sqlx::query("SELECT id, name, health FROM connections ORDER BY id")
        .fetch_all(&st.pool)
        .await;
    let mut out = render(&st);
    // The connections are read for the scrape rather than counted, because their health is a reading the
    // checks keep in the database. A database that cannot be read leaves them out rather than reporting
    // every connection as down.
    if let Ok(rows) = connections {
        family(
            &mut out,
            "dreamgate_connection_up",
            "gauge",
            "1 when the last health check reached the connection, 0 when it failed; a connection no check has reached yet is left out",
        );
        for row in &rows {
            let name = row.get::<String, _>("name");
            let up = match row.get::<String, _>("health").as_str() {
                "ok" => 1,
                "down" => 0,
                _ => continue,
            };
            let _ = writeln!(
                out,
                "dreamgate_connection_up{{connection=\"{}\"}} {up}",
                escape(&name)
            );
        }
        family(
            &mut out,
            "dreamgate_connection_in_flight",
            "gauge",
            "Requests a connection is working on now",
        );
        for row in &rows {
            let id = row.get::<i64, _>("id");
            if let Some(in_flight) = st.limits.connection_in_flight(id) {
                let _ = writeln!(
                    out,
                    "dreamgate_connection_in_flight{{connection=\"{}\"}} {in_flight}",
                    escape(&row.get::<String, _>("name"))
                );
            }
        }
    }
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        Body::from(out),
    )
        .into_response()
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
        "Requests to a connection that failed before it answered, by connection",
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
        for (bucket, bound) in histogram.buckets.iter().zip(BUCKETS) {
            let _ = writeln!(
                out,
                "dreamgate_request_duration_seconds_bucket{{streamed=\"{streamed}\",le=\"{bound}\"}} {}",
                bucket.load(Ordering::Relaxed)
            );
        }
        let count = histogram.count.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "dreamgate_request_duration_seconds_bucket{{streamed=\"{streamed}\",le=\"+Inf\"}} {count}"
        );
        let _ = writeln!(
            out,
            "dreamgate_request_duration_seconds_sum{{streamed=\"{streamed}\"}} {}",
            histogram.sum_ms.load(Ordering::Relaxed) as f64 / 1000.0
        );
        let _ = writeln!(
            out,
            "dreamgate_request_duration_seconds_count{{streamed=\"{streamed}\"}} {count}"
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
        "Requests the gateway as a whole is working on now",
    );
    let _ = writeln!(out, "dreamgate_in_flight {}", st.limits.in_flight());
    family(
        &mut out,
        "dreamgate_in_flight_limit",
        "gauge",
        "How many requests the gateway takes at once",
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
// gateway in front of it. A database that does not answer within a second is answered as unavailable rather
// than waited on, since a health check that hangs is read as a dead process anyway.
pub async fn health(AxumState(st): AxumState<State>) -> Response {
    let probe = sqlx::query("SELECT 1").fetch_one(&st.pool);
    match tokio::time::timeout(Duration::from_secs(1), probe).await {
        Ok(Ok(_)) => (StatusCode::OK, "ok").into_response(),
        Ok(Err(err)) => {
            tracing::error!("health check could not read the database: {err}");
            (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response()
        }
        Err(_) => {
            tracing::error!("health check: the database did not answer within a second");
            (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response()
        }
    }
}
