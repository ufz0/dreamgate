use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::{
    Router,
    body::Body,
    extract::{Json, State as AxumState},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use dreamgate::shutdown::Phase;
use dreamgate::{build_router, db, limits, live, metrics, state::State};
use futures_util::{future::join_all, stream};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::{Row, sqlite::SqlitePool};
use tower::ServiceExt;

static SEED: AtomicUsize = AtomicUsize::new(1);

// The time bounds are the ones the gateway runs with, unless a test is about a time bound and names its own:
// a stall cannot be measured in two minutes. `test_state` builds the client through `limits.http_client()`, the
// same builder `src/main.rs` uses, so what a test measures is the configuration the gateway actually runs with
// rather than one the test invented.
fn test_time_limits(
    global: usize,
    per_connection: usize,
    request_timeout: Duration,
    stream_stall: Duration,
) -> limits::Limits {
    limits::Limits::new(
        global,
        per_connection,
        request_timeout,
        stream_stall,
        Duration::from_secs(limits::DEFAULT_CONNECT_TIMEOUT_SECS),
    )
}

fn test_limits(global: usize, per_connection: usize) -> limits::Limits {
    test_time_limits(
        global,
        per_connection,
        Duration::from_secs(limits::DEFAULT_REQUEST_TIMEOUT_SECS),
        Duration::from_secs(limits::DEFAULT_STREAM_STALL_SECS),
    )
}

// A shared in-memory database with more than one connection. Plain "sqlite::memory:" gives every connection its
// own database, so a test that wants to see requests at the database at the same time has to name the file and
// share the cache.
async fn test_state(limits: limits::Limits, pool: u32) -> State {
    let name = SEED.fetch_add(1, Ordering::SeqCst);
    let url = format!("sqlite://file:concurrency{name}?mode=memory&cache=shared");
    let pool = db::connect(&url, pool).await.unwrap();
    State {
        pool,
        http: limits.http_client(),
        hub: live::hub(),
        presence: Default::default(),
        attempts: Default::default(),
        limits: Arc::new(limits),
        metrics: Default::default(),
        shutdown: Default::default(),
        reservations: Default::default(),
    }
}

// An account, a connection and a key, written straight to the database: these tests are about what happens
// at the gateway, not about the account routes. The key is given the connection it was seeded with, so a
// test that sends some requests to one connection and some to another can be sure which one answered. The
// connection is given a stated capability when the test asks for one, and none when it does not.
async fn seed(st: &State, username: &str, base_url: &str, key: &str, max_concurrent: Option<i64>) {
    let user_id = sqlx::query(
        "INSERT INTO users (username, password_hash, role, enabled, pending, created_at) \
         VALUES (?, 'hash', 'admin', 1, 0, '2026-01-01T00:00:00Z') RETURNING id",
    )
    .bind(username)
    .fetch_one(&st.pool)
    .await
    .unwrap()
    .get::<i64, _>("id");

    let connection_id = sqlx::query(
        "INSERT INTO connections (name, base_url, priority, max_concurrent, created_at) \
         VALUES (?, ?, 0, ?, '2026-01-01T00:00:00Z') RETURNING id",
    )
    .bind(format!("mock{key}"))
    .bind(base_url)
    .bind(max_concurrent)
    .fetch_one(&st.pool)
    .await
    .unwrap()
    .get::<i64, _>("id");

    sqlx::query(
        "INSERT INTO api_keys (user_id, name, key, connection_id, created_at) \
         VALUES (?, 'key', ?, ?, '2026-01-01T00:00:00Z')",
    )
    .bind(user_id)
    .bind(key)
    .bind(connection_id)
    .execute(&st.pool)
    .await
    .unwrap();
}

// A mock upstream that reports usage and keeps count of how many requests it is working on at once. Every
// request carries its own marker, and the numbers the mock reports and the text it answers with are that
// marker, so a test can tell which response and which usage row belong to which request.
#[derive(Clone)]
struct Mock {
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    cut_short: Arc<AtomicBool>,
    delay_ms: u64,
    // A quiet window before the usage chunk. Nothing arriving for it is what a stalled connection looks like
    // from the other side of the gateway, and the stream is not finished at that point.
    quiet_ms: u64,
    // A delay before the mock answers at all, so the wait for the first bytes is the whole of it.
    first_byte_ms: u64,
    chunks: usize,
}

impl Mock {
    fn new(delay_ms: u64, chunks: usize) -> Mock {
        Mock {
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            cut_short: Arc::new(AtomicBool::new(false)),
            delay_ms,
            quiet_ms: 0,
            first_byte_ms: 0,
            chunks,
        }
    }

    fn quiet(mut self, quiet_ms: u64) -> Mock {
        self.quiet_ms = quiet_ms;
        self
    }

    fn responds_after(mut self, first_byte_ms: u64) -> Mock {
        self.first_byte_ms = first_byte_ms;
        self
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn cut_short(&self) -> bool {
        self.cut_short.load(Ordering::SeqCst)
    }
}

// Held for as long as the mock is working on one request. A stream that ends before its usage chunk is the
// client having gone away rather than the answer having finished, which is what the disconnect test looks
// for.
struct Guard {
    mock: Mock,
    sent: usize,
    reported: bool,
}

impl Guard {
    fn new(mock: &Mock) -> Guard {
        let at_once = mock.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        mock.peak.fetch_max(at_once, Ordering::SeqCst);
        Guard {
            mock: mock.clone(),
            sent: 0,
            reported: false,
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.reported {
            self.mock.cut_short.store(true, Ordering::SeqCst);
        }
        self.mock.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn mock_chat(AxumState(mock): AxumState<Mock>, Json(body): Json<Value>) -> Response {
    let marker = body.get("marker").and_then(Value::as_i64).unwrap_or(0);
    let mut guard = Guard::new(&mock);

    // A connection that takes the request and says nothing back. The gateway's window for the first bytes is
    // what ends this, and there is no answer for it to be an answer of.
    if mock.first_byte_ms > 0 {
        tokio::time::sleep(Duration::from_millis(mock.first_byte_ms)).await;
    }

    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
        let mut out = Response::new(Body::empty());
        out.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        *out.body_mut() = Body::from_stream(stream::unfold(guard, move |mut guard| async move {
            if guard.sent < guard.mock.chunks {
                tokio::time::sleep(Duration::from_millis(guard.mock.delay_ms)).await;
                guard.sent += 1;
                let line = json!({ "choices": [{ "delta": { "content": format!("c{marker}-{}", guard.sent - 1) } }] });
                return Some((
                    Ok::<_, std::io::Error>(format!("data: {line}\n\n").into_bytes()),
                    guard,
                ));
            }
            if guard.reported {
                return None;
            }
            // The quiet window is read from the moment the last chunk was sent, which is where a stalled
            // connection is measured from.
            if guard.mock.quiet_ms > 0 {
                tokio::time::sleep(Duration::from_millis(guard.mock.quiet_ms)).await;
            }
            // The usage chunk is the last thing an engine that reports usage sends, so a stream cut short
            // never carries it.
            guard.reported = true;
            let usage = json!({ "choices": [], "usage": { "prompt_tokens": marker, "completion_tokens": marker } });
            Some((
                Ok::<_, std::io::Error>(format!("data: {usage}\n\n").into_bytes()),
                guard,
            ))
        }));
        return out;
    }

    tokio::time::sleep(Duration::from_millis(mock.delay_ms)).await;
    let payload = json!({
        "choices": [{ "message": { "content": format!("answer for {marker}") } }],
        "usage": { "prompt_tokens": marker, "completion_tokens": marker },
    });
    guard.reported = true;
    Json(payload).into_response()
}

// The mock answers the model list after the same first-byte delay, so a test that gives the model list a
// deadline shorter than that delay is measuring the gateway, not the mock.
async fn mock_models(AxumState(mock): AxumState<Mock>) -> Response {
    if mock.first_byte_ms > 0 {
        tokio::time::sleep(Duration::from_millis(mock.first_byte_ms)).await;
    }
    Json(json!({ "data": [] })).into_response()
}

async fn mock_upstream(mock: &Mock) -> String {
    let app = Router::new()
        .route("/v1/chat/completions", post(mock_chat))
        .route("/v1/models", get(mock_models))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn gateway_call(router: &Router, key: &str, marker: i64, stream: bool) -> Response {
    let mut body = json!({
        "model": format!("m{marker}"),
        "marker": marker,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    if stream {
        body["stream"] = json!(true);
    }
    router
        .clone()
        .oneshot(
            axum::http::Request::post("/v1/chat/completions")
                .header("authorization", format!("Bearer {key}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

// The stream tasks write their rows after the response completes, so wait for them rather than assuming
// they are there.
async fn wait_for_rows(pool: &SqlitePool, expected: usize) -> Vec<Value> {
    let mut rows = Vec::new();
    for _ in 0..200 {
        rows = sqlx::query(
            "SELECT api_key_id, model, prompt_tokens, completion_tokens, estimated, streamed \
             FROM usage ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|row| {
            json!({
                "api_key_id": row.get::<Option<i64>, _>("api_key_id"),
                "model": row.get::<Option<String>, _>("model"),
                "prompt_tokens": row.get::<i64, _>("prompt_tokens"),
                "completion_tokens": row.get::<i64, _>("completion_tokens"),
                "estimated": row.get::<i64, _>("estimated") != 0,
                "streamed": row.get::<i64, _>("streamed") != 0,
            })
        })
        .collect();
        if rows.len() >= expected {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "{} of the expected {} usage rows arrived",
        rows.len(),
        expected
    );
}

// A request that is turned away at a limit is not recorded, because nothing crossed the gate: the counters
// measure what reached an upstream. So the rows are exactly the admitted requests.
async fn row_count(pool: &SqlitePool) -> usize {
    sqlx::query("SELECT COUNT(*) AS n FROM usage")
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>("n") as usize
}

// Requests that should all be answered at the same time are sent together, and the mock is watched for how
// many it was working on at once. If anything in the request path serialized them, the peak would be one
// and the wall-clock time would be the sum of the delays.
async fn run_at_once(
    st: &State,
    key: &str,
    markers: Vec<i64>,
) -> (Vec<(i64, StatusCode)>, Duration) {
    let router = build_router(st.clone());
    let started = Instant::now();
    let statuses = join_all(markers.iter().map(|marker| {
        let router = router.clone();
        let key = key.to_string();
        async move {
            let response = gateway_call(&router, &key, *marker, false).await;
            (*marker, response.status())
        }
    }))
    .await;
    (statuses, started.elapsed())
}

#[tokio::test]
async fn requests_to_one_connection_are_handled_at_the_same_time() {
    let delay_ms: u64 = 100;
    let mock = Mock::new(delay_ms, 0);
    // The limits are above what the test sends, so this test is about whether the requests run at the
    // same time, not about admission. The tests that check admission set their own.
    let st = test_state(test_limits(128, 128), 8).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;

    for markers in [
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        (11..=60).collect(),
        (61..=160).collect(),
    ] {
        let count = markers.len();
        let (statuses, elapsed) = run_at_once(&st, "dreamgate_test", markers).await;
        for (marker, status) in &statuses {
            assert_eq!(status, &StatusCode::OK, "request {marker} was refused");
        }
        // Half of what a path that took one request at a time would take. A path that runs them at the same
        // time takes about one round of the delay whatever the count, so this margin does not narrow as the
        // batch grows: it says the requests were not taken one after another, and nothing about a particular
        // request taking a particular number of milliseconds.
        assert!(
            elapsed < Duration::from_millis(delay_ms * count as u64 / 2),
            "{count} requests took {elapsed:?}, which is close to running one after another"
        );
        assert!(
            mock.peak() >= count / 2,
            "the mock saw at most {} of {count} at once",
            mock.peak()
        );
    }

    // Nothing mixed: every row belongs to the request that sent it.
    let rows = wait_for_rows(&st.pool, 160).await;
    for row in &rows {
        let model = row["model"].as_str().unwrap_or("");
        let marker: i64 = model
            .strip_prefix('m')
            .and_then(|text| text.parse().ok())
            .unwrap_or(-1);
        assert_eq!(row["prompt_tokens"], json!(marker));
        assert_eq!(row["completion_tokens"], json!(marker));
        assert_eq!(row["estimated"], false, "the mock reported the counts");
    }
}

#[tokio::test]
async fn many_streams_run_at_the_same_time_and_are_recorded_separately() {
    let mock = Mock::new(40, 3);
    let st = test_state(test_limits(128, 64), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let streams = join_all((1..=20).map(|marker| {
        let router = router.clone();
        async move {
            let response = gateway_call(&router, "dreamgate_test", marker, true).await;
            let text = response.into_body().collect().await.unwrap().to_bytes();
            let text = String::from_utf8_lossy(&text).to_string();
            (marker, text)
        }
    }))
    .await;

    for (marker, text) in &streams {
        for chunk in 0..3 {
            assert!(text.contains(&format!("c{marker}-{chunk}")));
        }
    }
    assert!(
        mock.peak() >= 10,
        "the mock saw at most {} streams at once",
        mock.peak()
    );

    let rows = wait_for_rows(&st.pool, 20).await;
    for row in &rows {
        let marker: i64 = row["model"]
            .as_str()
            .unwrap_or("")
            .strip_prefix('m')
            .and_then(|text| text.parse().ok())
            .unwrap_or(-1);
        assert_eq!(row["completion_tokens"], json!(marker));
        assert_eq!(row["streamed"], true);
    }
}

#[tokio::test]
async fn a_slow_connection_does_not_hold_requests_that_go_elsewhere() {
    let slow = Mock::new(600, 0);
    let fast = Mock::new(20, 0);
    let st = test_state(test_limits(128, 64), 4).await;
    let slow_url = mock_upstream(&slow).await;
    let fast_url = mock_upstream(&fast).await;
    seed(&st, "ada", &slow_url, "key-slow", None).await;
    seed(&st, "ben", &fast_url, "key-fast", None).await;

    let router = build_router(st.clone());
    let started = Instant::now();
    let results = join_all((1..=8).map(|marker| {
        let router = router.clone();
        async move {
            let key = if marker % 2 == 0 {
                "key-fast"
            } else {
                "key-slow"
            };
            let response = gateway_call(&router, key, marker, false).await;
            (marker, key, response.status(), started.elapsed())
        }
    }))
    .await;

    for (marker, key, status, _) in &results {
        assert_eq!(
            status,
            &StatusCode::OK,
            "request {marker} to {key} was refused"
        );
    }

    // What is asserted here is the ordering, not a number of milliseconds: a request that went to the fast
    // connection was answered while requests to the slow one were still being worked on. A gateway that took
    // requests one after another could not do that, whatever the machine it runs on. The peak is what says the
    // two were genuinely overlapping rather than one finishing before the other started.
    let first_fast = results
        .iter()
        .filter(|(_, key, _, _)| *key == "key-fast")
        .map(|(_, _, _, elapsed)| *elapsed)
        .min()
        .unwrap();
    let first_slow = results
        .iter()
        .filter(|(_, key, _, _)| *key == "key-slow")
        .map(|(_, _, _, elapsed)| *elapsed)
        .min()
        .unwrap();
    assert!(
        first_fast < first_slow,
        "a request that went to the fast connection was answered after one that went to the slow one: \
         {first_fast:?} against {first_slow:?}"
    );
    assert!(
        fast.peak() >= 2 && slow.peak() >= 2,
        "the fast connection saw {} at once and the slow one {}, so the requests did not overlap",
        fast.peak(),
        slow.peak()
    );
}

#[tokio::test]
async fn a_connection_at_its_limit_answers_without_queueing_the_request() {
    let mock = Mock::new(400, 0);
    // The default is below what the test sends, so three of the five cannot be admitted.
    let st = test_state(test_limits(128, 2), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;

    let (statuses, _) = run_at_once(&st, "dreamgate_test", (1..=5).collect()).await;
    let admitted = statuses
        .iter()
        .filter(|(_, status)| status == &StatusCode::OK)
        .count();
    let refused = statuses
        .iter()
        .filter(|(_, status)| status == &StatusCode::TOO_MANY_REQUESTS)
        .count();
    assert_eq!(admitted, 2, "the limit allows two at once");
    assert_eq!(
        refused, 3,
        "the rest are answered at once rather than queued"
    );
    assert!(
        mock.peak() <= 2,
        "the mock saw {} at once, above the limit",
        mock.peak()
    );
    // A request that never reached an upstream is not recorded, so the rows are the admitted ones and no
    // more.
    assert_eq!(
        row_count(&st.pool).await,
        admitted,
        "a request turned away at the limit was recorded as usage"
    );

    // A second batch is admitted, which is what shows the slots from the first were given back. A gateway
    // that never released a permit would answer this exactly as it answered the three above.
    let (second, _) = run_at_once(&st, "dreamgate_test", (6..=7).collect()).await;
    for (marker, status) in &second {
        assert_eq!(
            status,
            &StatusCode::OK,
            "request {marker} was refused after a slot"
        );
    }
    assert_eq!(row_count(&st.pool).await, admitted + second.len());
}

#[tokio::test]
async fn a_connection_that_states_its_own_capability_is_admitted_by_it() {
    let mock = Mock::new(400, 0);
    // The gateway default is larger than what this connection says it can take.
    let st = test_state(test_limits(128, 16), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", Some(3)).await;

    let (statuses, _) = run_at_once(&st, "dreamgate_test", (1..=6).collect()).await;
    let admitted = statuses
        .iter()
        .filter(|(_, status)| status == &StatusCode::OK)
        .count();
    assert_eq!(
        admitted, 3,
        "the stated capability is what bounds this connection"
    );
    assert!(mock.peak() <= 3, "the mock saw {} at once", mock.peak());
}

#[tokio::test]
async fn a_gateway_at_its_total_limit_answers_with_503() {
    let mock = Mock::new(400, 0);
    let st = test_state(test_limits(1, 64), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;

    let (statuses, _) = run_at_once(&st, "dreamgate_test", (1..=3).collect()).await;
    let admitted = statuses
        .iter()
        .filter(|(_, status)| status == &StatusCode::OK)
        .count();
    assert_eq!(admitted, 1, "one request at a time");
    assert!(
        statuses
            .iter()
            .any(|(_, status)| status == &StatusCode::SERVICE_UNAVAILABLE),
        "the overload answer says the gateway is full, not the connection"
    );
    assert_eq!(
        row_count(&st.pool).await,
        admitted,
        "a request turned away at the limit was recorded as usage"
    );
}

// A request that reached a connection and failed there has already crossed the gate, so it is recorded and
// answered as that failure. Admitting at the start of the request rather than per attempt is what keeps it
// from being answered as an overload on a later attempt, which would be a request that left no row.
#[tokio::test]
async fn a_request_that_reached_a_connection_is_recorded_even_when_it_failed_there() {
    let st = test_state(test_limits(1, 64), 4).await;
    seed(&st, "ada", "http://127.0.0.1:1", "dreamgate_test", None).await;

    let (statuses, _) = run_at_once(&st, "dreamgate_test", vec![1]).await;
    assert_eq!(
        statuses[0].1,
        StatusCode::BAD_GATEWAY,
        "the connection that could not be reached is answered as it was"
    );
    assert_eq!(
        row_count(&st.pool).await,
        1,
        "the request that crossed the gate left its row"
    );
}

#[tokio::test]
async fn a_client_that_goes_away_mid_stream_stops_the_upstream_and_is_still_recorded() {
    let mock = Mock::new(60, 6);
    let st = test_state(test_limits(128, 64), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let response = gateway_call(&router, "dreamgate_test", 7, true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    for _ in 0..2 {
        let frame = body.frame().await.expect("a chunk arrived");
        assert!(frame.is_ok(), "the stream ended before the client left");
    }
    // Leaving the body here is the client disconnecting: the stream is dropped before its last chunk, so
    // the usage chunk is never seen.
    drop(body);

    for _ in 0..200 {
        if mock.cut_short() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        mock.cut_short(),
        "the mock finished all its chunks, so the stream was not cut short"
    );

    let rows = wait_for_rows(&st.pool, 1).await;
    assert_eq!(rows[0]["model"], "m7");
    assert_eq!(rows[0]["streamed"], true);
    assert_eq!(
        rows[0]["estimated"], true,
        "nothing was reported for a stream that ended before its usage chunk"
    );
}

// A stream holds its slots for as long as it runs, not until the handler returned, and gives them back when
// it ends — including when it ends because the client went away. One slot is what makes this visible: a
// request that arrives while the stream is open is answered at once rather than queued, and the next request
// after the stream is admitted.
#[tokio::test]
async fn a_stream_holds_its_slot_until_it_ends_and_then_gives_it_back() {
    let mock = Mock::new(60, 3);
    let st = test_state(test_limits(128, 1), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let running = gateway_call(&router, "dreamgate_test", 8, true).await;
    assert_eq!(
        running.status(),
        StatusCode::OK,
        "the stream holds the only slot"
    );
    let while_running = gateway_call(&router, "dreamgate_test", 9, false).await;
    assert_eq!(
        while_running.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "a request was queued behind the stream instead of being answered at once"
    );

    // Leaving the stream here is the client disconnecting. The slot is given back in the stream task after
    // its next read, so this is polled rather than asserted at once; a gateway that never released a permit
    // would answer every request like this one.
    drop(running.into_body());
    let mut admitted = StatusCode::TOO_MANY_REQUESTS;
    for _ in 0..100 {
        admitted = gateway_call(&router, "dreamgate_test", 10, false)
            .await
            .status();
        if admitted == StatusCode::OK {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(admitted, StatusCode::OK, "the slot was not given back");
    assert!(
        mock.cut_short(),
        "the stream finished normally, so the client did not leave"
    );
}

// The bug this test holds. A deadline for the whole of a response cuts a stream that is still producing an
// answer, and a cut stream is what a client feels as the socket dying in the middle of a generation. A stream
// has no known end, so it is bounded by silence instead, and an answer that keeps arriving is not stalled
// however long it takes. Here the mock runs past the request deadline without ever going quiet, so the answer
// has to arrive whole — including its usage chunk, which is what makes the row a reported figure rather than
// an estimate.
#[tokio::test]
async fn a_stream_that_runs_past_the_request_deadline_arrives_in_full() {
    // Four chunks at 120 ms each is half a second, past the deadline the test gives. The stall window is far
    // longer, so nothing in this stream is quiet and nothing should be cut.
    let mock = Mock::new(120, 4);
    let limits = test_time_limits(64, 64, Duration::from_millis(200), Duration::from_secs(10));
    let st = test_state(limits, 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let response = gateway_call(&router, "dreamgate_test", 11, true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let collected = response.into_body().collect().await;
    assert!(
        collected.is_ok(),
        "the stream was cut before it finished: {collected:?}"
    );
    let text = String::from_utf8_lossy(&collected.unwrap().to_bytes()).to_string();
    assert_eq!(
        text.matches("data: ").count(),
        5,
        "the answer arrived as: {text}"
    );
    assert!(
        text.contains("\"usage\""),
        "the usage chunk is the last thing the mock sends and it did not arrive"
    );

    let rows = wait_for_rows(&st.pool, 1).await;
    assert_eq!(rows[0]["model"], "m11");
    assert_eq!(rows[0]["streamed"], true);
    assert_eq!(
        rows[0]["estimated"], false,
        "the upstream reported its usage, so this row is not an estimate"
    );

    // And the same mock, read past the deadline, is not seen as stalled by the stall window: the two bounds
    // measure different things and the test would be meaningless if one were inside the other.
    assert!(
        !mock.cut_short(),
        "the mock's stream was cut, so the answer did not arrive in full"
    );
}

// What is kept: the deadline for a single answer, applied where there is an end. It is applied per request by
// the gateway rather than by the client to the whole response, which is what stops it reaching a stream. A
// request that was given up on at a connection has already crossed the gate, so it is answered as that failure
// and recorded, as it was before. Read on its own this test cannot tell the two mechanisms apart — a client
// total deadline would answer the same way — so it is the test above, where a stream runs past the deadline and
// is not cut, that says which one the gateway uses.
#[tokio::test]
async fn a_single_answer_is_still_given_its_deadline() {
    let mock = Mock::new(400, 0);
    let limits = test_time_limits(64, 64, Duration::from_millis(100), Duration::from_secs(10));
    let st = test_state(limits, 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let response = gateway_call(&router, "dreamgate_test", 12, false).await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_GATEWAY,
        "a single answer that takes longer than its deadline is still given up on"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice::<Value>(&body).unwrap();
    assert_eq!(json["error"]["type"], "upstream_unavailable");
    assert_eq!(
        row_count(&st.pool).await,
        1,
        "the request reached a connection, so its row stands"
    );
}

// A stream that goes quiet for the stall window is closed rather than held open: silence for the window is a
// dead connection, not a slow generation, and a request that holds a slot for a dead connection holds it away
// from other requests. The client sees the stream end with an error, so it can tell the answer is incomplete,
// and the row stands as an estimate because nothing was reported for a stream that never reached its usage
// chunk.
#[tokio::test]
async fn a_stream_that_goes_quiet_is_closed_and_its_slot_is_given_back() {
    // Two chunks, then two seconds of silence before the usage chunk — an order of magnitude longer than the
    // window, so the silence is what ends the stream rather than the answer being finished, and the margins are
    // wide enough that a loaded runner cannot turn the ordering around.
    let mock = Mock::new(25, 2).quiet(2000);
    let limits = test_time_limits(64, 1, Duration::from_secs(10), Duration::from_millis(500));
    let mut st = test_state(limits, 4).await;
    st.metrics = Arc::new(metrics::Metrics::new(Some("scrape-secret".to_string())));
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let response = gateway_call(&router, "dreamgate_test", 13, true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    for _ in 0..2 {
        let frame = body.frame().await.expect("a chunk arrived");
        assert!(frame.is_ok(), "the stream was closed before it went quiet");
    }

    // While it is quiet it still holds the only slot, so a request that arrives is answered at once rather
    // than made to wait for the stream.
    let while_quiet = gateway_call(&router, "dreamgate_test", 14, false).await;
    assert_eq!(
        while_quiet.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "a request was queued behind the stream instead of being answered at once"
    );

    let frame = body
        .frame()
        .await
        .expect("the stream was closed at the silence");
    assert!(
        frame.is_err(),
        "the stream ended as a complete answer, so the silence did not close it"
    );

    // The mock's stream is dropped when the gateway stops reading it, which is what the mock counts as a
    // stream cut short. That happens in the gateway's stream task, so it is polled rather than asserted at
    // once; if the mock had been allowed to finish, it would have reported its usage chunk and this would say
    // the answer was complete.
    for _ in 0..200 {
        if mock.cut_short() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        mock.cut_short(),
        "the mock finished its usage chunk, so the stream was not cut at the silence"
    );

    let rows = wait_for_rows(&st.pool, 1).await;
    assert_eq!(rows[0]["model"], "m13");
    assert_eq!(rows[0]["streamed"], true);
    assert_eq!(
        rows[0]["estimated"], true,
        "nothing was reported for a stream closed at the silence"
    );

    // A stream closed at the silence is written down against its connection, and counted for a scrape.
    let stalled =
        sqlx::query("SELECT COUNT(*) AS n FROM connection_failures WHERE kind = 'stalled'")
            .fetch_one(&st.pool)
            .await
            .unwrap()
            .get::<i64, _>("n");
    assert_eq!(stalled, 1);
    let scrape = router
        .clone()
        .oneshot(
            axum::http::Request::get("/metrics")
                .header("authorization", "Bearer scrape-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let scrape = scrape.into_body().collect().await.unwrap().to_bytes();
    assert!(
        String::from_utf8_lossy(&scrape)
            .lines()
            .any(|l| l == "dreamgate_stream_stalls_total 1")
    );

    // The slot is given back when the stalled stream ends, so the next request is admitted. The release
    // happens in the stream task, so this is polled rather than asserted at once.
    let mut admitted = StatusCode::TOO_MANY_REQUESTS;
    for _ in 0..100 {
        admitted = gateway_call(&router, "dreamgate_test", 15, false)
            .await
            .status();
        if admitted == StatusCode::OK {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        admitted,
        StatusCode::OK,
        "the stalled stream still held its slot"
    );
}

// The window bounds silence, not the length of the answer, so it restarts after every read that arrives. A
// stream that goes quiet for less than the window and then continues is a working connection and must not be
// closed: a test that only looked at a silence longer than the window could not tell a per-read window from a
// deadline for the whole answer.
#[tokio::test]
async fn a_stream_that_goes_quiet_below_the_window_and_then_resumes_is_not_closed() {
    // Eighty milliseconds of silence against a window four times that. The usage chunk still arrives, so the row
    // is a reported figure and not an estimate, and the mock finishes its stream.
    let mock = Mock::new(40, 2).quiet(80);
    let limits = test_time_limits(64, 64, Duration::from_secs(10), Duration::from_millis(400));
    let st = test_state(limits, 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let response = gateway_call(&router, "dreamgate_test", 16, true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let collected = response.into_body().collect().await;
    assert!(
        collected.is_ok(),
        "a silence shorter than the window closed the stream: {collected:?}"
    );

    let rows = wait_for_rows(&st.pool, 1).await;
    assert_eq!(rows[0]["model"], "m16");
    assert_eq!(
        rows[0]["estimated"], false,
        "the usage chunk arrived after a short silence, so the answer was not cut"
    );
    assert!(
        !mock.cut_short(),
        "the mock's stream was cut short by the window"
    );
}

// The model list is not a stream, so the same deadline covers it as covers any single answer, as the client's
// total deadline did before this change. A connection that never answers the list is given up on.
#[tokio::test]
async fn the_model_list_keeps_the_deadline_a_single_answer_is_given() {
    let mock = Mock::new(0, 0).responds_after(400);
    let limits = test_time_limits(
        64,
        64,
        Duration::from_millis(100),
        Duration::from_millis(100),
    );
    let st = test_state(limits, 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let response = router
        .clone()
        .oneshot(
            axum::http::Request::get("/v1/models")
                .header("authorization", "Bearer dreamgate_test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_GATEWAY,
        "a connection that never answers the model list is given up on"
    );
}

// The same window covers the wait for the first bytes, which is the reported symptom in its plainest form: a
// connection that takes the request and never answers. There is no answer to cut here, so the request is given
// up on and answered as the connection failing, as any other transport failure is.
#[tokio::test]
async fn a_connection_that_never_answers_is_given_up_on_within_the_window() {
    let mock = Mock::new(0, 2).responds_after(500);
    let limits = test_time_limits(64, 64, Duration::from_secs(10), Duration::from_millis(150));
    let st = test_state(limits, 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let response = gateway_call(&router, "dreamgate_test", 17, true).await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_GATEWAY,
        "a connection that never answered is answered as a failed connection"
    );
    assert_eq!(
        row_count(&st.pool).await,
        1,
        "the request reached a connection, so its row stands"
    );
}

#[tokio::test]
async fn usage_records_written_by_many_requests_at_once_are_all_there_and_do_not_mix() {
    let mock = Mock::new(10, 0);
    // More requests than the database had connections before this change.
    let st = test_state(test_limits(256, 256), 16).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;

    let markers: Vec<i64> = (1..=100).collect();
    let (statuses, _) = run_at_once(&st, "dreamgate_test", markers.clone()).await;
    for (marker, status) in &statuses {
        assert_eq!(status, &StatusCode::OK, "request {marker} was refused");
    }

    let rows = wait_for_rows(&st.pool, 100).await;
    let seen: HashMap<i64, Value> = rows
        .iter()
        .map(|row| {
            let marker: i64 = row["model"]
                .as_str()
                .unwrap_or("")
                .strip_prefix('m')
                .and_then(|text| text.parse().ok())
                .unwrap_or(-1);
            (marker, row.clone())
        })
        .collect();
    assert_eq!(seen.len(), 100, "every request left its own row");
    for marker in &markers {
        let row = &seen[marker];
        assert_eq!(row["prompt_tokens"], json!(marker));
        assert_eq!(row["completion_tokens"], json!(marker));
    }
}

async fn get_status(router: &Router, path: &str) -> (StatusCode, String) {
    let response = router
        .clone()
        .oneshot(
            axum::http::Request::get(path)
                .header("authorization", "Bearer dreamgate_test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

// Once the process is told to stop, nothing new is started: a request that could not be given the time to
// finish is turned away with 503 and not recorded, the model list the same, and the health check says the
// process is draining so a load balancer stops sending it requests.
#[tokio::test]
async fn a_request_that_arrives_while_the_gateway_is_stopping_is_turned_away() {
    let mock = Mock::new(0, 1);
    let st = test_state(test_limits(8, 8), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    st.shutdown.advance(Phase::Draining);
    let response = gateway_call(&router, "dreamgate_test", 1, false).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["type"], "shutting_down");
    assert_eq!(
        get_status(&router, "/v1/models").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        get_status(&router, "/health").await,
        (StatusCode::SERVICE_UNAVAILABLE, "draining".to_string())
    );
    assert_eq!(row_count(&st.pool).await, 0, "nothing was started");
    assert_eq!(mock.peak(), 0);
}

// A stream that is running when the process is told to stop is let finish, and the drain only ends once its
// row is written: every slot of the gateway coming back is every admitted request recorded.
#[tokio::test]
async fn a_request_in_flight_when_stopping_begins_finishes_and_is_recorded_before_the_drain_ends() {
    let mock = Mock::new(80, 5);
    let st = test_state(test_limits(8, 8), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let running = gateway_call(&router, "dreamgate_test", 7, true).await;
    assert_eq!(running.status(), StatusCode::OK);
    let reading = tokio::spawn(async move { running.into_body().collect().await.is_ok() });
    st.shutdown.advance(Phase::Draining);

    // The drain looks at the slots rather than taking them, so while it waits what is in flight reads as the
    // one stream, not as every slot of the gateway.
    let limits = st.limits.clone();
    let drain = tokio::spawn(async move { limits.drained(Duration::from_secs(10)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(st.limits.in_flight(), 1);
    assert!(drain.await.unwrap());
    assert_eq!(
        row_count(&st.pool).await,
        1,
        "the drain ended before the row was written"
    );
    assert!(
        reading.await.unwrap(),
        "the stream did not end as it should"
    );
    let rows = wait_for_rows(&st.pool, 1).await;
    assert_eq!(rows[0]["estimated"], false, "the stream finished whole");
    assert!(!mock.cut_short());
}

// A stream still running when the grace window has passed is ended so the process can stop, and its row is
// written with what was known when it was cut: no usage chunk arrived, so the counts are the gateway's
// estimate. The client sees the stream end as an error, which is how it tells a cut answer from a finished one.
#[tokio::test]
async fn a_stream_still_running_when_the_grace_window_passes_is_ended_and_recorded() {
    let mock = Mock::new(200, 100);
    let st = test_state(test_limits(8, 8), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let running = gateway_call(&router, "dreamgate_test", 3, true).await;
    assert_eq!(running.status(), StatusCode::OK);
    let reading = tokio::spawn(async move { running.into_body().collect().await.is_ok() });
    st.shutdown.advance(Phase::Draining);
    st.shutdown.advance(Phase::Closing);

    let started = Instant::now();
    assert!(st.limits.drained(Duration::from_secs(10)).await);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the stream was let run rather than ended"
    );
    assert_eq!(row_count(&st.pool).await, 1);
    assert!(
        !reading.await.unwrap(),
        "a cut stream ended as if it had finished"
    );
    let rows = wait_for_rows(&st.pool, 1).await;
    assert_eq!(rows[0]["estimated"], true);
    assert_eq!(rows[0]["streamed"], true);
}

// A client that keeps its connection but stops reading leaves the stream's channel full, and the stream task
// waiting to hand it the next chunk. The stop does not wait for that client: the stream is ended at Closing all
// the same, its row written and its slot given back, so the drain is not held to its deadline and the row is
// not lost.
#[tokio::test]
async fn a_stream_whose_client_stopped_reading_is_still_ended_and_recorded() {
    let mock = Mock::new(0, 2000);
    let st = test_state(test_limits(8, 8), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let held = gateway_call(&router, "dreamgate_test", 3, true).await;
    assert_eq!(held.status(), StatusCode::OK);
    // Long enough for the upstream to have filled the channel the response is not reading.
    tokio::time::sleep(Duration::from_millis(200)).await;
    st.shutdown.advance(Phase::Draining);
    st.shutdown.advance(Phase::Closing);

    assert!(
        st.limits.drained(Duration::from_secs(3)).await,
        "the stream waited on a client that does not read"
    );
    assert_eq!(row_count(&st.pool).await, 1);
    // What was queued still reaches the client, and then the error that says the answer was cut.
    let mut body = held.into_body();
    let mut ended_with_error = false;
    while let Some(frame) = body.frame().await {
        if frame.is_err() {
            ended_with_error = true;
            break;
        }
    }
    assert!(ended_with_error, "a cut stream ended as if it had finished");
}

// A request can pass the check at the top just before the process starts to stop and then wait on the
// database for its key. If the drain has found every slot back in the meantime, nothing would wait for that
// request or end it, so once it holds its slot it reads the phase again and is turned away, before it
// reaches a connection.
#[tokio::test]
async fn a_request_that_waited_on_the_database_while_the_drain_ended_is_turned_away() {
    let mock = Mock::new(0, 2);
    let st = test_state(test_limits(8, 8), 1).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let held = st.pool.acquire().await.unwrap();
    let waiting = {
        let router = router.clone();
        tokio::spawn(async move { gateway_call(&router, "dreamgate_test", 1, false).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    st.shutdown.advance(Phase::Draining);
    assert!(st.limits.drained(Duration::from_secs(1)).await);
    drop(held);

    let response = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["type"], "shutting_down");
    assert_eq!(
        mock.peak(),
        0,
        "the request reached a connection after the drain"
    );
    assert_eq!(row_count(&st.pool).await, 0);
}

// A single answer still waiting when the grace window has passed is ended as well, answered as the gateway
// shutting down rather than as a failed connection, and recorded against the connection that was working on
// it, with the input estimated because no answer came back.
#[tokio::test]
async fn a_single_answer_still_waiting_when_the_grace_window_passes_is_ended_and_recorded() {
    let mock = Mock::new(10_000, 0);
    let st = test_state(test_limits(8, 8), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let waiting = {
        let router = router.clone();
        tokio::spawn(async move { gateway_call(&router, "dreamgate_test", 5, false).await })
    };
    for _ in 0..200 {
        if mock.peak() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(mock.peak(), 1, "the request never reached the mock");
    st.shutdown.advance(Phase::Draining);
    st.shutdown.advance(Phase::Closing);

    let response = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the request was let wait out its answer")
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(st.limits.drained(Duration::from_secs(5)).await);
    let row = sqlx::query("SELECT status, connection_id, estimated FROM usage")
        .fetch_one(&st.pool)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>("status"), 503);
    assert!(row.get::<Option<i64>, _>("connection_id").is_some());
    assert_eq!(row.get::<i64, _>("estimated"), 1);
}

async fn set_key_limits(st: &State, key: &str, rpm_limit: Option<i64>, token_cap: Option<i64>) {
    sqlx::query("UPDATE api_keys SET rpm_limit = ?, token_cap = ? WHERE key = ?")
        .bind(rpm_limit)
        .bind(token_cap)
        .bind(key)
        .execute(&st.pool)
        .await
        .unwrap();
}

async fn answers(responses: Vec<Response>) -> Vec<(StatusCode, Value)> {
    let mut out = Vec::new();
    for response in responses {
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        out.push((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ));
    }
    out
}

// A key's per-minute limit used to be read from the rows, which are written when a request ends, so requests
// sent at the same moment all read none and all passed. Each is now counted from the moment it is admitted, so
// twenty at once on a key limited to two a minute are two admitted and eighteen refused.
#[tokio::test]
async fn twenty_requests_at_once_on_a_key_limited_to_two_a_minute_admit_two() {
    let mock = Mock::new(200, 0);
    let st = test_state(test_limits(64, 64), 8).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    set_key_limits(&st, "dreamgate_test", Some(2), None).await;
    let router = build_router(st.clone());

    let responses =
        join_all((0..20).map(|n| gateway_call(&router, "dreamgate_test", n, false))).await;
    let answers = answers(responses).await;
    let admitted = answers.iter().filter(|(s, _)| *s == StatusCode::OK).count();
    let limited = answers
        .iter()
        .filter(|(s, body)| {
            *s == StatusCode::TOO_MANY_REQUESTS && body["error"]["type"] == "rate_limit"
        })
        .count();
    assert_eq!((admitted, limited), (2, 18));
    assert_eq!(wait_for_rows(&st.pool, 2).await.len(), 2);
    assert_eq!(
        row_count(&st.pool).await,
        2,
        "only the admitted requests are recorded"
    );
}

// A token cap used to be read from the rows alone, so requests sent together all saw the cap unreached. The
// input of a request still running is now held against it, as the gateway estimated it, and the refusal says
// how much of what it counted is that estimate.
#[tokio::test]
async fn a_key_cap_counts_the_input_of_requests_still_running() {
    let mock = Mock::new(200, 0);
    let st = test_state(test_limits(64, 64), 8).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    set_key_limits(&st, "dreamgate_test", None, Some(1)).await;
    let router = build_router(st.clone());

    // Markers from one, because the mock reports the marker as its usage, and a row of nothing would leave
    // the cap unreached for a check that runs after it.
    let responses =
        join_all((1..=10).map(|n| gateway_call(&router, "dreamgate_test", n, false))).await;
    let answers = answers(responses).await;
    assert_eq!(
        answers.iter().filter(|(s, _)| *s == StatusCode::OK).count(),
        1
    );
    let refused: Vec<&Value> = answers
        .iter()
        .filter(|(s, _)| *s == StatusCode::TOO_MANY_REQUESTS)
        .map(|(_, body)| body)
        .collect();
    assert_eq!(refused.len(), 9);
    for body in refused {
        assert_eq!(body["error"]["type"], "token_cap");
        let message = body["error"]["message"].as_str().unwrap();
        // The admitted request may already have been recorded when a refused one was checked, so a refusal
        // either names the estimate of the one still running or none at all.
        assert!(
            message.starts_with("key 'key' has reached its token cap of 1 input and output tokens"),
            "{message}"
        );
    }
}

// The same for an account's daily limit, which a key cannot be used to work around.
#[tokio::test]
async fn an_account_daily_limit_counts_the_input_of_requests_still_running() {
    let mock = Mock::new(200, 0);
    let st = test_state(test_limits(64, 64), 8).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    sqlx::query("UPDATE users SET daily_token_cap = 1")
        .execute(&st.pool)
        .await
        .unwrap();
    let router = build_router(st.clone());

    // Markers from one, because the mock reports the marker as its usage, and a row of nothing would leave
    // the cap unreached for a check that runs after it.
    let responses =
        join_all((1..=10).map(|n| gateway_call(&router, "dreamgate_test", n, false))).await;
    let answers = answers(responses).await;
    assert_eq!(
        answers.iter().filter(|(s, _)| *s == StatusCode::OK).count(),
        1
    );
    let estimated = answers.iter().filter(|(_, body)| {
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("counting an estimated"))
    });
    assert!(
        estimated.count() > 0,
        "no refusal said that it counted the estimate of a request still running: {answers:?}"
    );
}

// A request turned away before it reached a connection crossed no gate and is never recorded, so it gives
// back its place in the minute as well: a key limited to two a minute whose second request found the
// connection full can still send one more.
#[tokio::test]
async fn a_request_turned_away_at_a_full_connection_gives_its_place_in_the_minute_back() {
    let mock = Mock::new(60, 3);
    let st = test_state(test_limits(64, 1), 8).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    set_key_limits(&st, "dreamgate_test", Some(2), None).await;
    let router = build_router(st.clone());

    let running = gateway_call(&router, "dreamgate_test", 1, true).await;
    assert_eq!(running.status(), StatusCode::OK);
    let full = gateway_call(&router, "dreamgate_test", 2, false).await;
    assert_eq!(full.status(), StatusCode::TOO_MANY_REQUESTS);
    let full = answers(vec![full]).await;
    assert_eq!(full[0].1["error"]["type"], "connection_full");
    running.into_body().collect().await.unwrap();
    wait_for_rows(&st.pool, 1).await;

    let mut second = StatusCode::TOO_MANY_REQUESTS;
    for _ in 0..100 {
        second = gateway_call(&router, "dreamgate_test", 3, false)
            .await
            .status();
        if second == StatusCode::OK {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        second,
        StatusCode::OK,
        "the refused request kept its place in the minute"
    );
    let third = answers(vec![
        gateway_call(&router, "dreamgate_test", 4, false).await,
    ])
    .await;
    assert_eq!(third[0].0, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(third[0].1["error"]["type"], "rate_limit");
}

// The rows of the last minute still count after a restart, which empties what the gateway holds in the
// process: they add to the requests that arrive together, rather than standing in for them.
#[tokio::test]
async fn after_a_restart_the_rows_of_the_last_minute_add_to_a_burst() {
    let mock = Mock::new(200, 0);
    let st = test_state(test_limits(64, 64), 8).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    set_key_limits(&st, "dreamgate_test", Some(2), None).await;
    let before = build_router(st.clone());
    assert_eq!(
        gateway_call(&before, "dreamgate_test", 1, false)
            .await
            .status(),
        StatusCode::OK
    );

    let mut restarted = st.clone();
    restarted.reservations = Default::default();
    let router = build_router(restarted);
    let responses =
        join_all((0..20).map(|n| gateway_call(&router, "dreamgate_test", n, false))).await;
    let admitted = responses
        .iter()
        .filter(|response| response.status() == StatusCode::OK)
        .count();
    assert_eq!(admitted, 1, "the row from before the restart did not count");
}

// A single answer whose client leaves after it reached a connection is not recorded, because the request is
// dropped with the client's connection, but the upstream was asked, so it keeps its place in the minute.
#[tokio::test]
async fn a_request_whose_client_left_after_it_reached_a_connection_keeps_its_place_in_the_minute() {
    let mock = Mock::new(2000, 0);
    let st = test_state(test_limits(64, 64), 8).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    set_key_limits(&st, "dreamgate_test", Some(1), None).await;
    let router = build_router(st.clone());

    let leaving = {
        let router = router.clone();
        tokio::spawn(async move { gateway_call(&router, "dreamgate_test", 1, false).await })
    };
    for _ in 0..200 {
        if mock.peak() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(mock.peak(), 1, "the request never reached the mock");
    leaving.abort();
    let _ = leaving.await;

    let next = answers(vec![
        gateway_call(&router, "dreamgate_test", 2, false).await,
    ])
    .await;
    assert_eq!(next[0].0, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(next[0].1["error"]["type"], "rate_limit");
}

// A disabled account is answered as disabled even when its key is also at a limit, because the account is
// checked before the limits.
#[tokio::test]
async fn a_disabled_account_is_answered_as_disabled_before_its_keys_limit() {
    let mock = Mock::new(0, 0);
    let st = test_state(test_limits(64, 64), 8).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    set_key_limits(&st, "dreamgate_test", Some(1), None).await;
    let router = build_router(st.clone());
    assert_eq!(
        gateway_call(&router, "dreamgate_test", 1, false)
            .await
            .status(),
        StatusCode::OK
    );
    sqlx::query("UPDATE users SET enabled = 0")
        .execute(&st.pool)
        .await
        .unwrap();
    let refused = answers(vec![
        gateway_call(&router, "dreamgate_test", 2, false).await,
    ])
    .await;
    assert_eq!(refused[0].0, StatusCode::FORBIDDEN);
    assert_eq!(refused[0].1["error"]["type"], "account_disabled");
}

// A request whose row lands while another request's check is reading the rows is in that reading or it is not,
// and the check knows which by the highest row id the reading saw, so it is never counted twice. Thirty
// requests a millisecond apart on a key limited to thirty a minute are all within the limit, and a pool of one
// connection, which widens the time a reading takes, is where counting by time refused some of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_within_the_per_minute_limit_are_not_refused_while_others_are_recorded() {
    for _ in 0..20 {
        let mock = Mock::new(3, 0);
        let st = test_state(test_limits(64, 64), 1).await;
        let base_url = mock_upstream(&mock).await;
        seed(&st, "ada", &base_url, "dreamgate_test", None).await;
        set_key_limits(&st, "dreamgate_test", Some(30), None).await;
        let router = build_router(st.clone());

        let mut sending = Vec::new();
        for n in 0..30 {
            let router = router.clone();
            sending.push(tokio::spawn(async move {
                gateway_call(&router, "dreamgate_test", n, false)
                    .await
                    .status()
            }));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for sent in sending {
            assert_eq!(sent.await.unwrap(), StatusCode::OK);
        }
    }
}

// A row that cannot be written is not in the rows, but the request reached a connection, so it keeps its place
// in the minute as one that has no row.
#[tokio::test]
async fn a_request_whose_row_could_not_be_written_keeps_its_place_in_the_minute() {
    let mock = Mock::new(0, 0);
    let st = test_state(test_limits(64, 64), 4).await;
    let base_url = mock_upstream(&mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    set_key_limits(&st, "dreamgate_test", Some(1), None).await;
    sqlx::query(
        "CREATE TRIGGER no_rows BEFORE INSERT ON usage BEGIN SELECT RAISE(ABORT, 'full'); END",
    )
    .execute(&st.pool)
    .await
    .unwrap();
    let router = build_router(st.clone());

    assert_eq!(
        gateway_call(&router, "dreamgate_test", 1, false)
            .await
            .status(),
        StatusCode::OK
    );
    let next = answers(vec![
        gateway_call(&router, "dreamgate_test", 2, false).await,
    ])
    .await;
    assert_eq!(next[0].0, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(next[0].1["error"]["type"], "rate_limit");
}

// Not part of the normal run: it measures, so it is only read when asked for. Run with
// `cargo test --release -- --ignored --nocapture`. The runtime is the multi-thread one, because a
// single-threaded runtime would measure one worker's throughput rather than the gateway's. The limits are
// above what is sent, so nothing is refused here: a refused request is not measured, and a benchmark of
// refusals would say nothing about the request path.
async fn measure(mock: &Mock, count: usize, pool: u32) {
    // Every request has to be admitted, so the limits have to be at least as large as the batch.
    let st = test_state(test_limits(count + 1, count + 1), pool).await;
    let base_url = mock_upstream(mock).await;
    seed(&st, "ada", &base_url, "dreamgate_test", None).await;
    let router = build_router(st.clone());

    let started = Instant::now();
    let results = join_all((1..=count as i64).map(|marker| {
        let router = router.clone();
        async move {
            // The time of one request from the moment it is sent to the moment its last byte is read,
            // not from the start of the batch: from the start of the batch this would only say when the
            // request finished, and the percentile would be a queue position rather than a latency.
            let request_started = Instant::now();
            let response = gateway_call(&router, "dreamgate_test", marker, false).await;
            let status = response.status();
            response.into_body().collect().await.unwrap();
            (status, request_started.elapsed().as_secs_f64())
        }
    }))
    .await;
    let elapsed = started.elapsed().as_secs_f64();

    let answered: Vec<f64> = results
        .iter()
        .filter(|(status, _)| status == &StatusCode::OK)
        .map(|(_, latency)| *latency)
        .collect();
    let refused = results.len() - answered.len();
    if answered.is_empty() {
        println!("nothing was answered, so there is nothing to measure");
        return;
    }
    let mut sorted = answered.clone();
    sorted.sort_by(f64::total_cmp);
    let percentile = |part: usize| sorted[sorted.len() * part / 100];
    let mean: f64 = answered.iter().sum::<f64>() / answered.len() as f64;

    println!(
        "pool of {pool}, {count} requests, {refused} refused, {elapsed:.3} s wall clock, {:.1} requests per second",
        results.len() as f64 / elapsed
    );
    println!(
        "one request, all of them running: mean {:.1} ms, p50 {:.1} ms, p95 {:.1} ms, p99 {:.1} ms",
        mean * 1000.0,
        percentile(50) * 1000.0,
        percentile(95) * 1000.0,
        percentile(99) * 1000.0
    );
    println!("peak requests the mock saw at once: {}", mock.peak());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn throughput_and_latency_against_the_mock() {
    // The mock answers at once, so what is measured here is the gateway's own cost per request: the key
    // and account lookups, the request to the upstream, the usage row. The peak at the mock is below the
    // number of requests sent because each request still has to wait its turn at the database for the
    // lookups and its own row, which is the narrow point left in the path.
    println!("--- mock with no delay, gateway cost only");
    measure(&Mock::new(0, 0), 200, 16).await;
    measure(&Mock::new(0, 0), 200, 64).await;

    // The mock holds each request for 50 ms. If the path were serialized, the wall clock would be twenty
    // seconds for two hundred requests; it is one round plus the gateway's cost.
    println!("--- mock holding each request for 50 ms");
    measure(&Mock::new(50, 0), 200, 64).await;
}
