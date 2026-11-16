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
    routing::post,
};
use dreamgate::{build_router, db, limits, live, state::State};
use futures_util::{future::join_all, stream};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::{Row, sqlite::SqlitePool};
use tower::ServiceExt;

static SEED: AtomicUsize = AtomicUsize::new(1);

// A shared in-memory database with more than one connection. Plain "sqlite::memory:" gives every
// connection its own database, so a test that wants to see requests at the database at the same time has
// to name the file and share the cache.
async fn test_state(global: usize, per_connection: usize, pool: u32) -> State {
    let name = SEED.fetch_add(1, Ordering::SeqCst);
    let url = format!("sqlite://file:concurrency{name}?mode=memory&cache=shared");
    let pool = db::connect(&url, pool).await.unwrap();
    State {
        pool,
        http: reqwest::Client::new(),
        hub: live::hub(),
        presence: Default::default(),
        attempts: Default::default(),
        limits: Arc::new(limits::Limits::new(global, per_connection)),
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
    chunks: usize,
}

impl Mock {
    fn new(delay_ms: u64, chunks: usize) -> Mock {
        Mock {
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            cut_short: Arc::new(AtomicBool::new(false)),
            delay_ms,
            chunks,
        }
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

    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
        let mut out = Response::new(Body::empty());
        out.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        *out.body_mut() = Body::from_stream(stream::unfold(guard, move |mut guard| async move {
            tokio::time::sleep(Duration::from_millis(guard.mock.delay_ms)).await;
            if guard.sent < guard.mock.chunks {
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

async fn mock_upstream(mock: &Mock) -> String {
    let app = Router::new()
        .route("/v1/chat/completions", post(mock_chat))
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
    let st = test_state(128, 128, 8).await;
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
    let st = test_state(128, 64, 4).await;
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
    let st = test_state(128, 64, 4).await;
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
    let st = test_state(128, 2, 4).await;
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
    let st = test_state(128, 16, 4).await;
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
    let st = test_state(1, 64, 4).await;
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
    let st = test_state(1, 64, 4).await;
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
    let st = test_state(128, 64, 4).await;
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
    let st = test_state(128, 1, 4).await;
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

#[tokio::test]
async fn usage_records_written_by_many_requests_at_once_are_all_there_and_do_not_mix() {
    let mock = Mock::new(10, 0);
    // More requests than the database had connections before this change.
    let st = test_state(256, 256, 16).await;
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

// Not part of the normal run: it measures, so it is only read when asked for. Run with
// `cargo test --release -- --ignored --nocapture`. The runtime is the multi-thread one, because a
// single-threaded runtime would measure one worker's throughput rather than the gateway's. The limits are
// above what is sent, so nothing is refused here: a refused request is not measured, and a benchmark of
// refusals would say nothing about the request path.
async fn measure(mock: &Mock, count: usize, pool: u32) {
    // Every request has to be admitted, so the limits have to be at least as large as the batch.
    let st = test_state(count + 1, count + 1, pool).await;
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
