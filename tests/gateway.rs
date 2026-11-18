use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Router,
    body::Body,
    extract::Json,
    http::{self, HeaderValue, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use dreamgate::{build_router, db, limits, live, metrics, presence, state::State};
use futures_util::{future::join_all, stream};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::{Row, sqlite::SqlitePool, sqlite::SqlitePoolOptions};
use tower::ServiceExt;

async fn test_state() -> State {
    // One pooled connection keeps the in-memory database shared across the test.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    // As `db::connect` does it: the migration is what makes the indexes.
    db::migrate(&pool).await.unwrap();

    // Far above what any test in this file sends, so a test is not shaped by admission: as a result the main
    // suite only exercises it in the one test that fills a connection on purpose, through the capability that
    // connection states, to see where the request goes next. The tests that check admission itself build their
    // own state in tests/concurrency.rs. The time bounds are the ones the gateway runs with, because the mock
    // answers long before either of them.
    let limits = limits::Limits::new(
        1024,
        1024,
        Duration::from_secs(limits::DEFAULT_REQUEST_TIMEOUT_SECS),
        Duration::from_secs(limits::DEFAULT_STREAM_STALL_SECS),
        Duration::from_secs(limits::DEFAULT_CONNECT_TIMEOUT_SECS),
    );
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

// One SSE line for a tool-call delta, so the mock and the test that reads it build the same bytes.
fn delta_line(piece: &Value) -> String {
    let delta = json!({
        "choices": [{ "delta": { "tool_calls": [{ "function": { "name": "shell", "arguments": piece } }] } }],
    });
    format!("data: {delta}\n\n")
}

// The arguments of a tool call as an upstream would send them: OpenAI puts them in a JSON string, some
// servers send them already parsed, and a stream carries them in pieces across deltas.
fn tool_arguments(model: &str) -> Option<Vec<Value>> {
    match model {
        "calls-tool" => Some(vec![json!("echo hello")]),
        "parsed-arguments" => Some(vec![json!({ "command": "echo hello" })]),
        "split-arguments" => Some(vec![json!("{\"comma"), json!("nd\":\"nd\"}")]),
        "paused-arguments" => Some(vec![
            json!("{\"command\""),
            json!(":\"echo"),
            json!(" hello"),
            json!("\"}"),
        ]),
        _ => None,
    }
}

// An OpenAI-compatible upstream with several behaviours: it reports usage, reports none, reports a
// prompt cache, reports a usage block with no token counts in it, reports timings in the llama.cpp
// shape, answers with a reasoning section beside the answer, or answers with a tool call and nothing
// else. One of the tool call shapes sends its pieces with a gap between them, so a client can tell a
// line forwarded as it arrived from a line held until the stream ended.
async fn mock_chat(Json(body): Json<Value>) -> Response {
    let model = body.get("model").and_then(Value::as_str).unwrap_or("");
    // What the upstream says about the request. Only some of the models say anything about the prompt
    // cache, and only one gives timings, so a test can tell a field an engine reported from one it left
    // out, and both shapes are read from wherever they appear in the body.
    let usage = match model {
        "reports-usage" => Some(json!({"prompt_tokens": 11, "completion_tokens": 7})),
        "reports-cache" => Some(json!({
            "prompt_tokens": 11,
            "completion_tokens": 7,
            "prompt_tokens_details": { "cached_tokens": 4 }
        })),
        "usage-without-counts" => Some(json!({ "prompt_tokens_details": { "cached_tokens": 4 } })),
        _ => None,
    };
    let timings = (model == "llama-timings").then(|| {
        json!({
            "cache_n": 3,
            "prompt_n": 9,
            "prompt_ms": 120.5,
            "predicted_n": 5,
            "predicted_ms": 200.25
        })
    });
    let says_anything = usage.is_some() || timings.is_some();
    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
        let mut lines: Vec<String> = Vec::new();
        if model == "reasons" {
            lines.push(
                "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think hard\"}}]}\n\n"
                    .to_string(),
            );
        }
        if let Some(pieces) = tool_arguments(model) {
            for piece in pieces {
                lines.push(delta_line(&piece));
            }
        } else {
            lines.push(
                "data: {\"choices\":[{\"delta\":{\"content\":\"hello world\"}}]}\n\n".to_string(),
            );
        }
        if says_anything {
            // The last chunk of a stream is where a cache hit and the timings appear, so the mock puts
            // them there rather than earlier.
            let mut chunk = json!({ "choices": [] });
            if let Some(block) = usage.as_ref() {
                chunk["usage"] = block.clone();
            }
            if let Some(block) = timings.as_ref() {
                chunk["timings"] = block.clone();
            }
            lines.push(format!("data: {chunk}\n\n"));
        }
        lines.push("data: [DONE]\n\n".to_string());

        let mut out = Response::new(Body::empty());
        out.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        // Only the paused shape is streamed, because the test that reads arrivals is the reason it
        // exists; the others answer at once.
        if model == "paused-arguments" {
            *out.body_mut() = Body::from_stream(stream::unfold(lines, move |pending| async move {
                let line = pending.first()?.clone();
                let rest = pending[1..].to_vec();
                tokio::time::sleep(Duration::from_millis(150)).await;
                Some((Ok::<_, std::io::Error>(line.into_bytes()), rest))
            }));
            return out;
        }
        *out.body_mut() = Body::from(lines.concat());
        return out;
    }
    let message = if model == "reasons" {
        json!({ "content": "hello world", "reasoning_content": "think hard" })
    } else if let Some(pieces) = tool_arguments(model) {
        // An answer that is not a stream carries the whole arguments at once, so the fragment case can
        // only be read from the stream.
        json!({ "tool_calls": [{ "function": { "name": "shell", "arguments": pieces[0] } }] })
    } else {
        json!({ "content": "hello world" })
    };
    let mut payload = json!({ "choices": [{ "message": message }] });
    if let Some(block) = usage.as_ref() {
        payload["usage"] = block.clone();
    }
    if let Some(block) = timings.as_ref() {
        payload["timings"] = block.clone();
    }
    Json(payload).into_response()
}

async fn mock_models() -> Json<Value> {
    Json(json!({ "object": "list", "data": [{ "id": "test-model" }] }))
}

// An upstream that answers with the body it was sent, so a test can read exactly what the
// gateway forwarded.
async fn mock_echo(Json(body): Json<Value>) -> Json<Value> {
    Json(body)
}

// The legacy completions endpoint: the answer is text rather than a message, and a stream's chunks carry the
// text itself rather than a delta. It reports usage for the same model the chat mock does.
async fn mock_completions(Json(body): Json<Value>) -> Response {
    let reports = body.get("model").and_then(Value::as_str) == Some("reports-usage");
    let usage = json!({ "prompt_tokens": 11, "completion_tokens": 7 });
    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
        let mut lines = vec![
            format!(
                "data: {}\n\n",
                json!({ "choices": [{ "text": "hello there" }] })
            ),
            format!("data: {}\n\n", json!({ "choices": [{ "text": " world" }] })),
        ];
        if reports {
            lines.push(format!(
                "data: {}\n\n",
                json!({ "choices": [], "usage": usage })
            ));
        }
        lines.push("data: [DONE]\n\n".to_string());
        let mut out = Response::new(Body::from(lines.concat()));
        out.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        return out;
    }
    let mut answer = json!({ "choices": [{ "text": "hello there world" }] });
    if reports {
        answer["usage"] = usage;
    }
    Json(answer).into_response()
}

// An embeddings server: it reports the input it read and nothing generated, as OpenAI's does, unless the
// model is one that reports nothing. It also says whether it was sent anything about streaming.
async fn mock_embeddings(Json(body): Json<Value>) -> Json<Value> {
    let mut answer = json!({
        "data": [{ "embedding": [0.1, 0.2] }],
        "streaming_fields": body.get("stream_options").is_some(),
        "model_sent": body.get("model"),
    });
    if body.get("model").and_then(Value::as_str) == Some("reports-usage") {
        answer["usage"] = json!({ "prompt_tokens": 5, "total_tokens": 5 });
    }
    Json(answer)
}

async fn mock_upstream() -> String {
    let app = Router::new()
        .route("/v1/models", get(mock_models))
        .route("/v1/chat/completions", post(mock_chat))
        .route("/v1/completions", post(mock_completions))
        .route("/v1/embeddings", post(mock_embeddings));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn call(router: &Router, req: Request<Body>) -> Response {
    router.clone().oneshot(req).await.unwrap()
}

async fn json_body(response: Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn send_json(
    router: &Router,
    method: http::Method,
    path: &str,
    cookie: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = call(
        router,
        Request::builder()
            .method(method)
            .uri(path)
            .header("cookie", cookie)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await;
    let status = response.status();
    (status, json_body(response).await)
}

async fn post_json(router: &Router, path: &str, cookie: &str, body: Value) -> (StatusCode, Value) {
    send_json(router, http::Method::POST, path, cookie, body).await
}

async fn get_json(router: &Router, path: &str, cookie: &str) -> Value {
    let response = call(
        router,
        Request::get(path)
            .header("cookie", cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    json_body(response).await
}

async fn register(router: &Router, username: &str) -> String {
    let response = call(
        router,
        Request::post("/api/register")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "username": username, "password": "test-password" }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    cookie_of(&response)
}

fn cookie_of(response: &Response) -> String {
    response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

// Self-registration is closed by default once an account exists, so tests that need a second
// account open it first.
async fn open_registration(router: &Router, admin: &str) {
    let (status, _) = send_json(
        router,
        http::Method::PATCH,
        "/api/settings",
        admin,
        json!({ "open_registration": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

// A new account waits for approval by default and gets no session until an admin approves it, so
// tests that are not about approval turn it off before registering. What approval itself does is
// tested on its own.
async fn no_approval(router: &Router, admin: &str) {
    let (status, _) = send_json(
        router,
        http::Method::PATCH,
        "/api/settings",
        admin,
        json!({ "require_approval": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

async fn sign_in(router: &Router, username: &str, password: &str) -> String {
    let response = call(
        router,
        Request::post("/api/login")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "username": username, "password": password }).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    cookie_of(&response)
}

#[tokio::test]
async fn a_new_account_waits_for_approval_before_it_can_sign_in() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    assert_eq!(
        get_json(&router, "/api/settings", &admin).await["require_approval"],
        true
    );

    open_registration(&router, &admin).await;
    let (status, body) = post_json(
        &router,
        "/api/register",
        "",
        json!({ "username": "ben", "password": "test-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["pending"], true,
        "the second account is held, the first is not"
    );

    // Nothing is signed in yet, and signing in says why.
    assert_eq!(
        post_json(
            &router,
            "/api/login",
            "",
            json!({ "username": "ben", "password": "test-password"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    let id = body["id"].as_i64().unwrap();
    let (status, changed) = send_json(
        &router,
        http::Method::PATCH,
        &format!("/api/users/{id}"),
        &admin,
        json!({ "pending": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(changed["changed"], json!(["pending"]));

    let member = sign_in(&router, "ben", "test-password").await;
    assert_eq!(
        get_json(&router, "/api/me", &member).await["username"],
        "ben"
    );
}

#[tokio::test]
async fn turning_approval_off_lets_an_account_register_and_sign_in_at_once() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;

    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    // The helper asserts a session comes back with the registration, which is what an account
    // that is not held gets.
    let member = register(&router, "ben").await;
    assert_eq!(
        get_json(&router, "/api/me", &member).await["username"],
        "ben"
    );
}

async fn gateway_call(router: &Router, key: &str, body: Value) -> Response {
    call(
        router,
        Request::post("/v1/chat/completions")
            .header("authorization", format!("Bearer {key}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
}

// Usage is written by the stream task after the response completes, so poll for it.
async fn recorded_usage(pool: &SqlitePool) -> Vec<Value> {
    for _ in 0..50 {
        let rows = sqlx::query(
            "SELECT connection_id, model, prompt_tokens, completion_tokens, total_tokens, \
             cached_tokens, prompt_ms, generation_ms, status, estimated, streamed \
             FROM usage ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        if !rows.is_empty() {
            return rows
                .iter()
                .map(|row| {
                    json!({
                        "connection_id": row.get::<Option<i64>, _>("connection_id"),
                        "model": row.get::<Option<String>, _>("model"),
                        "prompt_tokens": row.get::<i64, _>("prompt_tokens"),
                        "completion_tokens": row.get::<i64, _>("completion_tokens"),
                        "total_tokens": row.get::<i64, _>("total_tokens"),
                        "cached_tokens": row.get::<Option<i64>, _>("cached_tokens"),
                        "prompt_ms": row.get::<Option<i64>, _>("prompt_ms"),
                        "generation_ms": row.get::<Option<i64>, _>("generation_ms"),
                        "status": row.get::<i64, _>("status"),
                        "estimated": row.get::<i64, _>("estimated") != 0,
                        "streamed": row.get::<i64, _>("streamed") != 0,
                    })
                })
                .collect();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no usage row was recorded");
}

#[tokio::test]
async fn first_registered_account_is_admin() {
    let st = test_state().await;
    let router = build_router(st.clone());

    let admin = register(&router, "ada").await;
    assert_eq!(get_json(&router, "/api/me", &admin).await["role"], "admin");

    // Self-registration is closed by default once an account exists, so the next one needs the admin.
    let closed = post_json(
        &router,
        "/api/register",
        "",
        json!({ "username": "ben", "password": "test-password" }),
    )
    .await;
    assert_eq!(closed.0, StatusCode::FORBIDDEN);

    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    assert_eq!(get_json(&router, "/api/me", &member).await["role"], "user");
}

#[tokio::test]
async fn a_taken_username_is_named_when_registering() {
    let st = test_state().await;
    let router = build_router(st.clone());

    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;

    // Approval is on, so this account is held, and a held account's name is taken all the same.
    let (status, body) = post_json(
        &router,
        "/api/register",
        "",
        json!({ "username": "ben", "password": "test-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pending"], true);

    // The entry screen shows the response's message when it is a short sentence, so this is what a
    // person reads rather than the driver's text. The name is trimmed before it is stored, and the
    // trimmed name is the one reported.
    for attempt in ["ben", " ben "] {
        let (status, body) = post_json(
            &router,
            "/api/register",
            "",
            json!({ "username": attempt, "password": "test-password" }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            body["error"]["message"],
            "the username ben is already taken"
        );
    }

    // An insert that fails for another reason is a server error, not a taken name. A trigger that
    // refuses every insert stands in for a database that cannot take the row.
    sqlx::query(
        "CREATE TRIGGER refuse_accounts BEFORE INSERT ON users BEGIN SELECT RAISE(ABORT, 'refused'); END",
    )
    .execute(&st.pool)
    .await
    .unwrap();
    let (status, body) = post_json(
        &router,
        "/api/register",
        "",
        json!({ "username": "cy", "password": "test-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"]["type"], "server_error");
    assert!(
        !body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("already taken")
    );
}

// The account races are between statements on different connections, which the single pooled connection of
// `test_state` would put in a line by itself, so these tests run against a shared in-memory database with as many
// connections as the gateway opens by default. Each test names its own database, because they run at once.
async fn shared_state(name: &str) -> State {
    let mut st = test_state().await;
    st.pool = db::connect(
        &format!("sqlite://file:{name}?mode=memory&cache=shared"),
        db::DEFAULT_POOL_SIZE,
    )
    .await
    .unwrap();
    st
}

// The first account owns the instance. The role used to be decided by counting the accounts before a password
// hash that takes tens of milliseconds, so registrations that arrived together all counted none and were all made
// admin; the insert decides it now, in the same step as it writes.
#[tokio::test]
async fn registrations_at_once_on_an_empty_instance_make_one_admin() {
    let st = shared_state("registrations_at_once").await;
    let router = build_router(st.clone());

    let names: Vec<String> = (0..12).map(|n| format!("racer{n}")).collect();
    let answers = join_all(names.iter().map(|name| {
        post_json(
            &router,
            "/api/register",
            "",
            json!({ "username": name, "password": "test-password" }),
        )
    }))
    .await;

    let admitted: Vec<&Value> = answers
        .iter()
        .filter(|(status, _)| *status == StatusCode::OK)
        .map(|(_, body)| body)
        .collect();
    assert_eq!(admitted.len(), 1, "one registration owns the instance");
    assert_eq!(admitted[0]["role"], "admin");
    // The first account is not held for approval, because there is no one to approve it.
    assert!(admitted[0]["pending"].is_null());
    // Once there is an admin, registration is closed by default, and the others are told so.
    for (status, body) in answers
        .iter()
        .filter(|(status, _)| *status != StatusCode::OK)
    {
        assert_eq!(*status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"]["type"], "registration_closed");
    }
    assert_eq!(count(&st.pool, "users").await, 1);
    let admins =
        sqlx::query("SELECT COUNT(*) AS n FROM users WHERE role = 'admin' AND pending = 0")
            .fetch_one(&st.pool)
            .await
            .unwrap()
            .get::<i64, _>("n");
    assert_eq!(admins, 1);
}

#[tokio::test]
async fn registration_setting_is_read_before_sign_in_and_only_an_admin_can_change_it() {
    let st = test_state().await;
    let router = build_router(st.clone());

    // The entry screen asks before anyone is signed in, and with no accounts yet the first one
    // is always allowed.
    let before = get_json(&router, "/api/settings", "").await;
    assert_eq!(before["open_registration"], true);
    assert_eq!(before["has_accounts"], false);

    let admin = register(&router, "ada").await;
    let after = get_json(&router, "/api/settings", "").await;
    assert_eq!(
        after["open_registration"], false,
        "closed by default after the first account"
    );
    assert_eq!(after["has_accounts"], true);
    assert_eq!(
        after["require_approval"], true,
        "approval is on by default once there is an admin to give it"
    );

    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        "/api/settings",
        &admin,
        json!({ "open_registration": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get_json(&router, "/api/settings", &admin).await["open_registration"],
        true
    );

    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        "/api/settings",
        &member,
        json!({ "require_approval": true }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "only an admin can change approval"
    );
    assert_eq!(
        get_json(&router, "/api/settings", &member).await["require_approval"],
        false,
        "the admin's setting stands"
    );
}

#[tokio::test]
async fn records_usage_reported_by_the_upstream() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    let (status, connection) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    assert_eq!(status, StatusCode::OK);
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["prompt_tokens"], 11);
    assert_eq!(usage[0]["completion_tokens"], 7);
    assert_eq!(usage[0]["total_tokens"], 18);
    assert_eq!(usage[0]["estimated"], false);
    assert_eq!(usage[0]["connection_id"], connection["id"]);
    assert_eq!(
        usage[0]["model"], "reports-usage",
        "the key's connection default model is applied"
    );
}

#[tokio::test]
async fn records_the_prompt_cache_and_timings_an_engine_reports() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    // Four engines with four shapes: one reports the prompt cache in the OpenAI-shaped usage block,
    // one reports it and the timings as llama.cpp does, one reports only the token counts, and one
    // reports a usage block with no counts in it.
    for model in [
        "reports-cache",
        "llama-timings",
        "reports-usage",
        "usage-without-counts",
    ] {
        let response = gateway_call(
            &router,
            &key,
            json!({ "model": model, "messages": [{ "role": "user", "content": "hi" }] }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["cached_tokens"], 4);
    assert_eq!(usage[0]["estimated"], false);
    assert_eq!(usage[1]["cached_tokens"], 3);
    assert_eq!(
        usage[1]["prompt_ms"], 121,
        "a timing is stored as whole milliseconds"
    );
    assert_eq!(usage[1]["generation_ms"], 200);
    // A cache hit or a timing on its own does not make the counts figures the upstream gave, so the row
    // for an engine that reports timings and no counts is still an estimate.
    assert_eq!(usage[1]["estimated"], true);
    assert_eq!(usage[2]["cached_tokens"], Value::Null);
    assert_eq!(usage[2]["prompt_ms"], Value::Null);
    assert_eq!(
        usage[2]["generation_ms"],
        Value::Null,
        "an engine that reports nothing leaves the fields unknown, not zero"
    );
    assert_eq!(
        usage[3]["estimated"], true,
        "a usage block with no counts in it is not a report of counts"
    );
    assert_eq!(usage[3]["completion_tokens"], 4, "two words in the answer");
    assert_eq!(
        usage[3]["cached_tokens"], 4,
        "the cache it did report is stored"
    );

    // The last chunk of a stream is where a cache hit and the timings appear, so a streamed request has
    // to read them there.
    for model in ["reports-cache", "llama-timings"] {
        let response = gateway_call(
            &router,
            &key,
            json!({
                "model": model,
                "stream": true,
                "messages": [{ "role": "user", "content": "hi" }],
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();
    }

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[4]["cached_tokens"], 4);
    assert_eq!(usage[5]["cached_tokens"], 3);
    assert_eq!(usage[5]["prompt_ms"], 121);
    assert_eq!(usage[5]["generation_ms"], 200);
}

#[tokio::test]
async fn estimates_tokens_when_the_upstream_reports_none() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "silent" }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["estimated"], true);
    assert!(usage[0]["prompt_tokens"].as_i64().unwrap() > 0);
    assert_eq!(
        usage[0]["completion_tokens"], 4,
        "two words in the answer, two subword pieces each"
    );
}

#[tokio::test]
async fn estimates_the_arguments_of_a_tool_call_when_the_upstream_reports_none() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    // No default model on the connection, so the model in the request chooses which shape of tool call
    // the mock answers with.
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    // A turn that is only a tool call is text the model produced, so its estimate is not zero. The
    // numbers are the estimator's own arithmetic: "echo hello" is one piece for the first word and two
    // for the second, and the JSON text of the parsed form adds a piece for the braces and the key.
    for (shape, expected) in [("calls-tool", 3), ("parsed-arguments", 6)] {
        for stream in [true, false] {
            let response = gateway_call(
                &router,
                &key,
                json!({
                    "model": shape,
                    "messages": [{ "role": "user", "content": "hi" }],
                    "stream": stream,
                }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            if stream {
                // The row is written when the stream ends, so the response has to be read for it.
                response.into_body().collect().await.unwrap();
            }

            let usage = recorded_usage(&st.pool).await;
            let row = usage.last().unwrap();
            assert_eq!(row["estimated"], true, "{row}");
            assert_eq!(
                row["completion_tokens"], expected,
                "the arguments of the tool call"
            );
            assert_eq!(row["streamed"], stream);
        }
    }
}

#[tokio::test]
async fn counts_each_piece_of_a_fragmented_tool_call_as_the_stream_sent_it() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "split-arguments" }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    // The same text as one piece would be 4, and the mock sends it in two. The pieces are joined with a
    // space before the estimate counts per whitespace chunk, so a delta boundary is counted as a word: 2
    // for `{"comma` and 3 for `nd":"nd"}`. That is how a rough estimate can sit above what an upstream
    // reports, and it is asserted here rather than left assumed.
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }], "stream": true }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await.unwrap();

    let usage = recorded_usage(&st.pool).await;
    let row = usage.last().unwrap();
    assert_eq!(row["estimated"], true, "{row}");
    assert_eq!(row["completion_tokens"], 5);
}

#[tokio::test]
async fn forwards_a_tool_call_stream_as_the_pieces_arrive_rather_than_at_the_end() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "paused-arguments" }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({
            "messages": [{ "role": "user", "content": "hi" }],
            "stream": true,
            "tools": [{ "type": "function", "function": { "name": "shell" } }],
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    // The upstream puts one line on the wire every 150 ms, so every gap between the arrivals here is one
    // the upstream made. A gateway that held the body until the stream ended would deliver the pieces in
    // one read, with no gaps at all. The margin is wide so this says something about the behaviour and
    // not about the speed of the machine.
    let started = Instant::now();
    let mut body = response.into_body();
    let mut arrivals = Vec::new();
    let mut received = String::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.unwrap();
        let Some(bytes) = frame.data_ref() else {
            continue;
        };
        received.push_str(&String::from_utf8_lossy(bytes));
        arrivals.push(started.elapsed());
    }

    assert_eq!(arrivals.len(), 5, "the pieces and the end of the stream");
    let gaps = arrivals
        .windows(2)
        .filter(|gap| gap[1] - gap[0] >= Duration::from_millis(100))
        .count();
    assert!(
        gaps >= 3,
        "the pieces arrived in one read rather than as they were sent: {arrivals:?}"
    );
    // The bytes reach the client unchanged, in the order the upstream sent them.
    let expected: String = tool_arguments("paused-arguments")
        .unwrap()
        .iter()
        .map(delta_line)
        .chain(std::iter::once("data: [DONE]\n\n".to_string()))
        .collect();
    assert_eq!(received, expected);
}

#[tokio::test]
async fn streams_sse_through_unchanged_and_records_at_the_end() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("data: {\"choices\":[{\"delta\":{\"content\":\"hello world\"}}]}"),
        "the stream is passed through unchanged: {text}"
    );

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["streamed"], true);
    assert_eq!(usage[0]["completion_tokens"], 7);
}

#[tokio::test]
async fn counts_streamed_reasoning_as_generated_text_in_the_estimate() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reasons" }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think hard\"}}]}"),
        "the reasoning line is passed through unchanged: {text}"
    );

    let usage = recorded_usage(&st.pool).await;
    // Reasoning is text the model produced, so it counts even though it is not the answer:
    // "think hard" is three subword pieces, "hello world" is four.
    assert_eq!(usage[0]["estimated"], true);
    assert_eq!(usage[0]["completion_tokens"], 7);
}

#[tokio::test]
async fn counts_reasoning_in_the_estimate_when_the_upstream_reports_none() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reasons" }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["estimated"], true);
    assert_eq!(
        usage[0]["completion_tokens"], 7,
        "the answer and the reasoning together"
    );
}

#[tokio::test]
async fn reasoning_effort_is_forwarded_to_the_upstream_unchanged() {
    let echo = Router::new().route("/v1/chat/completions", post(mock_echo));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, echo).await.unwrap() });

    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    // DreamGate does not interpret reasoning levels, it forwards the request body as it is, so a
    // client's reasoning_effort reaches the runtime.
    let response = gateway_call(
        &router,
        &key,
        json!({
            "model": "reasoning-model",
            "messages": [{ "role": "user", "content": "hi" }],
            "reasoning_effort": "high",
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let sent = json_body(response).await;
    assert_eq!(sent["reasoning_effort"], "high");
    assert_eq!(sent["model"], "reasoning-model");
}

// Nothing names a model here: none is pinned to the key, the connection has no default and the request carries
// none. The body reaches the upstream without one, as the client sent it, so the upstream answers with its own
// default rather than with a name the gateway made up; the stream_options a stream is given is the only field
// written.
#[tokio::test]
async fn a_request_that_names_no_model_reaches_the_upstream_without_one() {
    let echo = Router::new().route("/v1/chat/completions", post(mock_echo));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, echo).await.unwrap() });

    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();
    let messages = json!([{ "role": "user", "content": "hi" }]);

    let response = gateway_call(&router, &key, json!({ "messages": messages })).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await,
        json!({ "messages": messages }),
        "the body arrived as the client sent it"
    );

    // The echo answers a stream with the one body it was sent, and the gateway passes that through.
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": messages, "stream": true }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await,
        json!({ "messages": messages, "stream": true, "stream_options": { "include_usage": true } }),
        "the stream arrived with stream_options added and nothing else"
    );

    // The rows name the model the gateway sent, so they say unknown when it sent none.
    let deadline = Instant::now() + Duration::from_secs(5);
    let usage = loop {
        let usage = recorded_usage(&st.pool).await;
        if usage.len() == 2 {
            break usage;
        }
        assert!(
            Instant::now() < deadline,
            "the streamed request was not recorded"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    for row in &usage {
        assert_eq!(row["model"], "unknown");
    }
}

#[tokio::test]
async fn records_the_model_the_client_asked_for_when_streaming() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    // Nothing is pinned on the connection or the key, so the recorded model has to come
    // from the request itself.
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "model": "reports-usage", "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await.unwrap();

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["model"], "reports-usage");
}

#[tokio::test]
async fn fails_over_to_a_healthy_connection() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    // Nothing listens on port 1, so the first attempt fails at the transport level.
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 0 }),
    )
    .await;
    let (_, healthy) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "live", "base_url": base_url, "default_model": "reports-usage", "priority": 1 }),
    )
    .await;

    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["connection_id"], healthy["id"]);
    assert_eq!(usage[0]["completion_tokens"], 7);
}

// The health a check would have written, set directly, so a test decides what the gateway reads without
// waiting for a round.
async fn mark_health(pool: &SqlitePool, connection: &Value, health: &str) {
    sqlx::query("UPDATE connections SET health = ? WHERE id = ?")
        .bind(health)
        .bind(connection["id"].as_i64().unwrap())
        .execute(pool)
        .await
        .unwrap();
}

// A check is only every 30 seconds, so a connection marked down may be answering again. When every connection
// is marked down they are still tried in their order, rather than all skipped and the request answered as if
// nothing could be reached.
#[tokio::test]
async fn connections_that_are_all_marked_down_are_still_tried() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    // Nothing listens on port 1, so the first is down in fact; the second is marked down but answers.
    let (_, dead) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 0 }),
    )
    .await;
    let (_, stale) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "stale", "base_url": base_url, "default_model": "reports-usage", "priority": 1 }),
    )
    .await;
    mark_health(&st.pool, &dead, "down").await;
    mark_health(&st.pool, &stale, "down").await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(
        usage.len(),
        1,
        "a request is one row, whichever connections it tried"
    );
    assert_eq!(usage[0]["connection_id"], stale["id"]);
    assert_eq!(usage[0]["status"], 200);
    assert_eq!(usage[0]["completion_tokens"], 7);
}

// Being tried last is what marked down means: a connection that is not marked down is asked first, even when
// the one marked down has the better priority or is the one the key is bound to.
#[tokio::test]
async fn a_connection_that_is_up_is_tried_before_one_marked_down() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    // Both answer, so the row says which one was asked first rather than which one was reachable.
    let (_, stale) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "stale", "base_url": base_url, "default_model": "reports-usage", "priority": 0 }),
    )
    .await;
    let (_, up) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "up", "base_url": base_url, "default_model": "reports-usage", "priority": 1 }),
    )
    .await;
    mark_health(&st.pool, &stale, "down").await;
    mark_health(&st.pool, &up, "ok").await;
    let (_, free) = post_json(&router, "/api/keys", &cookie, json!({ "name": "free key" })).await;
    let (_, bound) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "bound key", "connection_id": stale["id"] }),
    )
    .await;

    for created in [&free, &bound] {
        let response = gateway_call(
            &router,
            created["key"].as_str().unwrap(),
            json!({ "messages": [{ "role": "user", "content": "hi" }] }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage.len(), 2);
    for row in &usage {
        assert_eq!(row["connection_id"], up["id"]);
    }
}

// A connection that is full is passed over as it always was, and one marked down is still a connection the
// request can use, so it is asked rather than the request being answered as an overload. Only when it is full
// as well is the request turned away, and then the answer names both.
#[tokio::test]
async fn a_full_connection_passes_the_request_on_to_one_marked_down() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    let (_, busy) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "busy", "base_url": base_url, "default_model": "reports-usage", "priority": 0, "max_concurrent": 1 }),
    )
    .await;
    let (_, stale) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "stale", "base_url": base_url, "default_model": "reports-usage", "priority": 1, "max_concurrent": 1 }),
    )
    .await;
    mark_health(&st.pool, &busy, "ok").await;
    mark_health(&st.pool, &stale, "down").await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();
    let body = json!({ "messages": [{ "role": "user", "content": "hi" }] });

    // The slot is taken from the semaphore the gateway admits by, so the connection is full without a request
    // being held open on it.
    let slot = |connection: &Value| {
        st.limits
            .for_connection(connection["id"].as_i64().unwrap(), Some(1))
            .try_acquire_owned()
            .unwrap()
    };
    let _busy_slot = slot(&busy);
    let response = gateway_call(&router, &key, body.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0]["connection_id"], stale["id"]);

    let _stale_slot = slot(&stale);
    let response = gateway_call(&router, &key, body).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let refused = json_body(response).await;
    assert_eq!(refused["error"]["type"], "connection_full");
    assert!(
        refused["error"]["message"]
            .as_str()
            .unwrap()
            .ends_with("busy, stale"),
        "the answer names every connection that was full: {refused}"
    );
    assert_eq!(
        recorded_usage(&st.pool).await.len(),
        1,
        "a request that reached no connection is not recorded"
    );
}

// The model list reads the same order a completion does, so a connection marked down is not the one it
// asks while another is up: the Sandbox reads its models from here, and would have none while chat works.
#[tokio::test]
async fn the_model_list_asks_a_connection_that_is_up_before_one_marked_down() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    let (_, dead) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 0 }),
    )
    .await;
    let (_, up) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "up", "base_url": base_url, "priority": 1 }),
    )
    .await;
    mark_health(&st.pool, &dead, "down").await;
    mark_health(&st.pool, &up, "ok").await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;

    let response = models_call(&router, created["key"].as_str().unwrap()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["data"][0]["id"], "test-model");
}

// When the connection that is up is full and the one marked down then fails, the answer is that failure,
// with the full connection named beside it rather than left out.
#[tokio::test]
async fn a_failure_after_a_full_connection_names_the_full_one_as_well() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;

    let (_, busy) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "busy", "base_url": base_url, "default_model": "reports-usage", "priority": 0, "max_concurrent": 1 }),
    )
    .await;
    let (_, dead) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 1 }),
    )
    .await;
    mark_health(&st.pool, &busy, "ok").await;
    mark_health(&st.pool, &dead, "down").await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let _busy_slot = st
        .limits
        .for_connection(busy["id"].as_i64().unwrap(), Some(1))
        .try_acquire_owned()
        .unwrap();

    let response = gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let message = json_body(response).await["error"]["message"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(message.starts_with("dead: "), "{message}");
    assert!(
        message.ends_with("at their limit of requests at once: busy"),
        "{message}"
    );
    let usage = recorded_usage(&st.pool).await;
    assert_eq!(
        usage.len(),
        1,
        "a request that reached a connection is recorded"
    );
    assert_eq!(usage[0]["status"], 502);
}

// A key bound to a connection that does not exist is a mistake in what was sent, so it is answered as one
// rather than as a failure of the server, whether the key is being made or changed.
#[tokio::test]
async fn a_key_bound_to_a_connection_that_does_not_exist_is_told_so() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;

    let (status, body) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "orphan", "connection_id": 4242 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["message"], "no connection with that id");

    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "plain" })).await;
    let (status, body) = send_json(
        &router,
        http::Method::PATCH,
        &format!("/api/keys/{}", created["id"]),
        &cookie,
        json!({ "connection_id": 4242 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["message"], "no connection with that id");
}

// A key holder is told which connection failed, not where it is: the HTTP client's error ends in the
// full upstream address, which names an internal host and port, and any credential an upstream takes
// in its query string.
#[tokio::test]
async fn a_failed_upstream_is_named_without_its_address() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;

    // Nothing listens on port 1, so the request fails at the transport level.
    let (status, _) = post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1/?token=upstream-secret" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, created) = post_json(&router, "/api/keys", &admin, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let completion = gateway_call(
        &router,
        &key,
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(completion.status(), StatusCode::BAD_GATEWAY);
    let models = models_call(&router, &key).await;
    assert_eq!(models.status(), StatusCode::BAD_GATEWAY);

    for response in [completion, models] {
        let body = json_body(response).await;
        let message = body["error"]["message"].as_str().unwrap().to_string();
        assert!(message.starts_with("dead: "), "{message}");
        for part in ["127.0.0.1", "upstream-secret", "token", "http"] {
            assert!(!message.contains(part), "{message}");
        }
    }
}

// The round of health checks is started by the binary rather than by a route, so no other test reaches
// it. A connection is created without a check, so its row reads unknown until a round has looked at it.
#[tokio::test]
async fn the_health_check_round_marks_a_connection_that_answers() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let (_, created) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "live", "base_url": base_url }),
    )
    .await;
    let id = created["id"].as_i64().unwrap();

    // The first round runs as soon as the task starts, and the mock answers at once, so the window is
    // far longer than the round needs. A round that stops is only started again after 30 seconds, so a
    // round that cannot read the row is caught here as a row that still reads unknown.
    // A failure older than the figures' window is removed after the round, and a recent one is kept.
    for at in ["2020-01-01T00:00:00Z".to_string(), dreamgate::clock::now()] {
        sqlx::query(
            "INSERT INTO connection_failures (connection_id, kind, created_at) VALUES (?, 'unreachable', ?)",
        )
        .bind(id)
        .bind(at)
        .execute(&st.pool)
        .await
        .unwrap();
    }
    let round = tokio::spawn(dreamgate::registry::health_checker(st.clone()));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let health: String = sqlx::query_scalar("SELECT health FROM connections WHERE id = ?")
            .bind(id)
            .fetch_one(&st.pool)
            .await
            .unwrap();
        let failures: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM connection_failures")
            .fetch_one(&st.pool)
            .await
            .unwrap();
        if health == "ok" && failures == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the connection still reads {health}, with {failures} failures, after the first round"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    round.abort();
}

#[tokio::test]
async fn per_key_rate_limit_blocks_the_third_request() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;

    let (_, created) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "limited", "rpm_limit": 2 }),
    )
    .await;
    let key = created["key"].as_str().unwrap().to_string();

    for _ in 0..2 {
        let response = gateway_call(
            &router,
            &key,
            json!({ "messages": [{ "role": "user", "content": "hi" }] }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn key_limits_must_be_positive() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;

    // A limit of 0 would refuse every request, so it is rejected rather than stored.
    let (status, _) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "zero", "token_cap": 0 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "negative", "rpm_limit": -1 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "unset limits" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_connection_can_state_its_capability_or_leave_it_out_but_not_state_zero() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;

    // A capability of 0 would admit nothing, which is not what leaving it out means, so it is rejected
    // rather than stored, as a key limit of 0 is.
    let (status, _) = post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "zero", "base_url": "http://127.0.0.1:8080", "max_concurrent": 0 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Leaving it out is not a limit of one: it is nothing stated, and the default applies to that
    // connection alone.
    let (status, body) = post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "unstated", "base_url": "http://127.0.0.1:8080" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["connections"][0]["max_concurrent"], json!(null));
}

// A name that is taken means something to the admin who typed it, so it is named; anything else the
// database refuses is a server error with a reference, and the driver's text, which names tables and
// constraints, stays in the log.
#[tokio::test]
async fn a_duplicate_connection_name_is_named_and_no_database_failure_carries_the_driver_text() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;

    let (status, _) = post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "local", "base_url": "http://127.0.0.1:8080" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // The name is trimmed before it is stored, and the trimmed name is the one reported.
    let (status, body) = post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": " local ", "base_url": "http://127.0.0.1:8081" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["error"]["message"],
        "a connection named local already exists"
    );

    // Triggers that refuse every insert stand in for a database that cannot take the row. Neither
    // failure is a taken name, and neither answer carries what the database said.
    for table in ["connections", "users"] {
        let sql = format!(
            "CREATE TRIGGER refuse_{table} BEFORE INSERT ON {table} \
             BEGIN SELECT RAISE(ABORT, 'refused by trigger on {table}'); END"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&st.pool)
            .await
            .unwrap();
    }
    let (connection_status, connection) = post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "another", "base_url": "http://127.0.0.1:8082" }),
    )
    .await;
    let (account_status, account) =
        post_json(&router, "/api/users", &admin, json!({ "username": "ben" })).await;
    for (status, body) in [(connection_status, connection), (account_status, account)] {
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"]["type"], "server_error");
        let message = body["error"]["message"].as_str().unwrap();
        assert!(message.contains("reference"), "{message}");
        for part in ["trigger", "already", "constraint", "connections", "users"] {
            assert!(!message.contains(part), "{message}");
        }
    }
}

#[tokio::test]
async fn the_connection_limit_is_added_to_a_database_written_before_it() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }

    // A connections table as it was before a capability could be stated, with a row written then.
    sqlx::query("DROP TABLE connections")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE connections (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            base_url TEXT NOT NULL,
            api_key TEXT,
            default_model TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 0,
            health TEXT NOT NULL DEFAULT 'unknown',
            last_checked_at TEXT,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO connections (name, base_url, created_at) VALUES ('old', 'http://127.0.0.1:8080', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    db::migrate(&pool).await.unwrap();

    // The row that was already there reads as nothing stated, which is not a limit of one.
    let row = sqlx::query("SELECT name, max_concurrent FROM connections")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("name"), "old");
    assert_eq!(
        row.get::<Option<i64>, _>("max_concurrent"),
        None,
        "a connection written before the change reads as nothing stated"
    );
    let models = sqlx::query("SELECT models FROM connections")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<Option<String>, _>("models");
    assert_eq!(
        models, None,
        "a connection written before its list was kept reads as no list read"
    );

    // Running it again on a table that is already current changes nothing.
    db::migrate(&pool).await.unwrap();
    assert_eq!(
        sqlx::query("SELECT max_concurrent FROM connections")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<Option<i64>, _>("max_concurrent"),
        None
    );

    // A connection can state its capability now.
    sqlx::query(
        "INSERT INTO connections (name, base_url, max_concurrent, created_at) \
         VALUES ('stated', 'http://127.0.0.1:11434', 3, '2026-01-01T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        sqlx::query("SELECT max_concurrent FROM connections WHERE name = 'stated'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<Option<i64>, _>("max_concurrent"),
        Some(3)
    );
}

#[tokio::test]
async fn the_keys_list_is_the_callers_own_and_only_an_admin_can_read_the_others() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;

    post_json(&router, "/api/keys", &admin, json!({ "name": "admin key" })).await;
    post_json(
        &router,
        "/api/keys",
        &member,
        json!({ "name": "member key" }),
    )
    .await;

    // The Keys page shows one table, so the main list is always the caller's own and another
    // account's keys come from a read an admin can make on their own.
    let own = get_json(&router, "/api/keys", &admin).await;
    assert_eq!(
        own["keys"].as_array().unwrap().len(),
        1,
        "an admin's own list is their own"
    );
    assert_eq!(own["keys"][0]["name"], "admin key");

    let others = get_json(&router, "/api/keys/others", &admin).await;
    assert_eq!(others["keys"].as_array().unwrap().len(), 1);
    assert_eq!(
        others["keys"][0]["owner"], "ben",
        "the other list names who a key belongs to"
    );

    let denied = get_json(&router, "/api/keys/others", &member).await;
    assert_eq!(
        denied["error"]["message"],
        "only an admin can see keys from other accounts"
    );
}

#[tokio::test]
async fn the_key_account_and_model_figures_carry_the_input_and_the_output_apart() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    // An account an admin made has no usage, and still has a row.
    post_json(&router, "/api/users", &admin, json!({ "username": "cy" })).await;

    // The mock reports 11 tokens in and 7 out for every request.
    let (_, used) = post_json(&router, "/api/keys", &admin, json!({ "name": "used" })).await;
    post_json(&router, "/api/keys", &admin, json!({ "name": "unused" })).await;
    let (_, member_key) = post_json(
        &router,
        "/api/keys",
        &member,
        json!({ "name": "member key" }),
    )
    .await;
    for key in [&used, &used, &member_key] {
        gateway_call(
            &router,
            key["key"].as_str().unwrap(),
            json!({ "messages": [{ "role": "user", "content": "hi" }] }),
        )
        .await;
    }

    let own = get_json(&router, "/api/keys", &admin).await;
    let named = |list: &Value, name: &str| {
        list["keys"]
            .as_array()
            .unwrap()
            .iter()
            .find(|key| key["name"] == name)
            .cloned()
            .unwrap()
    };
    let key = named(&own, "used");
    assert_eq!(key["prompt_tokens"], 22);
    assert_eq!(key["completion_tokens"], 14);
    assert_eq!(key["tokens"], 36, "the total a key's cap counts stays");
    let idle = named(&own, "unused");
    assert_eq!(
        (&idle["prompt_tokens"], &idle["completion_tokens"]),
        (&json!(0), &json!(0)),
        "a key that has not been used reads zero on both sides, not null"
    );
    let others = get_json(&router, "/api/keys/others", &admin).await;
    let theirs = named(&others, "member key");
    assert_eq!(theirs["prompt_tokens"], 11);
    assert_eq!(theirs["completion_tokens"], 7);

    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    let models = summary["models"].as_array().unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["prompt_tokens"], 33);
    assert_eq!(models[0]["completion_tokens"], 21);
    assert_eq!(models[0]["tokens"], 54);
    let member_summary = get_json(&router, "/api/stats/summary", &member).await;
    assert_eq!(
        member_summary["models"][0]["prompt_tokens"], 11,
        "a member's models are their own"
    );
    assert_eq!(member_summary["models"][0]["completion_tokens"], 7);

    // A recorded request outlives its key, so the account's split stands when the key goes.
    let (status, _) = send_json(
        &router,
        http::Method::DELETE,
        &format!("/api/keys/{}", used["id"]),
        &admin,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    let account = |name: &str| {
        summary["users"]
            .as_array()
            .unwrap()
            .iter()
            .find(|user| user["username"] == name)
            .cloned()
            .unwrap()
    };
    let ada = account("ada");
    assert_eq!(ada["prompt_tokens"], 22);
    assert_eq!(ada["completion_tokens"], 14);
    assert_eq!(
        ada["used_today"], 36,
        "the day figure is the input and the output together, as the daily limit counts it"
    );
    let ben = account("ben");
    assert_eq!(ben["prompt_tokens"], 11);
    assert_eq!(ben["completion_tokens"], 7);
    let cy = account("cy");
    assert_eq!(
        (
            &cy["prompt_tokens"],
            &cy["completion_tokens"],
            &cy["used_today"]
        ),
        (&json!(0), &json!(0), &json!(0)),
        "an account with no usage reads zero on both sides, not null"
    );
}

#[tokio::test]
async fn deleting_a_key_leaves_the_usage_recorded_for_it() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;

    let (_, created) = post_json(&router, "/api/keys", &admin, json!({ "name": "test key" })).await;
    gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    let before = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(before["self"]["total_tokens"], 18);

    let key_id = created["id"].as_i64().unwrap();
    let (status, _) = send_json(
        &router,
        http::Method::DELETE,
        &format!("/api/keys/{key_id}"),
        &admin,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The counters are what crossed the gate, so removing the key cannot make them smaller.
    let after = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(
        after["self"]["total_tokens"], 18,
        "the account total stands"
    );
    assert_eq!(
        after["global"]["total_tokens"], 18,
        "the platform total stands"
    );

    let row = sqlx::query("SELECT api_key_id FROM usage")
        .fetch_one(&st.pool)
        .await
        .unwrap();
    assert_eq!(
        row.get::<Option<i64>, _>("api_key_id"),
        None,
        "the row is left without its key"
    );

    let recent = get_json(&router, "/api/stats/recent", &admin).await;
    assert_eq!(
        recent["events"][0]["key"],
        Value::Null,
        "the activity row has no key name left"
    );
}

#[tokio::test]
async fn per_key_token_cap_blocks_the_next_request() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;

    let (_, created) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "capped", "token_cap": 20 }),
    )
    .await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["total_tokens"], 18);

    // Lower the cap below what the first call already used, so the next call is refused.
    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        &format!("/api/keys/{}", created["id"]),
        &cookie,
        json!({ "token_cap": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    // The cap counts the input and the output together, so the refusal says so.
    assert_eq!(
        json_body(response).await["error"]["message"],
        "key 'capped' has reached its token cap of 10 input and output tokens"
    );
}

#[tokio::test]
async fn account_daily_limit_blocks_the_next_request() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;

    // The key has no cap of its own, so only the account limit can stop it.
    let (_, created) = post_json(&router, "/api/keys", &admin, json!({ "name": "uncapped" })).await;
    let key = created["key"].as_str().unwrap().to_string();
    gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["total_tokens"], 18);

    let me = get_json(&router, "/api/me", &admin).await;
    // The limit has to sit below what the first call already used, otherwise the account is still under it.
    let (status, body) = send_json(
        &router,
        http::Method::PATCH,
        &format!("/api/users/{}", me["id"]),
        &admin,
        json!({ "daily_token_cap": 15 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["daily_token_cap"]));

    // 18 tokens are already counted for today, so the next request is refused.
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        json_body(response).await["error"]["message"],
        "account 'ada' has used its daily token limit of 15 input and output tokens"
    );
}

#[tokio::test]
async fn account_daily_limit_counts_only_the_current_day() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;

    let (_, created) = post_json(&router, "/api/keys", &admin, json!({ "name": "uncapped" })).await;
    let key = created["key"].as_str().unwrap().to_string();
    gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;

    let me = get_json(&router, "/api/me", &admin).await;
    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        &format!("/api/users/{}", me["id"]),
        &admin,
        json!({ "daily_token_cap": 15 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The same tokens dated to an earlier day are outside the window the limit counts.
    sqlx::query("UPDATE usage SET created_at = datetime('now', '-1 day') WHERE user_id = ?")
        .bind(me["id"].as_i64().unwrap())
        .execute(&st.pool)
        .await
        .unwrap();
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(summary["users"][0]["used_today"], 18);
    assert_eq!(summary["users"][0]["tokens"], 36);
}

#[tokio::test]
async fn only_an_admin_can_set_an_account_cap() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let member_id = get_json(&router, "/api/me", &member).await;
    let path = format!("/api/users/{}", member_id["id"]);

    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        &path,
        &member,
        json!({ "daily_token_cap": 100 }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, body) = send_json(
        &router,
        http::Method::PATCH,
        &path,
        &admin,
        json!({ "daily_token_cap": 100 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["daily_token_cap"]));

    // An empty value clears the limit, which COALESCE would not allow.
    let (status, body) = send_json(
        &router,
        http::Method::PATCH,
        &path,
        &admin,
        json!({ "daily_token_cap": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["daily_token_cap"]));
    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    let ben = summary["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|user| user["username"] == "ben")
        .unwrap();
    assert!(
        ben["daily_token_cap"].is_null(),
        "the limit was cleared, not left at 100"
    );

    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        &path,
        &admin,
        json!({ "daily_token_cap": 0 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Accounts without any usage are listed too, so a limit can be set before the first request.
    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(summary["users"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn an_admin_can_make_an_account_whose_password_is_chosen_on_the_first_sign_in() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;

    // Self-registration is closed by default, so this is the only way in for the next account.
    let (status, created) = post_json(
        &router,
        "/api/users",
        &admin,
        json!({ "username": "ben", "daily_token_cap": 200 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["role"], "user");

    // The first sign-in is where the password is chosen, because there was none to check.
    let member = sign_in(&router, "ben", "their-own-password").await;
    assert_eq!(
        get_json(&router, "/api/me", &member).await["username"],
        "ben"
    );
    // From then on the password is checked, not taken as whatever is typed.
    let (status, _) = post_json(
        &router,
        "/api/login",
        "",
        json!({ "username": "ben", "password": "something else" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    let ben = summary["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|user| user["username"] == "ben")
        .unwrap();
    assert_eq!(
        ben["daily_token_cap"], 200,
        "the limit set on the form is the account's limit"
    );
    assert_eq!(
        ben["pending"], false,
        "an account the admin made is not waiting for approval"
    );

    // A member cannot make an account, a name that is taken is named rather than passed through
    // as a database error, and a limit below 1 is refused.
    let (status, body) =
        post_json(&router, "/api/users", &member, json!({ "username": "dee" })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        body["error"]["message"],
        "only an admin can make an account"
    );
    let (status, body) =
        post_json(&router, "/api/users", &admin, json!({ "username": "ben" })).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["error"]["message"],
        "the username ben is already taken"
    );
    let (status, _) = post_json(&router, "/api/users", &admin, json!({ "username": " " })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = post_json(
        &router,
        "/api/users",
        &admin,
        json!({ "username": "cy", "daily_token_cap": 0 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// Whoever signs in first on an account an admin made sets its password. Two first sign-ins at once used to both
// read that there was none, both write their own, and both be signed in; the write only lands now where there is
// still no password, and the sign-in that comes second is answered as the wrong password it has become.
#[tokio::test]
async fn first_sign_ins_at_once_on_an_admin_made_account_let_one_in() {
    let st = shared_state("first_sign_ins_at_once").await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    let (status, created) =
        post_json(&router, "/api/users", &admin, json!({ "username": "ben" })).await;
    assert_eq!(status, StatusCode::OK);
    let ben = created["id"].as_i64().unwrap();

    let passwords = ["first-password", "second-password"];
    let answers = join_all(passwords.iter().map(|password| {
        post_json(
            &router,
            "/api/login",
            "",
            json!({ "username": "ben", "password": password }),
        )
    }))
    .await;

    let winners: Vec<usize> = (0..answers.len())
        .filter(|&n| answers[n].0 == StatusCode::OK)
        .collect();
    assert_eq!(winners.len(), 1, "one sign-in sets the password");
    let (status, body) = &answers[1 - winners[0]];
    assert_eq!(*status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["type"], "invalid_credentials");
    assert_eq!(
        body["error"]["message"],
        "username or password is not correct"
    );
    let sessions = sqlx::query("SELECT COUNT(*) AS n FROM sessions WHERE user_id = ?")
        .bind(ben)
        .fetch_one(&st.pool)
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(sessions, 1);

    // The password that stays is the one whose sign-in was let in.
    let winner = passwords[winners[0]];
    let loser = passwords[1 - winners[0]];
    sign_in(&router, "ben", winner).await;
    let (status, _) = post_json(
        &router,
        "/api/login",
        "",
        json!({ "username": "ben", "password": loser }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

async fn usage_count(pool: &SqlitePool, user: i64) -> i64 {
    sqlx::query("SELECT COUNT(*) AS n FROM usage WHERE user_id = ?")
        .bind(user)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>("n")
}

#[tokio::test]
async fn a_disabled_account_cannot_sign_in_or_use_its_key() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let member_id = get_json(&router, "/api/me", &member).await["id"]
        .as_i64()
        .unwrap();

    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let (_, created) = post_json(
        &router,
        "/api/keys",
        &member,
        json!({ "name": "member key" }),
    )
    .await;
    let key = created["key"].as_str().unwrap().to_string();

    let (status, body) = send_json(
        &router,
        http::Method::PATCH,
        &format!("/api/users/{member_id}"),
        &admin,
        json!({ "enabled": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["enabled"]));

    // Disabling signs the account out, so the session it was already using stops working.
    let response = call(
        &router,
        Request::get("/api/me")
            .header("cookie", &member)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (status, _) = post_json(
        &router,
        "/api/login",
        "",
        json!({ "username": "ben", "password": "test-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The key is refused too, before any limit is looked at.
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json_body(response).await["error"]["type"],
        "account_disabled"
    );

    // The model list is the same gateway path with the same key, so it is refused the same way.
    let response = call(
        &router,
        Request::get("/v1/models")
            .header("authorization", format!("Bearer {key}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "a key alone does not reach an upstream for an account that is off"
    );
}

async fn models_call(router: &Router, key: &str) -> Response {
    call(
        router,
        Request::get("/v1/models")
            .header("authorization", format!("Bearer {key}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
}

// Dropping the usage table makes every limit read fail while the key and the account can still be
// read, which is the shape of a database that is held or failing in the middle of a request. Each
// key reaches a different read first: the per-minute count, the key's cap, and the account's day.
#[tokio::test]
async fn a_limit_that_cannot_be_read_refuses_the_request_rather_than_letting_it_through() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let mut keys = Vec::new();
    for body in [
        json!({ "name": "rated", "rpm_limit": 100 }),
        json!({ "name": "capped", "token_cap": 1000 }),
        json!({ "name": "plain" }),
    ] {
        let (_, created) = post_json(&router, "/api/keys", &admin, body).await;
        keys.push(created["key"].as_str().unwrap().to_string());
    }
    let me = get_json(&router, "/api/me", &admin).await;
    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        &format!("/api/users/{}", me["id"]),
        &admin,
        json!({ "daily_token_cap": 1000 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    sqlx::query("DROP TABLE usage")
        .execute(&st.pool)
        .await
        .unwrap();
    for key in &keys {
        let response = gateway_call(
            &router,
            key,
            json!({ "messages": [{ "role": "user", "content": "hi" }] }),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a limit that could not be read is not taken as one that was not reached"
        );
        let body = json_body(response).await;
        assert_eq!(body["error"]["type"], "unavailable");
        let message = body["error"]["message"].as_str().unwrap();
        assert!(message.contains("logged as reference"), "{message}");
        assert!(!message.contains("usage"), "no table name: {message}");
    }
}

// Renaming the account table makes the account read fail while the key read still works, so the
// disabled-account check is the one that cannot be made, at both gateway endpoints.
#[tokio::test]
async fn an_account_that_cannot_be_read_is_not_taken_as_one_that_is_enabled() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &admin, json!({ "name": "plain" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    sqlx::query("ALTER TABLE users RENAME TO users_unreadable")
        .execute(&st.pool)
        .await
        .unwrap();
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        models_call(&router, &key).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

// Deleting an account takes its keys with it, so a key whose account is already gone can only be
// seen between the two. Foreign keys are switched off on the test's one connection to hold that
// moment still.
#[tokio::test]
async fn a_key_whose_account_is_gone_is_answered_as_a_key_that_no_longer_exists() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &admin, json!({ "name": "plain" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&st.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM users")
        .execute(&st.pool)
        .await
        .unwrap();
    let response = gateway_call(
        &router,
        &key,
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        json_body(response).await["error"]["type"],
        "invalid_api_key"
    );
    assert_eq!(
        models_call(&router, &key).await.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        count(&st.pool, "usage").await,
        0,
        "nothing reached an upstream"
    );
}

#[tokio::test]
async fn an_admin_can_rename_an_account_and_set_a_new_password() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let member_id = get_json(&router, "/api/me", &member).await["id"]
        .as_i64()
        .unwrap();
    let path = format!("/api/users/{member_id}");

    let (status, body) = send_json(
        &router,
        http::Method::PATCH,
        &path,
        &admin,
        json!({ "username": "bob" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["username"]));

    // A username that is taken is refused rather than silently overwritten, and an empty one
    // is refused before anything is written.
    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        &path,
        &admin,
        json!({ "username": "ada" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = send_json(
        &router,
        http::Method::PATCH,
        &path,
        &admin,
        json!({ "username": " " }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Only the field in the body is applied, so setting a password leaves the limit alone.
    send_json(
        &router,
        http::Method::PATCH,
        &path,
        &admin,
        json!({ "daily_token_cap": 100 }),
    )
    .await;
    let (status, body) = send_json(
        &router,
        http::Method::PATCH,
        &path,
        &admin,
        json!({ "password": "another-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["password"]));

    let (status, _) = post_json(
        &router,
        "/api/login",
        "",
        json!({ "username": "bob", "password": "another-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    let bob = summary["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|user| user["username"] == "bob")
        .unwrap();
    assert_eq!(
        bob["daily_token_cap"], 100,
        "the password change did not clear the limit"
    );
}

#[tokio::test]
async fn deleting_an_account_takes_its_keys_and_its_usage_with_it() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    let admin_id = get_json(&router, "/api/me", &admin).await["id"]
        .as_i64()
        .unwrap();
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let member_id = get_json(&router, "/api/me", &member).await["id"]
        .as_i64()
        .unwrap();
    let path = format!("/api/users/{member_id}");

    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let (_, created) = post_json(
        &router,
        "/api/keys",
        &member,
        json!({ "name": "member key" }),
    )
    .await;
    gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(usage_count(&st.pool, member_id).await, 1);

    // A member is not an admin, and an admin cannot remove the account they are signed in with.
    let (status, _) = send_json(&router, http::Method::DELETE, &path, &member, json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send_json(
        &router,
        http::Method::DELETE,
        &format!("/api/users/{admin_id}"),
        &admin,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = send_json(&router, http::Method::DELETE, &path, &admin, json!({})).await;
    assert_eq!(status, StatusCode::OK);

    // Keys, usage and sessions reference the account with ON DELETE CASCADE.
    assert_eq!(usage_count(&st.pool, member_id).await, 0);
    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(summary["users"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn an_existing_database_with_the_old_columns_is_moved_to_the_current_shape() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();

    // A database written before accounts were identified by a username, and before the account
    // limit counted a day.
    sqlx::query(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            token_cap INTEGER,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    // The rest of the schema as it was written then, so the rows that point at an account exist.
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO users (name, email, password_hash, role, token_cap, created_at) VALUES ('ada', 'ada@example.com', 'hash', 'admin', 500, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO api_keys (user_id, name, key, created_at) VALUES (1, 'key', 'dreamgate_test', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO usage (api_key_id, user_id, model, prompt_tokens, completion_tokens, total_tokens, latency_ms, status, created_at) VALUES (1, 1, 'test-model', 11, 7, 18, 5, 200, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions (token, user_id, created_at) VALUES ('session', 1, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    db::migrate(&pool).await.unwrap();

    assert_eq!(
        user_columns(&pool).await.join(","),
        "created_at,daily_token_cap,enabled,id,password_hash,pending,role,username"
    );

    // The account keeps its name and its limit; only the column names changed, and the email is
    // gone with no place left for it.
    let row = sqlx::query("SELECT username, role, daily_token_cap, enabled FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("username"), "ada");
    assert_eq!(
        row.get::<i64, _>("daily_token_cap"),
        500,
        "the stored limit still applies"
    );
    assert_eq!(
        row.get::<i64, _>("enabled"),
        1,
        "an existing account is enabled by default"
    );

    // Running it again on a database that is already current changes nothing.
    db::migrate(&pool).await.unwrap();
    assert_eq!(
        user_columns(&pool).await.join(","),
        "created_at,daily_token_cap,enabled,id,password_hash,pending,role,username"
    );

    // Rebuilding the table must not take the rows that belong to the account with it.
    assert_eq!(usage_count(&pool, 1).await, 1);
    assert_eq!(count(&pool, "api_keys").await, 1);
    assert_eq!(count(&pool, "sessions").await, 1);

    // Foreign keys are left on, so removing an account still takes its keys, usage and sessions.
    sqlx::query("DELETE FROM users WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(usage_count(&pool, 1).await, 0);
    assert_eq!(count(&pool, "api_keys").await, 0);
    assert_eq!(count(&pool, "sessions").await, 0);
}

async fn count(pool: &SqlitePool, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) AS n FROM {table}");
    sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>("n")
}

// The shape the existing database on disk is actually in: a display name, an email with a UNIQUE
// constraint, and a limit column that was already renamed. Only the username and enabled are
// missing, so the rebuild has to read from the names as they are rather than assume both old
// spellings.
#[tokio::test]
async fn an_account_table_with_a_unique_email_and_a_current_limit_is_rebuilt() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            created_at TEXT NOT NULL,
            daily_token_cap INTEGER
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO users (email, name, password_hash, role, created_at, daily_token_cap) VALUES ('ada@example.com', 'ada', 'hash', 'admin', '2026-01-01T00:00:00Z', 500)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions (token, user_id, created_at) VALUES ('session', 1, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    db::migrate(&pool).await.unwrap();

    assert_eq!(
        user_columns(&pool).await.join(","),
        "created_at,daily_token_cap,enabled,id,password_hash,pending,role,username"
    );
    let row = sqlx::query("SELECT username, daily_token_cap, enabled, pending FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("username"), "ada");
    assert_eq!(row.get::<i64, _>("daily_token_cap"), 500);
    assert_eq!(
        row.get::<i64, _>("pending"),
        0,
        "an account that was already there is not held for approval"
    );
    assert_eq!(
        count(&pool, "sessions").await,
        1,
        "the sign-in for this account is still there"
    );
}

async fn user_columns(pool: &SqlitePool) -> Vec<String> {
    sqlx::query("SELECT name FROM pragma_table_info('users') ORDER BY name")
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect()
}

async fn usage_columns(pool: &SqlitePool) -> Vec<String> {
    sqlx::query("SELECT name FROM pragma_table_info('usage') ORDER BY name")
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect()
}

#[tokio::test]
async fn the_usage_columns_are_added_to_a_database_written_before_them() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }

    // A usage table as it was before a prompt cache or timings were recorded: no columns for them,
    // and a row written with that shape.
    sqlx::query("DROP TABLE usage")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE usage (
            id INTEGER PRIMARY KEY,
            api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL,
            model TEXT,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            latency_ms INTEGER NOT NULL,
            status INTEGER NOT NULL,
            streamed INTEGER NOT NULL DEFAULT 0,
            estimated INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO users (id, username, password_hash, role, created_at) VALUES (1, 'ada', 'hash', 'admin', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO api_keys (id, user_id, name, key, created_at) VALUES (1, 1, 'key', 'dreamgate_test', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO usage (api_key_id, user_id, model, prompt_tokens, completion_tokens, total_tokens, latency_ms, status, created_at) VALUES (1, 1, 'test-model', 11, 7, 18, 5, 200, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    db::migrate(&pool).await.unwrap();

    assert_eq!(
        usage_columns(&pool).await.join(","),
        "api_key_id,cached_tokens,completion_tokens,connection_id,created_at,estimated,generation_ms,id,latency_ms,model,prompt_ms,prompt_tokens,status,streamed,total_tokens,user_id"
    );

    // The row that was already there keeps its numbers and reads as unknown for what no engine
    // reported for it.
    let row = sqlx::query(
        "SELECT model, prompt_tokens, completion_tokens, total_tokens, cached_tokens, prompt_ms, \
         generation_ms FROM usage",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("model"), "test-model");
    assert_eq!(row.get::<i64, _>("prompt_tokens"), 11);
    assert_eq!(row.get::<i64, _>("completion_tokens"), 7);
    assert_eq!(row.get::<i64, _>("total_tokens"), 18);
    assert_eq!(row.get::<Option<i64>, _>("cached_tokens"), None);
    assert_eq!(row.get::<Option<i64>, _>("prompt_ms"), None);
    assert_eq!(
        row.get::<Option<i64>, _>("generation_ms"),
        None,
        "a row written before the change reads as unknown"
    );

    // Running it again on a table that is already current changes nothing.
    db::migrate(&pool).await.unwrap();
    assert_eq!(usage_count(&pool, 1).await, 1);

    // A new row can be written with the fields filled.
    sqlx::query(
        "INSERT INTO usage (api_key_id, user_id, model, prompt_tokens, completion_tokens, total_tokens, \
         cached_tokens, prompt_ms, generation_ms, latency_ms, status, created_at) \
         VALUES (1, 1, 'test-model', 11, 7, 18, 4, 121, 200, 5, 200, '2026-01-01T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(usage_count(&pool, 1).await, 2);
}

#[tokio::test]
async fn a_database_that_takes_usage_with_a_deleted_key_is_moved_to_the_current_shape() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }

    // A usage table as it was written before a recorded request outlived its key: the column
    // could not be null and the row went with the key.
    sqlx::query("DROP TABLE usage")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE usage (
            id INTEGER PRIMARY KEY,
            api_key_id INTEGER NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL,
            model TEXT,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            latency_ms INTEGER NOT NULL,
            status INTEGER NOT NULL,
            streamed INTEGER NOT NULL DEFAULT 0,
            estimated INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO users (id, username, password_hash, role, created_at) VALUES (1, 'ada', 'hash', 'admin', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO api_keys (id, user_id, name, key, created_at) VALUES (1, 1, 'key', 'dreamgate_test', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO usage (api_key_id, user_id, model, prompt_tokens, completion_tokens, total_tokens, latency_ms, status, created_at) VALUES (1, 1, 'test-model', 11, 7, 18, 5, 200, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    // A value reported for a row on a database that already has the columns has to be carried across the
    // rebuild, not dropped with the table it was written into.
    sqlx::query("ALTER TABLE usage ADD COLUMN cached_tokens INTEGER")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE usage SET cached_tokens = 4")
        .execute(&pool)
        .await
        .unwrap();

    db::migrate(&pool).await.unwrap();

    assert_eq!(
        sqlx::query("SELECT cached_tokens FROM usage")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<Option<i64>, _>("cached_tokens"),
        Some(4),
        "what an engine reported for a row outlives the rebuild"
    );

    // The rows that were already recorded stand, and the key reference now sets the column null.
    sqlx::query("DELETE FROM api_keys WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        usage_count(&pool, 1).await,
        1,
        "the recorded usage outlives the key"
    );

    // Running it again on a table that is already current changes nothing.
    db::migrate(&pool).await.unwrap();
    assert_eq!(usage_count(&pool, 1).await, 1);

    // An account still takes the rows that belong to it.
    sqlx::query("DELETE FROM users WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(usage_count(&pool, 1).await, 0);
}

async fn usage_indexes(pool: &SqlitePool) -> Vec<String> {
    sqlx::query(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'usage' ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>("name"))
    .collect()
}

async fn table_exists(pool: &SqlitePool, table: &str) -> bool {
    sqlx::query("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = ?")
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>("n")
        > 0
}

async fn key_action(pool: &SqlitePool) -> String {
    sqlx::query(
        "SELECT \"on_delete\" AS action FROM pragma_foreign_key_list('usage') WHERE \"table\" = 'api_keys'",
    )
    .fetch_one(pool)
    .await
    .unwrap()
    .get::<String, _>("action")
}

// Rebuilding the usage table drops its indexes with it, and the start that rebuilt it used to make
// only the one on the key again, so every per-account query read the whole table until the next start.
#[tokio::test]
async fn the_usage_indexes_are_in_place_after_the_table_is_rebuilt() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }

    // A usage table that still takes its rows with the key, with the indexes a database written then
    // has on it.
    sqlx::query("DROP TABLE usage")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE usage (
            id INTEGER PRIMARY KEY,
            api_key_id INTEGER NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL,
            model TEXT,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            latency_ms INTEGER NOT NULL,
            status INTEGER NOT NULL,
            streamed INTEGER NOT NULL DEFAULT 0,
            estimated INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    for statement in [
        "CREATE INDEX idx_usage_created ON usage (created_at)",
        "CREATE INDEX idx_usage_key ON usage (api_key_id)",
        "CREATE INDEX idx_usage_user ON usage (user_id)",
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO users (id, username, password_hash, role, created_at) VALUES (1, 'ada', 'hash', 'admin', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO api_keys (id, user_id, name, key, created_at) VALUES (1, 1, 'key', 'dreamgate_test', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO usage (api_key_id, user_id, model, prompt_tokens, completion_tokens, total_tokens, latency_ms, status, created_at) VALUES (1, 1, 'test-model', 11, 7, 18, 5, 200, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    db::migrate(&pool).await.unwrap();

    assert_eq!(key_action(&pool).await, "SET NULL", "the table was rebuilt");
    assert_eq!(
        usage_indexes(&pool).await.join(","),
        "idx_usage_created,idx_usage_key,idx_usage_user"
    );
    // A per-account query reads through its index rather than the whole table.
    let plan = sqlx::query("EXPLAIN QUERY PLAN SELECT COUNT(*) FROM usage WHERE user_id = 1")
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join(" ");
    assert!(plan.contains("idx_usage_user"), "{plan}");
    assert_eq!(usage_count(&pool, 1).await, 1);

    // Running it again on a table that is already current leaves them as they are.
    db::migrate(&pool).await.unwrap();
    assert_eq!(
        usage_indexes(&pool).await.join(","),
        "idx_usage_created,idx_usage_key,idx_usage_user"
    );
}

// A start that was interrupted while it rebuilt a table, before the rebuilds were one transaction,
// left the new table beside the old one: empty when the copy failed, full when it stopped after the
// copy. The old table still holds every row, so the rebuild runs again from it rather than failing on
// a table that already exists.
#[tokio::test]
async fn a_rebuild_left_unfinished_by_an_earlier_start_is_run_again() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();

    // Accounts with a display name, an email and a lifetime cap, and usage rows that go with their key.
    sqlx::query(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            token_cap INTEGER,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    sqlx::query("DROP TABLE usage")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE usage (
            id INTEGER PRIMARY KEY,
            api_key_id INTEGER NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL,
            model TEXT,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            latency_ms INTEGER NOT NULL,
            status INTEGER NOT NULL,
            streamed INTEGER NOT NULL DEFAULT 0,
            estimated INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO users (name, email, password_hash, role, token_cap, created_at) VALUES ('ada', 'ada@example.com', 'hash', 'admin', 500, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO api_keys (user_id, name, key, created_at) VALUES (1, 'key', 'dreamgate_test', '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO usage (api_key_id, user_id, model, prompt_tokens, completion_tokens, total_tokens, latency_ms, status, estimated, created_at) VALUES (1, 1, 'test-model', 11, 7, 18, 5, 200, 1, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions (token, user_id, created_at) VALUES ('session', 1, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    // What the interrupted start left: the copy of the accounts it finished, and the usage table it
    // made before the copy into it failed.
    sqlx::query(
        "CREATE TABLE users_new (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            role TEXT NOT NULL DEFAULT 'user',
            daily_token_cap INTEGER,
            enabled INTEGER NOT NULL DEFAULT 1,
            pending INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users_new (id, username, password_hash, role, daily_token_cap, enabled, pending, created_at) \
         SELECT id, name, password_hash, role, token_cap, 1, 0, created_at FROM users",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE usage_new (
            id INTEGER PRIMARY KEY,
            api_key_id INTEGER REFERENCES api_keys(id) ON DELETE SET NULL,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL,
            model TEXT,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            cached_tokens INTEGER,
            prompt_ms INTEGER,
            generation_ms INTEGER,
            latency_ms INTEGER NOT NULL,
            status INTEGER NOT NULL,
            streamed INTEGER NOT NULL DEFAULT 0,
            estimated INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    db::migrate(&pool).await.unwrap();

    assert!(!table_exists(&pool, "users_new").await);
    assert!(!table_exists(&pool, "usage_new").await);
    assert_eq!(
        user_columns(&pool).await.join(","),
        "created_at,daily_token_cap,enabled,id,password_hash,pending,role,username"
    );
    assert_eq!(key_action(&pool).await, "SET NULL");

    // The account and its row keep their values, once each.
    assert_eq!(count(&pool, "users").await, 1);
    let account =
        sqlx::query("SELECT id, username, role, daily_token_cap, enabled, pending FROM users")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(account.get::<i64, _>("id"), 1);
    assert_eq!(account.get::<String, _>("username"), "ada");
    assert_eq!(account.get::<String, _>("role"), "admin");
    assert_eq!(account.get::<i64, _>("daily_token_cap"), 500);
    assert_eq!(account.get::<i64, _>("enabled"), 1);
    assert_eq!(account.get::<i64, _>("pending"), 0);
    assert_eq!(count(&pool, "usage").await, 1);
    let usage = sqlx::query(
        "SELECT api_key_id, user_id, model, prompt_tokens, completion_tokens, total_tokens, \
         latency_ms, status, estimated, created_at FROM usage",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(usage.get::<Option<i64>, _>("api_key_id"), Some(1));
    assert_eq!(usage.get::<i64, _>("user_id"), 1);
    assert_eq!(usage.get::<String, _>("model"), "test-model");
    assert_eq!(usage.get::<i64, _>("prompt_tokens"), 11);
    assert_eq!(usage.get::<i64, _>("completion_tokens"), 7);
    assert_eq!(usage.get::<i64, _>("total_tokens"), 18);
    assert_eq!(usage.get::<i64, _>("latency_ms"), 5);
    assert_eq!(usage.get::<i64, _>("status"), 200);
    assert_eq!(
        usage.get::<i64, _>("estimated"),
        1,
        "an estimate is still marked as one"
    );
    assert_eq!(usage.get::<String, _>("created_at"), "2026-01-01T00:00:00Z");
    assert_eq!(count(&pool, "api_keys").await, 1);
    assert_eq!(count(&pool, "sessions").await, 1);
    assert_eq!(
        usage_indexes(&pool).await.join(","),
        "idx_usage_created,idx_usage_key,idx_usage_user"
    );
}

// Two accounts the old table let share a display name cannot both become a username, so the copy into
// the rebuilt table fails. What is tested is what that failure leaves: the table as it was, nothing
// beside it, and a connection with foreign keys on, so the next start meets the same database and fails
// for the same reason rather than on a table an earlier start left behind.
#[tokio::test]
async fn a_rebuild_that_fails_partway_leaves_the_table_as_it_was() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            created_at TEXT NOT NULL,
            daily_token_cap INTEGER
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO users (email, name, password_hash, role, created_at, daily_token_cap) VALUES ('ada@example.com', 'ada', 'hash', 'admin', '2026-01-01T00:00:00Z', 500)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO users (email, name, password_hash, role, created_at) VALUES ('ada@example.org', 'ada', 'hash', 'user', '2026-01-02T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO sessions (token, user_id, created_at) VALUES ('session', 1, '2026-01-01T00:00:00Z')")
        .execute(&pool)
        .await
        .unwrap();

    for _ in 0..2 {
        let err = db::migrate(&pool).await.unwrap_err();
        assert!(err.to_string().contains("UNIQUE"), "{err}");

        assert!(!table_exists(&pool, "users_new").await);
        assert_eq!(
            user_columns(&pool).await.join(","),
            "created_at,daily_token_cap,email,id,name,password_hash,role"
        );
        assert_eq!(count(&pool, "users").await, 2);
        assert_eq!(
            sqlx::query("SELECT daily_token_cap FROM users WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap()
                .get::<i64, _>("daily_token_cap"),
            500
        );
        assert_eq!(count(&pool, "sessions").await, 1);
        assert_eq!(
            sqlx::query("PRAGMA foreign_keys")
                .fetch_one(&pool)
                .await
                .unwrap()
                .get::<i64, _>(0),
            1,
            "the connection went back to the pool with foreign keys on"
        );
    }
}

#[tokio::test]
async fn the_summary_separates_the_callers_own_figures_from_the_platform_total() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;

    let admin = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let (_, admin_key) =
        post_json(&router, "/api/keys", &admin, json!({ "name": "admin key" })).await;
    gateway_call(
        &router,
        admin_key["key"].as_str().unwrap(),
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;

    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let (_, member_key) = post_json(
        &router,
        "/api/keys",
        &member,
        json!({ "name": "member key" }),
    )
    .await;
    gateway_call(
        &router,
        member_key["key"].as_str().unwrap(),
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;

    // The summary reports the caller's own figures and the platform total separately, so the
    // page does not have to infer which one it is showing. The generated side is reported for
    // both scopes, because the overview shows it for both.
    let admin_summary = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(
        admin_summary["self"]["requests"], 1,
        "an admin's own figures are their own"
    );
    assert_eq!(admin_summary["self"]["total_tokens"], 18);
    assert_eq!(
        admin_summary["self"]["completion_tokens"], 7,
        "the own figures carry the generated side, not only the total"
    );
    assert_eq!(admin_summary["global"]["requests"], 2);
    assert_eq!(admin_summary["global"]["total_tokens"], 36);
    assert_eq!(
        admin_summary["hourly"].as_array().unwrap().len(),
        1,
        "the charts are per account"
    );
    assert_eq!(admin_summary["models"].as_array().unwrap().len(), 1);
    assert_eq!(admin_summary["users"].as_array().unwrap().len(), 2);

    let member_summary = get_json(&router, "/api/stats/summary", &member).await;
    assert_eq!(member_summary["self"]["requests"], 1, "{member_summary}");
    assert_eq!(member_summary["self"]["total_tokens"], 18);
    assert_eq!(
        member_summary["self"]["completion_tokens"], 7,
        "a member sees the generated side of their own usage"
    );
    assert_eq!(
        member_summary["global"]["requests"], 2,
        "the platform total is not scoped"
    );
    assert_eq!(
        member_summary["users"].as_array().unwrap().len(),
        0,
        "a member does not see other accounts"
    );

    let recent = get_json(&router, "/api/stats/recent", &member).await;
    assert_eq!(recent["events"].as_array().unwrap().len(), 1);
    assert_eq!(recent["events"][0]["user"], "ben");
}

// The accounts in the summary are the only list an admin has to act on an account, so every account is in
// it, however many there are. The last one by name registers itself and waits for approval, which is the
// account an admin most needs to find.
#[tokio::test]
async fn the_summary_lists_every_account_for_an_admin() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    for n in 0..24 {
        let (status, _) = post_json(
            &router,
            "/api/users",
            &admin,
            json!({ "username": format!("member-{n:02}") }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    open_registration(&router, &admin).await;
    let (status, body) = post_json(
        &router,
        "/api/register",
        "",
        json!({ "username": "zoe", "password": "test-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pending"], true);

    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    let names: Vec<&str> = summary["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|user| user["username"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 26, "every account is listed: {names:?}");
    assert_eq!(names.len() as i64, count(&st.pool, "users").await);
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "the accounts are listed by username");
    assert!(
        names.contains(&"member-23"),
        "an account that sorts after the twentieth is listed: {names:?}"
    );
    let zoe = summary["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|user| user["username"] == "zoe")
        .expect("the last account by name is listed");
    assert_eq!(
        zoe["pending"], true,
        "the account waiting for approval can be found to approve"
    );
}

#[tokio::test]
async fn presence_counts_an_account_while_its_request_is_running() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    let admin_id = get_json(&router, "/api/me", &admin).await["id"]
        .as_i64()
        .unwrap();

    // The gateway holds the same guard for the length of a request, so holding it here is what
    // a running request looks like: generating and online at the same time.
    let active = presence::track(&st, admin_id);
    let during = get_json(&router, "/api/presence", &admin).await;
    assert_eq!(during["online"], 1);
    assert_eq!(during["generating"], 1);
    assert_eq!(during["users"][0]["username"], "ada");

    drop(active);
    let after = get_json(&router, "/api/presence", &admin).await;
    assert_eq!(
        after["generating"], 0,
        "the guard is released when the request ends"
    );
    assert_eq!(after["online"], 1, "still inside the five minute window");

    // Names are scoped like the rest of the stats: a member sees the counts, not the accounts.
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let seen = get_json(&router, "/api/presence", &member).await;
    assert_eq!(seen["users"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn endpoint_urls_follow_the_openai_convention() {
    assert_eq!(
        dreamgate::registry::endpoint("http://localhost:11434", "models"),
        "http://localhost:11434/v1/models"
    );
    assert_eq!(
        dreamgate::registry::endpoint("https://api.example.com/v1/", "models"),
        "https://api.example.com/v1/models"
    );
}

#[tokio::test]
async fn only_an_admin_can_change_a_connection_and_read_the_key_it_carries() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;

    let (status, created) = post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "test", "base_url": "http://127.0.0.1:1", "api_key": "upstream-secret" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let id = created["id"].as_i64().unwrap();

    // The page shows a connection to every account, because the gate readout and the model list
    // need it. The key it carries is a secret, and its address names an internal host, so those
    // parts go to an admin alone.
    let member_view = get_json(&router, "/api/connections", &member).await;
    assert_eq!(
        member_view["connections"][0]["name"], "test",
        "the connection itself is still readable"
    );
    assert!(member_view["connections"][0]["api_key"].is_null());
    assert!(member_view["connections"][0]["base_url"].is_null());
    let admin_view = get_json(&router, "/api/connections", &admin).await;
    assert_eq!(admin_view["connections"][0]["api_key"], "upstream-secret");
    assert_eq!(
        admin_view["connections"][0]["base_url"],
        "http://127.0.0.1:1"
    );

    // Adding, removing and checking a connection is admin-only in the page, so the rule has to be
    // on the routes as well rather than only on the view.
    let (status, _) = post_json(
        &router,
        "/api/connections",
        &member,
        json!({ "name": "another", "base_url": "http://127.0.0.1:2" }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = post_json(
        &router,
        &format!("/api/connections/{id}/check"),
        &member,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send_json(
        &router,
        http::Method::DELETE,
        &format!("/api/connections/{id}"),
        &member,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // For an admin the same routes work as they always did.
    let (status, _) = post_json(
        &router,
        &format!("/api/connections/{id}/check"),
        &admin,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_json(
        &router,
        http::Method::DELETE,
        &format!("/api/connections/{id}"),
        &admin,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_sign_in_does_not_say_which_part_was_wrong() {
    let st = test_state().await;
    let router = build_router(st.clone());
    register(&router, "ada").await;

    let (_, unknown) = post_json(
        &router,
        "/api/login",
        "",
        json!({ "username": "ben", "password": "test-password" }),
    )
    .await;
    let (_, mismatch) = post_json(
        &router,
        "/api/login",
        "",
        json!({ "username": "ada", "password": "wrong-password" }),
    )
    .await;
    assert_eq!(
        unknown["error"]["message"],
        "username or password is not correct"
    );
    assert_eq!(
        mismatch["error"]["message"], unknown["error"]["message"],
        "an attempt cannot list the accounts"
    );
}

#[tokio::test]
async fn sign_in_attempts_for_one_account_are_bounded() {
    let st = test_state().await;
    let router = build_router(st.clone());
    register(&router, "ada").await;

    for _ in 0..10 {
        let (status, _) = post_json(
            &router,
            "/api/login",
            "",
            json!({ "username": "ada", "password": "wrong-password" }),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    // The eleventh attempt is refused even with the right password, so the loop is bounded rather
    // than paid for with a bcrypt hash per call.
    let (status, body) = post_json(
        &router,
        "/api/login",
        "",
        json!({ "username": "ada", "password": "test-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["type"], "too_many_attempts");

    // The window is per account, so another account is not limited by what was tried against the
    // first one.
    let (status, _) = post_json(
        &router,
        "/api/login",
        "",
        json!({ "username": "nobody", "password": "wrong-password" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_session_older_than_the_window_the_cookie_last_is_refused() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    let admin_id = get_json(&router, "/api/me", &admin).await["id"]
        .as_i64()
        .unwrap();

    sqlx::query("INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, datetime('now', '-31 days'))")
        .bind("kept-token")
        .bind(admin_id)
        .execute(&st.pool)
        .await
        .unwrap();

    let response = call(
        &router,
        Request::get("/api/me")
            .header("cookie", "dg_session=kept-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "a token that is kept and replayed does not work forever"
    );

    // The account itself is still signed in with its own session.
    let response = call(
        &router,
        Request::get("/api/me")
            .header("cookie", &admin)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn the_session_cookie_is_marked_secure_only_when_the_request_arrived_over_tls() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let body = json!({ "username": "ada", "password": "test-password" }).to_string();

    let plain = call(
        &router,
        Request::post("/api/register")
            .header("content-type", "application/json")
            .body(Body::from(body.clone()))
            .unwrap(),
    )
    .await;
    assert_eq!(plain.status(), StatusCode::OK);
    let plain_cookie = plain.headers().get("set-cookie").unwrap().to_str().unwrap();
    assert!(plain_cookie.contains("HttpOnly") && plain_cookie.contains("SameSite=Lax"));
    assert!(
        !plain_cookie.contains("Secure"),
        "plain HTTP cannot promise a secure cookie"
    );

    // Behind a TLS-terminating proxy the same response can promise it, and the proxy header is
    // what says so.
    let tls = call(
        &router,
        Request::post("/api/login")
            .header("content-type", "application/json")
            .header("x-forwarded-proto", "https")
            .body(Body::from(body))
            .unwrap(),
    )
    .await;
    assert_eq!(tls.status(), StatusCode::OK);
    let cookie = tls.headers().get("set-cookie").unwrap().to_str().unwrap();
    assert!(
        cookie.contains("Secure"),
        "the cookie is marked Secure when the request came over TLS"
    );
    assert!(
        cookie.contains("Max-Age=2592000"),
        "the cookie and the stored session last the same window"
    );
}

// An upstream that streams one word every `delay_ms`, `words` times, and reports usage in its last chunk as
// an OpenAI-shaped server does, so a stream is still running for long enough to be watched from the page.
// It holds the stream open for a second after the usage chunk, four progress ticks, as an upstream that
// sends its end separately can, so the counts it reported are seen in flight before the row.
async fn slow_upstream(words: usize, delay_ms: u64, input_first: bool) -> String {
    let chat = move || async move {
        let body = stream::unfold(0usize, move |sent| async move {
            if sent > words {
                tokio::time::sleep(Duration::from_millis(1000)).await;
                return None;
            }
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            // Some engines report the input in their first chunk and what they generated only in the last,
            // so the mock can send the input's count with the first word.
            let line = if sent == 0 && input_first {
                json!({ "choices": [{ "delta": { "content": "word0" } }], "usage": { "prompt_tokens": 11 } })
            } else if sent < words {
                json!({ "choices": [{ "delta": { "content": format!("word{sent}") } }] })
            } else {
                json!({ "choices": [], "usage": { "prompt_tokens": 11, "completion_tokens": 42 } })
            };
            Some((
                Ok::<_, std::io::Error>(format!("data: {line}\n\n")),
                sent + 1,
            ))
        });
        let mut out = Response::new(Body::from_stream(body));
        out.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        out
    };
    let app = Router::new().route("/v1/chat/completions", post(chat));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

// The live stream read the way the page reads it: one event at a time, each a `data:` line and a blank line.
// A read waits no longer than five seconds, so a stream that stops sending fails the test rather than
// hanging it.
struct Live {
    body: Body,
    buffer: String,
}

impl Live {
    async fn open(router: &Router, cookie: &str) -> Live {
        let response = call(
            router,
            Request::get("/api/live")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        Live {
            body: response.into_body(),
            buffer: String::new(),
        }
    }

    async fn next(&mut self) -> Value {
        loop {
            if let Some(end) = self.buffer.find("\n\n") {
                let event: String = self.buffer.drain(..end + 2).collect();
                let data = event.trim().strip_prefix("data: ").unwrap();
                return serde_json::from_str(data).unwrap();
            }
            let frame = tokio::time::timeout(Duration::from_secs(5), self.body.frame())
                .await
                .expect("an event within five seconds")
                .expect("the live stream is still open")
                .unwrap();
            if let Ok(data) = frame.into_data() {
                self.buffer.push_str(std::str::from_utf8(&data).unwrap());
            }
        }
    }
}

// A stream that is still running is on the live stream, and its count moves while it runs rather than
// arriving with the row. The count is the gateway's estimate until the upstream reports one and then what
// the upstream reported, and the row that settles the stream names it, so the page can put one in the
// other's place. The stream leaves the list in the same step that sends the row, so a page never reads it
// gone before the row has arrived. The stream is two seconds of words against a progress tick of a quarter
// of a second, so it is seen moving with room to spare.
#[tokio::test]
async fn a_stream_in_flight_is_on_the_live_stream_and_its_row_takes_its_place() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = slow_upstream(40, 50, false).await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "slow", "base_url": base_url }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "watched" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let mut live = Live::open(&router, &cookie).await;
    assert_eq!(live.next().await["type"], "snapshot");

    let answer = {
        let router = router.clone();
        tokio::spawn(async move {
            let response = gateway_call(
                &router,
                &key,
                json!({ "model": "slow-model", "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
            )
            .await;
            response.into_body().collect().await.unwrap().to_bytes()
        })
    };

    let mut estimates = Vec::new();
    let mut reported = false;
    let mut stream = None;
    let mut usage = None;
    let mut cleared = false;
    while usage.is_none() || !cleared {
        let event = live.next().await;
        match event["type"].as_str() {
            Some("progress") => match event["streams"].as_array().unwrap().first() {
                Some(progress) => {
                    assert!(
                        usage.is_none(),
                        "the stream's progress came after the row that settles it"
                    );
                    assert_eq!(progress["key"], "watched");
                    assert_eq!(progress["model"], "slow-model");
                    assert_eq!(
                        event["shares"],
                        json!([{
                            "stream": progress["stream"],
                            "completion_tokens": progress["completion_tokens"],
                            "estimated": progress["estimated"],
                        }])
                    );
                    stream = Some(progress["stream"].clone());
                    if progress["estimated"] == true {
                        assert!(
                            !reported,
                            "the count went back to an estimate after the upstream reported one"
                        );
                        assert_eq!(
                            progress["prompt_estimated"], true,
                            "the input is the gateway's estimate until the upstream reports it"
                        );
                        estimates.push(progress["completion_tokens"].as_i64().unwrap());
                    } else {
                        assert_eq!(progress["prompt_tokens"], 11);
                        assert_eq!(progress["completion_tokens"], 42);
                        assert_eq!(progress["prompt_estimated"], false);
                        reported = true;
                    }
                    cleared = false;
                }
                None if stream.is_some() => {
                    assert!(
                        usage.is_some(),
                        "the stream left the list before its row arrived, so the figure dipped"
                    );
                    assert_eq!(event["shares"], json!([]));
                    cleared = true;
                }
                None => {}
            },
            Some("usage") => usage = Some(event),
            _ => {}
        }
    }
    answer.await.unwrap();

    assert!(
        estimates.windows(2).all(|pair| pair[0] <= pair[1]),
        "the estimate only goes up while the stream runs: {estimates:?}"
    );
    let mut moving = estimates.clone();
    moving.retain(|count| *count > 0);
    moving.dedup();
    assert!(
        moving.len() >= 2,
        "the count was seen moving while the stream ran, not only at its end: {estimates:?}"
    );
    assert!(
        reported,
        "the counts the upstream reported were seen in flight before the row"
    );
    let usage = usage.unwrap();
    assert_eq!(
        usage["event"]["stream"],
        stream.unwrap(),
        "the row names the stream it settles"
    );
    assert_eq!(usage["event"]["completion_tokens"], 42);
}

// The feed shows the input and what was generated in columns of their own, so each says on its own whether
// it is the gateway's estimate. An upstream that reports the input in its first chunk has the input shown as
// reported while the generated count is still an estimate, rather than both waiting for the last chunk.
#[tokio::test]
async fn a_stream_that_reports_its_input_first_shows_it_as_reported_while_the_rest_is_estimated() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = slow_upstream(20, 50, true).await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "slow", "base_url": base_url }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "watched" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let mut live = Live::open(&router, &cookie).await;
    assert_eq!(live.next().await["type"], "snapshot");

    let answer = {
        let router = router.clone();
        tokio::spawn(async move {
            let response = gateway_call(
                &router,
                &key,
                json!({ "model": "slow-model", "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
            )
            .await;
            response.into_body().collect().await.unwrap().to_bytes()
        })
    };

    let mut input_first = false;
    loop {
        let event = live.next().await;
        if event["type"] == "usage" {
            break;
        }
        if event["type"] != "progress" {
            continue;
        }
        let Some(progress) = event["streams"].as_array().unwrap().first() else {
            continue;
        };
        if progress["prompt_estimated"] == false && progress["estimated"] == true {
            assert_eq!(
                progress["prompt_tokens"], 11,
                "the input shown as reported is the upstream's count"
            );
            input_first = true;
        }
        if progress["prompt_estimated"] == true {
            assert!(
                !input_first,
                "the input went back to an estimate after the upstream reported it"
            );
        }
    }
    answer.await.unwrap();
    assert!(
        input_first,
        "the input was seen as reported while the generated count was still an estimate"
    );
}

// A recorded row says whether its counts are the gateway's estimate, in the recent rows and in the event a
// page is sent for it, so the page can mark an estimate rather than show it as a number the upstream
// reported. The upstream reports usage for one model and nothing for the other, both as a single answer
// and as a stream, because the two are recorded on different paths.
#[tokio::test]
async fn a_recorded_row_says_whether_its_counts_were_estimated() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let mut live = Live::open(&router, &cookie).await;
    assert_eq!(live.next().await["type"], "snapshot");

    let requests = [
        ("reports-usage", false),
        ("silent", false),
        ("reports-usage", true),
        ("silent", true),
    ];
    // Each request is finished before the next is sent, and a stream's row is written before its body
    // ends, so the rows are recorded in this order.
    for (model, stream) in requests {
        let response = gateway_call(
            &router,
            &key,
            json!({ "model": model, "stream": stream, "messages": [{ "role": "user", "content": "hi" }] }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();
    }

    let mut events = Vec::new();
    while events.len() < requests.len() {
        let event = live.next().await;
        if event["type"] == "usage" {
            events.push(event["event"].clone());
        }
    }
    for ((model, stream), event) in requests.iter().zip(&events) {
        assert_eq!(event["model"], *model, "{event}");
        assert_eq!(event["streamed"], *stream, "{event}");
        assert_eq!(
            event["estimated"],
            *model == "silent",
            "the live event says whether the counts were estimated: {event}"
        );
    }

    let recent = get_json(&router, "/api/stats/recent", &cookie).await;
    let rows = recent["events"].as_array().unwrap();
    assert_eq!(rows.len(), requests.len());
    // The recent rows are newest first.
    for ((model, stream), row) in requests.iter().rev().zip(rows) {
        assert_eq!(row["model"], *model, "{row}");
        assert_eq!(row["streamed"], *stream, "{row}");
        assert_eq!(
            row["estimated"],
            *model == "silent",
            "the recent row says whether the counts were estimated: {row}"
        );
    }
}

// Another account's stream is part of the platform total, which anyone signed in sees, but the stream
// itself, its key and its model, is listed for an admin alone, as the rest of the usage is. A member is
// told the stream's share of what is being generated and, when it ends, the row's figures and nothing else,
// naming the stream so its share leaves the total as the figures arrive.
#[tokio::test]
async fn another_accounts_stream_is_listed_for_an_admin_and_only_counted_for_a_member() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = slow_upstream(40, 50, false).await;
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let streaming = register(&router, "ben").await;
    let watching = register(&router, "cy").await;
    let ben = get_json(&router, "/api/me", &streaming).await["id"].clone();
    post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "slow", "base_url": base_url }),
    )
    .await;
    let (_, created) =
        post_json(&router, "/api/keys", &streaming, json!({ "name": "ben's" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let mut admin_live = Live::open(&router, &admin).await;
    assert_eq!(admin_live.next().await["type"], "snapshot");
    let mut member_live = Live::open(&router, &watching).await;
    assert_eq!(member_live.next().await["type"], "snapshot");

    let answer = {
        let router = router.clone();
        tokio::spawn(async move {
            let response = gateway_call(
                &router,
                &key,
                json!({ "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
            )
            .await;
            response.into_body().collect().await.unwrap().to_bytes()
        })
    };

    loop {
        let event = admin_live.next().await;
        if let Some(progress) = event["streams"]
            .as_array()
            .and_then(|streams| streams.first())
        {
            assert_eq!(progress["key"], "ben's");
            assert_eq!(progress["user_id"], ben);
            break;
        }
    }

    let mut share = None;
    let total = loop {
        let event = member_live.next().await;
        match event["type"].as_str() {
            Some("progress") => {
                assert_eq!(
                    event["streams"],
                    json!([]),
                    "another account's stream is not listed for a member"
                );
                if let Some(first) = event["shares"].as_array().unwrap().first() {
                    share = Some(first["stream"].clone());
                }
            }
            Some("total") => break event,
            Some("usage") => panic!("another account's row is not sent to a member"),
            _ => {}
        }
    };
    answer.await.unwrap();

    let share = share.expect("the stream's share reached the member while it ran");
    assert_eq!(
        total["event"]["stream"], share,
        "the figures name the stream whose share they replace"
    );
    assert_eq!(total["event"]["completion_tokens"], 42);
    assert_eq!(total["event"]["total_tokens"], 53);
    for field in ["id", "key", "user", "user_id", "model"] {
        assert!(
            total["event"].get(field).is_none(),
            "a member is not sent the row's {field}"
        );
    }
}

async fn get_text(
    router: &Router,
    path: &str,
    authorization: Option<&str>,
) -> (StatusCode, String) {
    let mut request = Request::get(path);
    if let Some(value) = authorization {
        request = request.header("authorization", value);
    }
    let response = call(router, request.body(Body::empty()).unwrap()).await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

// The health check is for whatever decides whether to restart the process, so it reads the database rather
// than answering ok whatever happens, and a database it cannot reach is a 503.
#[tokio::test]
async fn the_health_check_reads_the_database() {
    let st = test_state().await;
    let router = build_router(st.clone());
    assert_eq!(
        get_text(&router, "/health", None).await,
        (StatusCode::OK, "ok".to_string())
    );
    st.pool.close().await;
    assert_eq!(
        get_text(&router, "/health", None).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "database unavailable".to_string()
        )
    );
}

// The gateway binds every interface by default, so the counts are not there at all until a token is set,
// and with one they are read with it alone.
#[tokio::test]
async fn metrics_are_not_there_without_a_token_and_need_it_when_there_is_one() {
    let router = build_router(test_state().await);
    assert_eq!(
        get_text(&router, "/metrics", None).await.0,
        StatusCode::NOT_FOUND
    );

    let mut st = test_state().await;
    st.metrics = Arc::new(metrics::Metrics::new(Some("scrape-secret".to_string())));
    let router = build_router(st);
    assert_eq!(
        get_text(&router, "/metrics", None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get_text(&router, "/metrics", Some("Bearer wrong")).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (status, body) = get_text(&router, "/metrics", Some("Bearer scrape-secret")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("# TYPE dreamgate_responses_total counter"),
        "{body}"
    );
}

// What a scrape reads after traffic: every answer by route, status and type, refusals included; the tokens
// with what was reported kept apart from what was estimated; a connection that failed before answering; and
// what is in flight now.
#[tokio::test]
async fn metrics_count_answers_refusals_tokens_and_failed_upstreams() {
    let mut st = test_state().await;
    st.metrics = Arc::new(metrics::Metrics::new(Some("scrape-secret".to_string())));
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage", "priority": 0 }),
    )
    .await;
    let (_, dead) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 1 }),
    )
    .await;
    let (_, free) = post_json(&router, "/api/keys", &cookie, json!({ "name": "free" })).await;
    // Bound to the connection that does not answer, so it fails there first and falls over to the other.
    let (_, bound) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "bound", "connection_id": dead["id"] }),
    )
    .await;
    let body = json!({ "messages": [{ "role": "user", "content": "hi" }] });
    for created in [&free, &bound] {
        let response = gateway_call(&router, created["key"].as_str().unwrap(), body.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(
        gateway_call(&router, "dreamgate_not-a-key", body)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(recorded_usage(&st.pool).await.len(), 2);
    // A stream's row is written by the stream task rather than by the handler, so it is counted as well.
    let response = gateway_call(
        &router,
        free["key"].as_str().unwrap(),
        json!({ "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await.unwrap();
    for _ in 0..50 {
        if recorded_usage(&st.pool).await.len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(recorded_usage(&st.pool).await.len(), 3);

    let (_, scrape) = get_text(&router, "/metrics", Some("Bearer scrape-secret")).await;
    for line in [
        "dreamgate_responses_total{route=\"/v1/chat/completions\",status=\"200\",type=\"\"} 3",
        "dreamgate_responses_total{route=\"/v1/chat/completions\",status=\"401\",type=\"invalid_api_key\"} 1",
        "dreamgate_upstream_failures_total{connection=\"dead\"} 1",
        "dreamgate_tokens_total{direction=\"input\",source=\"reported\"} 33",
        "dreamgate_tokens_total{direction=\"output\",source=\"reported\"} 21",
        "dreamgate_tokens_total{direction=\"input\",source=\"estimated\"} 0",
        "dreamgate_request_duration_seconds_count{streamed=\"false\"} 2",
        "dreamgate_request_duration_seconds_count{streamed=\"true\"} 1",
        "dreamgate_usage_write_failures_total 0",
        "dreamgate_in_flight 0",
        "dreamgate_in_flight_limit 1024",
        "dreamgate_connection_in_flight{connection=\"test\"} 0",
        "dreamgate_request_duration_seconds_bucket{streamed=\"false\",le=\"+Inf\"} 2",
        "dreamgate_request_duration_seconds_bucket{streamed=\"false\",le=\"300\"} 2",
    ] {
        assert!(
            scrape.lines().any(|l| l == line),
            "missing {line} in:\n{scrape}"
        );
    }
    // The buckets of a histogram are cumulative, so no bucket is ever above the one after it.
    let buckets: Vec<u64> = scrape
        .lines()
        .filter(|l| l.starts_with("dreamgate_request_duration_seconds_bucket{streamed=\"false\""))
        .map(|l| l.rsplit(' ').next().unwrap().parse().unwrap())
        .collect();
    assert_eq!(buckets.len(), 12);
    assert!(
        buckets.windows(2).all(|pair| pair[0] <= pair[1]),
        "{buckets:?}"
    );
}

// A token is read without the line break a secret file leaves behind it, and one that is blank is no token.
#[tokio::test]
async fn a_metrics_token_is_read_without_the_space_around_it_and_a_blank_one_is_none() {
    let mut st = test_state().await;
    st.metrics = Arc::new(metrics::Metrics::new(Some("  ".to_string())));
    assert_eq!(
        get_text(&build_router(st), "/metrics", Some("Bearer "))
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    let mut st = test_state().await;
    st.metrics = Arc::new(metrics::Metrics::new(Some("scrape-secret\n".to_string())));
    assert_eq!(
        get_text(&build_router(st), "/metrics", Some("Bearer scrape-secret"))
            .await
            .0,
        StatusCode::OK
    );
}

// A pool whose every connection is in use is a burst being served, so the health check says busy with a 200
// rather than a 503 that would get a working gateway restarted.
#[tokio::test]
async fn the_health_check_answers_busy_rather_than_down_when_the_pool_is_in_use() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let _held = st.pool.acquire().await.unwrap();
    assert_eq!(
        get_text(&router, "/health", None).await,
        (StatusCode::OK, "busy".to_string())
    );
}

// Busy is a burst being served only while a query could still get a connection by waiting. A pool that has
// given the checks none for longer than a query waits is answered as down, and the first check that gets a
// connection again starts over.
#[tokio::test]
async fn a_pool_that_stays_in_use_past_the_wait_for_a_connection_is_answered_as_down() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(600))
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    let mut st = test_state().await;
    st.pool = pool;
    let router = build_router(st.clone());

    let held = st.pool.acquire().await.unwrap();
    assert_eq!(
        get_text(&router, "/health", None).await,
        (StatusCode::OK, "busy".to_string())
    );
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        get_text(&router, "/health", None).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "database unavailable".to_string()
        )
    );
    drop(held);
    assert_eq!(
        get_text(&router, "/health", None).await,
        (StatusCode::OK, "ok".to_string())
    );
    let _held = st.pool.acquire().await.unwrap();
    assert_eq!(
        get_text(&router, "/health", None).await,
        (StatusCode::OK, "busy".to_string())
    );
}

// The gauges read what is happening now, so a stream that is still running is in flight for the gateway and
// for its connection, and a connection's last check is read for whether it is up.
#[tokio::test]
async fn a_scrape_reads_what_is_in_flight_and_which_connections_are_up() {
    let mut st = test_state().await;
    st.metrics = Arc::new(metrics::Metrics::new(Some("scrape-secret".to_string())));
    let router = build_router(st.clone());
    let slow = slow_upstream(20, 50, false).await;
    let cookie = register(&router, "ada").await;
    let (_, running) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "slow", "base_url": slow, "priority": 0 }),
    )
    .await;
    let (_, dead) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 1 }),
    )
    .await;
    mark_health(&st.pool, &running, "ok").await;
    mark_health(&st.pool, &dead, "down").await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;

    let stream = gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "model": "m", "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(stream.status(), StatusCode::OK);
    let (_, scrape) = get_text(&router, "/metrics", Some("Bearer scrape-secret")).await;
    for line in [
        "dreamgate_in_flight 1",
        "dreamgate_connection_in_flight{connection=\"slow\"} 1",
        "dreamgate_connection_up{connection=\"slow\"} 1",
        "dreamgate_connection_up{connection=\"dead\"} 0",
    ] {
        assert!(
            scrape.lines().any(|l| l == line),
            "missing {line} in:\n{scrape}"
        );
    }
    stream.into_body().collect().await.unwrap();
}

// A row that could not be written is counted, because the caps then do not see that request.
#[tokio::test]
async fn a_usage_row_that_could_not_be_written_is_counted() {
    let mut st = test_state().await;
    st.metrics = Arc::new(metrics::Metrics::new(Some("scrape-secret".to_string())));
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    sqlx::query("DROP TABLE usage")
        .execute(&st.pool)
        .await
        .unwrap();
    let response = gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let (_, scrape) = get_text(&router, "/metrics", Some("Bearer scrape-secret")).await;
    assert!(
        scrape
            .lines()
            .any(|l| l == "dreamgate_usage_write_failures_total 1"),
        "{scrape}"
    );

    // A stream's row is written by its own task, which counts a row it could not write as well.
    let response = gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await.unwrap();
    let mut scrape = String::new();
    for _ in 0..50 {
        scrape = get_text(&router, "/metrics", Some("Bearer scrape-secret"))
            .await
            .1;
        if scrape
            .lines()
            .any(|l| l == "dreamgate_usage_write_failures_total 2")
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the stream's lost row was not counted:\n{scrape}");
}

// A dashboard's live stream ends as soon as the process starts to stop, because an open stream would hold the
// stop for as long as the page stayed open. The page reconnects on its own.
#[tokio::test]
async fn a_dashboards_live_stream_ends_when_the_gateway_starts_to_stop() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    let response = call(
        &router,
        Request::get("/api/live")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    // The snapshot comes first, so the stream is open and waiting when the stop begins.
    let first = body.frame().await.unwrap().unwrap();
    assert!(first.data_ref().is_some());

    st.shutdown.advance(dreamgate::shutdown::Phase::Draining);
    let ended = tokio::time::timeout(Duration::from_secs(2), body.collect()).await;
    assert!(
        ended.is_ok(),
        "the live stream stayed open after the stop began"
    );
}

// nginx holds a response back until its buffer fills unless the response says not to, which would turn a
// stream into one late answer; both kinds of event stream say so.
#[tokio::test]
async fn event_streams_ask_a_proxy_not_to_buffer_them() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let stream = gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(stream.headers()["x-accel-buffering"], "no");
    stream.into_body().collect().await.unwrap();

    let live = call(
        &router,
        Request::get("/api/live")
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(live.headers()["x-accel-buffering"], "no");
}

async fn endpoint_call(router: &Router, path: &str, key: &str, body: Value) -> Response {
    call(
        router,
        Request::post(path)
            .header("authorization", format!("Bearer {key}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
}

async fn endpoint_setup() -> (State, Router, String) {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let key = created["key"].as_str().unwrap().to_string();
    (st, router, key)
}

// Embeddings are forwarded and recorded like any other request: the input the upstream reports, nothing
// generated, and the gateway's estimate of the input, marked as one, when the upstream reports nothing. An
// embedding does not stream, so nothing about streaming is added to its body even when a client asks.
#[tokio::test]
async fn embeddings_are_forwarded_and_recorded() {
    let (st, router, key) = endpoint_setup().await;
    let response = endpoint_call(
        &router,
        "/v1/embeddings",
        &key,
        json!({ "model": "reports-usage", "input": "hello there", "stream": true }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["data"][0]["embedding"], json!([0.1, 0.2]));
    assert_eq!(body["streaming_fields"], false);

    let response = endpoint_call(
        &router,
        "/v1/embeddings",
        &key,
        json!({ "model": "silent", "input": ["hello there", "general kenobi"] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage.len(), 2);
    assert_eq!(
        (
            &usage[0]["prompt_tokens"],
            &usage[0]["completion_tokens"],
            &usage[0]["estimated"]
        ),
        (&json!(5), &json!(0), &json!(false))
    );
    assert_eq!(usage[0]["streamed"], false);
    assert_eq!(
        (
            &usage[1]["prompt_tokens"],
            &usage[1]["completion_tokens"],
            &usage[1]["estimated"]
        ),
        (&json!(8), &json!(0), &json!(true))
    );
}

// The legacy completions endpoint is forwarded too, a single answer and a stream, and when the upstream
// reports nothing the estimate counts the prompt and the text it generated.
#[tokio::test]
async fn completions_are_forwarded_and_recorded_single_and_streamed() {
    let (st, router, key) = endpoint_setup().await;
    let response = endpoint_call(
        &router,
        "/v1/completions",
        &key,
        json!({ "model": "reports-usage", "prompt": "say hello" }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await["choices"][0]["text"],
        "hello there world"
    );

    let response = endpoint_call(
        &router,
        "/v1/completions",
        &key,
        json!({ "model": "silent", "prompt": "say hello", "stream": true }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(String::from_utf8_lossy(&bytes).contains("[DONE]"));

    let mut usage = recorded_usage(&st.pool).await;
    for _ in 0..50 {
        if usage.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        usage = recorded_usage(&st.pool).await;
    }
    assert_eq!(usage.len(), 2);
    assert_eq!(
        (
            &usage[0]["prompt_tokens"],
            &usage[0]["completion_tokens"],
            &usage[0]["estimated"]
        ),
        (&json!(11), &json!(7), &json!(false))
    );
    assert_eq!(usage[1]["streamed"], true);
    assert_eq!(usage[1]["estimated"], true);
    // "say hello" for the input and "hello there" " world" for the output, each word of five letters two tokens
    // by the same estimate a chat answer is given.
    assert_eq!(usage[1]["prompt_tokens"], 3);
    assert_eq!(usage[1]["completion_tokens"], 6);
}

// A path under /v1 the gateway does not serve, and a served path asked with a method it does not take, are
// answered as JSON in the shape an OpenAI client reads, not as the empty answer of the file server.
#[tokio::test]
async fn an_unknown_path_or_method_under_v1_is_answered_as_json() {
    let router = build_router(test_state().await);
    let response = call(
        &router,
        Request::post("/v1/responses")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = json_body(response).await;
    assert_eq!(body["error"]["type"], "not_found");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("/v1/responses is not served here")
    );

    let response = call(
        &router,
        Request::get("/v1/chat/completions")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        json_body(response).await["error"]["type"],
        "invalid_request"
    );
}

// An embedding is sent with the model the client names, because its vectors only mean something next to the
// ones the same model made: a key's own model and a connection's default are models that generate, and they
// fill in only when the client names none.
#[tokio::test]
async fn an_embedding_is_sent_with_the_model_the_client_names() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "test", "base_url": base_url, "default_model": "chat-default" }),
    )
    .await;
    let (_, pinned) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "pinned", "model": "chat-pinned" }),
    )
    .await;
    let (_, plain) = post_json(&router, "/api/keys", &cookie, json!({ "name": "plain" })).await;

    for key in [&pinned, &plain] {
        let response = endpoint_call(
            &router,
            "/v1/embeddings",
            key["key"].as_str().unwrap(),
            json!({ "model": "embedder", "input": "hello" }),
        )
        .await;
        assert_eq!(json_body(response).await["model_sent"], "embedder");
    }
    // A model left blank names none, as leaving it out does.
    for body in [
        json!({ "input": "hello" }),
        json!({ "model": " ", "input": "hello" }),
    ] {
        let response = endpoint_call(
            &router,
            "/v1/embeddings",
            pinned["key"].as_str().unwrap(),
            body,
        )
        .await;
        assert_eq!(json_body(response).await["model_sent"], "chat-pinned");
    }
    let response = endpoint_call(
        &router,
        "/v1/embeddings",
        plain["key"].as_str().unwrap(),
        json!({ "input": "hello" }),
    )
    .await;
    assert_eq!(json_body(response).await["model_sent"], "chat-default");
}

// A single completion the upstream reports nothing for is estimated from its prompt, the text after the cursor
// an editor sends with it, and the text it generated.
#[tokio::test]
async fn a_single_completion_is_estimated_from_its_prompt_suffix_and_text() {
    let (st, router, key) = endpoint_setup().await;
    let response = endpoint_call(
        &router,
        "/v1/completions",
        &key,
        json!({ "model": "silent", "prompt": "say hello", "suffix": "and then stop" }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["estimated"], true);
    // "say hello" is three and "and then stop" three, a word of four letters or fewer being one; "hello there
    // world" is two for each word.
    assert_eq!(usage[0]["prompt_tokens"], 6);
    assert_eq!(usage[0]["completion_tokens"], 6);
}

// `/v1` itself is under `/v1` as much as any path below it.
#[tokio::test]
async fn v1_itself_is_answered_as_json() {
    let router = build_router(test_state().await);
    for path in ["/v1", "/v1/"] {
        let response = call(&router, Request::get(path).body(Body::empty()).unwrap()).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_eq!(json_body(response).await["error"]["type"], "not_found");
    }
}

// An embedding that no connection answered records the model it was sent with, which is the one its client
// named, not the key's.
#[tokio::test]
async fn an_embedding_no_connection_answered_records_the_model_its_client_named() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1" }),
    )
    .await;
    let (_, pinned) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "pinned", "model": "chat-pinned" }),
    )
    .await;
    let response = endpoint_call(
        &router,
        "/v1/embeddings",
        pinned["key"].as_str().unwrap(),
        json!({ "model": "embedder", "input": "hello" }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(recorded_usage(&st.pool).await[0]["model"], "embedder");
}

// A server that takes an embedding's input as chat messages is estimated from them, and so is a body whose
// input is null.
#[tokio::test]
async fn an_embedding_sent_as_messages_is_estimated_from_them() {
    let (st, router, key) = endpoint_setup().await;
    for body in [
        json!({ "model": "silent", "messages": [{ "role": "user", "content": "hello there" }] }),
        json!({ "model": "silent", "input": null, "messages": [{ "role": "user", "content": "hello there" }] }),
    ] {
        let response = endpoint_call(&router, "/v1/embeddings", &key, body).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage.len(), 2);
    for row in usage {
        assert_eq!(row["estimated"], true);
        assert!(row["prompt_tokens"].as_i64().unwrap() > 0, "{row}");
    }
}

// A list as a check stores it, from the ids it names.
async fn set_models(pool: &SqlitePool, connection: &Value, models: Option<Value>) {
    let stored = models.map(|ids| {
        let listed: Vec<Value> = ids
            .as_array()
            .unwrap()
            .iter()
            .map(|id| json!({ "id": id }))
            .collect();
        Value::from(listed).to_string()
    });
    sqlx::query("UPDATE connections SET models = ? WHERE id = ?")
        .bind(stored)
        .bind(connection["id"].as_i64().unwrap())
        .execute(pool)
        .await
        .unwrap();
}

async fn connection_named(router: &Router, cookie: &str, name: &str) -> Value {
    let listed = get_json(router, "/api/connections", cookie).await;
    listed["connections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|connection| connection["name"] == name)
        .cloned()
        .unwrap()
}

// A model list in a shape other than OpenAI's, which the check cannot read.
async fn unlisted_upstream() -> String {
    let app = Router::new().route(
        "/v1/models",
        get(|| async { Json(json!({ "models": [{ "name": "elsewhere" }] })) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

// The health check keeps the list a connection answers with. A list in another shape is not read as a list of
// nothing but as no list, and a connection that does not answer keeps the list it gave last.
#[tokio::test]
async fn a_health_check_keeps_the_model_list_it_read() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let unlisted = unlisted_upstream().await;
    let cookie = register(&router, "ada").await;
    for (name, url) in [
        ("listed", base_url.as_str()),
        ("unlisted", unlisted.as_str()),
        ("dead", "http://127.0.0.1:1"),
    ] {
        post_json(
            &router,
            "/api/connections",
            &cookie,
            json!({ "name": name, "base_url": url }),
        )
        .await;
    }
    let dead = connection_named(&router, &cookie, "dead").await;
    set_models(&st.pool, &dead, Some(json!(["kept"]))).await;

    for name in ["listed", "unlisted", "dead"] {
        let id = connection_named(&router, &cookie, name).await["id"].clone();
        post_json(
            &router,
            &format!("/api/connections/{id}/check"),
            &cookie,
            json!({}),
        )
        .await;
    }
    let listed = connection_named(&router, &cookie, "listed").await;
    assert_eq!(listed["health"], "ok");
    assert_eq!(listed["models"], json!(["test-model"]));
    let unlisted = connection_named(&router, &cookie, "unlisted").await;
    assert_eq!(unlisted["health"], "ok");
    assert_eq!(unlisted["models"], Value::Null);
    let dead = connection_named(&router, &cookie, "dead").await;
    assert_eq!(dead["health"], "down");
    assert_eq!(dead["models"], json!(["kept"]));
}

async fn routed_to(router: &Router, pool: &SqlitePool, key: &str, model: &str) -> i64 {
    let before = sqlx::query("SELECT COUNT(*) AS n FROM usage")
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>("n");
    let response = gateway_call(
        router,
        key,
        json!({ "model": model, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let row = sqlx::query("SELECT connection_id FROM usage ORDER BY id LIMIT 1 OFFSET ?")
        .bind(before)
        .fetch_one(pool)
        .await
        .unwrap();
    row.get::<i64, _>("connection_id")
}

// A request goes first to a connection whose list names the model it would be sent, ahead of one earlier in
// priority whose list does not. A connection whose list was never read keeps its place, a model no list names
// keeps the order as it was, a key's own model is the one matched, and a model Ollama lists under its tag is
// found by its name alone.
#[tokio::test]
async fn a_request_goes_first_to_a_connection_that_lists_its_model() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let (_, first) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "first", "base_url": base_url, "priority": 0 }),
    )
    .await;
    let (_, second) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "second", "base_url": base_url, "priority": 1 }),
    )
    .await;
    let (first_id, second_id) = (
        first["id"].as_i64().unwrap(),
        second["id"].as_i64().unwrap(),
    );
    set_models(&st.pool, &first, Some(json!(["a-model", "other:7b"]))).await;
    set_models(&st.pool, &second, Some(json!(["b-model", "llama3:latest"]))).await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let key = created["key"].as_str().unwrap();

    assert_eq!(
        routed_to(&router, &st.pool, key, "b-model").await,
        second_id
    );
    assert_eq!(routed_to(&router, &st.pool, key, "a-model").await, first_id);
    assert_eq!(routed_to(&router, &st.pool, key, "llama3").await, second_id);
    assert_eq!(
        routed_to(&router, &st.pool, key, "nowhere").await,
        first_id,
        "a model no list names is sent in the order of priority"
    );

    let (_, pinned) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "pinned", "model": "a-model" }),
    )
    .await;
    assert_eq!(
        routed_to(
            &router,
            &st.pool,
            pinned["key"].as_str().unwrap(),
            "b-model"
        )
        .await,
        first_id,
        "the key's own model is the one sent, so it is the one matched"
    );

    set_models(&st.pool, &first, None).await;
    assert_eq!(
        routed_to(&router, &st.pool, key, "b-model").await,
        first_id,
        "a connection whose list was never read is not moved back"
    );
}

// The model list is the lists the connections gave, each model once and named after the first connection a
// request for it reaches, a key's own connection first. A connection whose list was never read is left out.
#[tokio::test]
async fn the_model_list_merges_the_lists_the_connections_gave() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let mut connections = Vec::new();
    for (name, priority) in [("first", 0), ("second", 1), ("unread", 2)] {
        let (_, created) = post_json(
            &router,
            "/api/connections",
            &cookie,
            json!({ "name": name, "base_url": base_url, "priority": priority }),
        )
        .await;
        connections.push(created);
    }
    set_models(&st.pool, &connections[0], Some(json!(["m1", "m2"]))).await;
    set_models(&st.pool, &connections[1], Some(json!(["m2", "m3"]))).await;

    let listed = |body: Value| -> Vec<(String, String)> {
        body["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    entry["id"].as_str().unwrap().to_string(),
                    entry["owned_by"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let response = models_call(&router, created["key"].as_str().unwrap()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["object"], "list");
    assert_eq!(
        listed(body),
        [("m1", "first"), ("m2", "first"), ("m3", "second")]
            .map(|(id, by)| (id.to_string(), by.to_string()))
    );

    let (_, bound) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "bound", "connection_id": connections[1]["id"] }),
    )
    .await;
    let response = models_call(&router, bound["key"].as_str().unwrap()).await;
    assert_eq!(
        listed(json_body(response).await),
        [("m2", "second"), ("m3", "second"), ("m1", "first")]
            .map(|(id, by)| (id.to_string(), by.to_string()))
    );
}

// Routing by the lists moves a connection back only when some connection is known to serve the model, never moves
// the key's own connection, and puts a connection marked down that lists the model ahead of one that is up and
// does not. Ollama's `:latest` is matched both ways, a connection's default is the model matched, and so is the
// model an embedding's client names on a key pinned to another.
#[tokio::test]
async fn routing_by_model_keeps_the_keys_connection_and_moves_nothing_on_no_knowledge() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let mut ids = Vec::new();
    let mut connections = Vec::new();
    for (name, priority) in [("first", 0), ("second", 1)] {
        let (_, created) = post_json(
            &router,
            "/api/connections",
            &cookie,
            json!({ "name": name, "base_url": base_url, "priority": priority }),
        )
        .await;
        ids.push(created["id"].as_i64().unwrap());
        connections.push(created);
    }
    let (_, plain) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let plain = plain["key"].as_str().unwrap().to_string();

    // Nothing lists the model, so nothing is moved: the first connection's own list being something else is not
    // a reason to send the request to one known no better.
    set_models(
        &st.pool,
        &connections[0],
        Some(json!(["/models/qwen.gguf"])),
    )
    .await;
    assert_eq!(
        routed_to(&router, &st.pool, &plain, "qwen2.5").await,
        ids[0]
    );

    // A key bound to a connection goes there first even when another lists the model and its own does not.
    set_models(
        &st.pool,
        &connections[1],
        Some(json!(["qwen2.5", "llama3"])),
    )
    .await;
    let (_, bound) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "bound", "connection_id": ids[0] }),
    )
    .await;
    assert_eq!(
        routed_to(&router, &st.pool, bound["key"].as_str().unwrap(), "qwen2.5").await,
        ids[0]
    );
    assert_eq!(
        routed_to(&router, &st.pool, &plain, "qwen2.5").await,
        ids[1]
    );
    assert_eq!(
        routed_to(&router, &st.pool, &plain, "llama3:latest").await,
        ids[1]
    );

    // Marked down and listing the model goes ahead of up and not listing it.
    mark_health(&st.pool, &connections[1], "down").await;
    assert_eq!(
        routed_to(&router, &st.pool, &plain, "qwen2.5").await,
        ids[1]
    );
    mark_health(&st.pool, &connections[1], "ok").await;

    // A connection's default is the model it is sent, so it is the one matched: the first is sent the file it
    // lists, so it keeps its place although it does not list the model the client asked for.
    sqlx::query("UPDATE connections SET default_model = '/models/qwen.gguf' WHERE id = ?")
        .bind(ids[0])
        .execute(&st.pool)
        .await
        .unwrap();
    assert_eq!(
        routed_to(&router, &st.pool, &plain, "qwen2.5").await,
        ids[0]
    );

    // And a default only keeps its own connection in place: the second listing the default it is sent says
    // nothing about a model no list names, so the first is not moved behind it.
    sqlx::query("UPDATE connections SET default_model = NULL WHERE id = ?")
        .bind(ids[0])
        .execute(&st.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE connections SET default_model = 'llama3' WHERE id = ?")
        .bind(ids[1])
        .execute(&st.pool)
        .await
        .unwrap();
    assert_eq!(
        routed_to(&router, &st.pool, &plain, "nothing-lists-this").await,
        ids[0]
    );
}

// An embedding is matched by the model its client names, on a key pinned to another.
#[tokio::test]
async fn an_embedding_is_routed_by_the_model_its_client_names() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let (_, chat) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "chat", "base_url": base_url, "priority": 0 }),
    )
    .await;
    let (_, embedder) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "embedder", "base_url": base_url, "priority": 1 }),
    )
    .await;
    set_models(&st.pool, &chat, Some(json!(["chat-pinned"]))).await;
    set_models(&st.pool, &embedder, Some(json!(["embed-model"]))).await;
    let (_, pinned) = post_json(
        &router,
        "/api/keys",
        &cookie,
        json!({ "name": "pinned", "model": "chat-pinned" }),
    )
    .await;
    let response = endpoint_call(
        &router,
        "/v1/embeddings",
        pinned["key"].as_str().unwrap(),
        json!({ "model": "embed-model", "input": "hello" }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        recorded_usage(&st.pool).await[0]["connection_id"],
        embedder["id"]
    );
}

// A list whose entries all have ids is kept with the dates it gave, which the model list passes on, and an empty
// one too, because that is what the upstream said; one with an entry it cannot read an id from is no list at all,
// rather than the part it could read.
#[tokio::test]
async fn a_list_with_an_entry_without_an_id_is_not_read_in_part() {
    let app = Router::new()
        .route(
            "/c/v1/models",
            get(|| async { Json(json!({ "data": [{ "id": "dated", "created": 1700000000 }] })) }),
        )
        .route(
            "/a/v1/models",
            get(|| async { Json(json!({ "data": [{ "id": "ok", "created": 7 }, { "id": 5 }] })) }),
        )
        .route(
            "/b/v1/models",
            get(|| async { Json(json!({ "data": [] })) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    for name in ["a", "b", "c"] {
        let (_, created) = post_json(
            &router,
            "/api/connections",
            &cookie,
            json!({ "name": name, "base_url": format!("http://{addr}/{name}") }),
        )
        .await;
        post_json(
            &router,
            &format!("/api/connections/{}/check", created["id"]),
            &cookie,
            json!({}),
        )
        .await;
    }
    assert_eq!(
        connection_named(&router, &cookie, "a").await["models"],
        Value::Null
    );
    assert_eq!(
        connection_named(&router, &cookie, "b").await["models"],
        json!([])
    );
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let body = json_body(models_call(&router, created["key"].as_str().unwrap()).await).await;
    assert_eq!(
        body["data"],
        json!([{ "id": "dated", "object": "model", "owned_by": "c", "created": 1700000000 }])
    );
}

// A check whose answer breaks off before its body ends leaves the list the connection gave last, because what it
// serves has not changed for a body that could not be read.
#[tokio::test]
async fn a_list_that_breaks_off_keeps_the_last_one() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{\"data\":[",
                )
                .await;
        }
    });

    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    let (_, created) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "broken", "base_url": format!("http://{addr}") }),
    )
    .await;
    set_models(&st.pool, &created, Some(json!(["kept"]))).await;
    post_json(
        &router,
        &format!("/api/connections/{}/check", created["id"]),
        &cookie,
        json!({}),
    )
    .await;
    assert_eq!(
        connection_named(&router, &cookie, "broken").await["models"],
        json!(["kept"])
    );
}

// The merged list passes on the time a model was made when its connection's list gave one, includes the list of
// a connection marked down, and leaves out a connection whose list was never read.
#[tokio::test]
async fn the_model_list_passes_on_dates_and_includes_a_connection_marked_down() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let mut connections = Vec::new();
    for (name, priority) in [("dated", 0), ("down", 1), ("unread", 2)] {
        let (_, created) = post_json(
            &router,
            "/api/connections",
            &cookie,
            json!({ "name": name, "base_url": base_url, "priority": priority }),
        )
        .await;
        connections.push(created);
    }
    sqlx::query("UPDATE connections SET models = ? WHERE id = ?")
        .bind(r#"[{"id":"dated-model","created":1700000000}]"#)
        .bind(connections[0]["id"].as_i64().unwrap())
        .execute(&st.pool)
        .await
        .unwrap();
    set_models(&st.pool, &connections[1], Some(json!(["down-model"]))).await;
    mark_health(&st.pool, &connections[1], "down").await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let body = json_body(models_call(&router, created["key"].as_str().unwrap()).await).await;
    assert_eq!(
        body["data"],
        json!([
            { "id": "dated-model", "object": "model", "owned_by": "dated", "created": 1700000000 },
            { "id": "down-model", "object": "model", "owned_by": "down" },
        ])
    );
}

// A connection that fails a request is written down against it, and when the request goes on to another
// connection that failure is a failover; the one that answered counts the answer and how long it took. The
// figures are an admin's alone, and a connection's failures go with it when it is removed.
#[tokio::test]
async fn each_connection_counts_its_answers_its_failures_and_the_requests_it_passed_on() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let (_, dead) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 0 }),
    )
    .await;
    let (_, up) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "up", "base_url": base_url, "priority": 1 }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let response = gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let figures = get_json(&router, "/api/stats/connections", &cookie).await;
    let of = |connection: &Value| {
        figures["connections"]
            .as_array()
            .unwrap()
            .iter()
            .find(|figures| figures["id"] == connection["id"])
            .cloned()
            .unwrap()
    };
    let (dead_figures, up_figures) = (of(&dead), of(&up));
    assert_eq!(dead_figures["failures"], 1);
    assert_eq!(dead_figures["passed_on"], 1);
    assert_eq!(dead_figures["answered"], 0);
    assert_eq!(dead_figures["single"]["p50_ms"], Value::Null);
    assert_eq!(up_figures["answered"], 1);
    assert_eq!(up_figures["failures"], 0);
    assert_eq!(up_figures["single"]["count"], 1);
    assert!(up_figures["single"]["p50_ms"].is_i64());
    assert_eq!(up_figures["streamed"]["p50_ms"], Value::Null);

    open_registration(&router, &cookie).await;
    no_approval(&router, &cookie).await;
    let member = register(&router, "bob").await;
    let refused = call(
        &router,
        Request::get("/api/stats/connections")
            .header("cookie", &member)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);

    let removed = call(
        &router,
        Request::delete(format!("/api/connections/{}", dead["id"]))
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(removed.status(), StatusCode::OK);
    let left = sqlx::query("SELECT COUNT(*) AS n FROM connection_failures")
        .fetch_one(&st.pool)
        .await
        .unwrap()
        .get::<i64, _>("n");
    assert_eq!(left, 0, "a removed connection's failures were kept");
}

// A request that every connection fails was passed on from all but the last, which had nothing to pass it to.
#[tokio::test]
async fn the_last_connection_to_fail_a_request_did_not_pass_it_on() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    for (name, priority) in [("first", 0), ("second", 1)] {
        post_json(
            &router,
            "/api/connections",
            &cookie,
            json!({ "name": name, "base_url": "http://127.0.0.1:1", "priority": priority }),
        )
        .await;
    }
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let response = gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let figures = get_json(&router, "/api/stats/connections", &cookie).await;
    let counted: Vec<(i64, i64)> = figures["connections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["failures"].as_i64().unwrap(),
                f["passed_on"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(counted, [(1, 1), (1, 0)]);
}

async fn figures_of(router: &Router, cookie: &str, connection: &Value) -> Value {
    get_json(router, "/api/stats/connections", cookie).await["connections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|figures| figures["id"] == connection["id"])
        .cloned()
        .unwrap()
}

async fn failures_written(pool: &SqlitePool, at_least: i64) -> Vec<Value> {
    for _ in 0..200 {
        let rows = sqlx::query(
            "SELECT connection_id, kind, passed_on, usage_id FROM connection_failures ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        if rows.len() as i64 >= at_least {
            return rows
                .iter()
                .map(|row| {
                    json!({
                        "connection_id": row.get::<i64, _>("connection_id"),
                        "kind": row.get::<String, _>("kind"),
                        "passed_on": row.get::<i64, _>("passed_on") != 0,
                        "usage_id": row.get::<Option<i64>, _>("usage_id"),
                    })
                })
                .collect();
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("fewer than {at_least} failures were written");
}

// An upstream that answers every request with a head and then breaks off before the body it promised.
async fn breaking_upstream() -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{\"x\":")
                    .await;
            });
        }
    });
    format!("http://{addr}")
}

// An answer that was an error is counted among the connection's errors and left out of its times, and a row or
// a failure older than the day is left out of the figures altogether.
#[tokio::test]
async fn an_error_answer_is_counted_but_not_timed_and_old_rows_are_left_out() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": { "message": "no" } })),
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    let ada = get_json(&router, "/api/me", &cookie).await["id"]
        .as_i64()
        .unwrap();
    let (_, errs) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "errs", "base_url": format!("http://{addr}") }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let response = gateway_call(
        &router,
        created["key"].as_str().unwrap(),
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    sqlx::query(
        "INSERT INTO usage (user_id, connection_id, model, prompt_tokens, completion_tokens, total_tokens, \
         latency_ms, status, created_at) VALUES (?, ?, 'm', 1, 1, 2, 5, 200, '2020-01-01T00:00:00Z')",
    )
    .bind(ada)
    .bind(errs["id"].as_i64().unwrap())
    .execute(&st.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO connection_failures (connection_id, kind, created_at) VALUES (?, 'unreachable', '2020-01-01T00:00:00Z')",
    )
    .bind(errs["id"].as_i64().unwrap())
    .execute(&st.pool)
    .await
    .unwrap();

    let figures = figures_of(&router, &cookie, &errs).await;
    assert_eq!(figures["answered"], 1);
    assert_eq!(figures["errors"], 1);
    assert_eq!(figures["failures"], 0);
    assert_eq!(figures["single"]["count"], 0);
    assert_eq!(figures["single"]["p50_ms"], Value::Null);
}

// A single answer that breaks off is a failure, passed on to the next connection; a stream that breaks off after
// its head was sent has a row, but its failure names that row, so it is counted as a failure rather than as an
// answer, and its time is not an answer's.
#[tokio::test]
async fn a_broken_answer_is_passed_on_and_a_broken_stream_is_not_an_answer() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let (_, broken) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "broken", "base_url": breaking_upstream().await, "priority": 0 }),
    )
    .await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "up", "base_url": base_url, "priority": 1 }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = gateway_call(
        &router,
        &key,
        json!({ "model": "m", "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.into_body().collect().await;

    let failures = failures_written(&st.pool, 2).await;
    let stream_row = sqlx::query("SELECT id FROM usage WHERE streamed = 1")
        .fetch_one(&st.pool)
        .await
        .unwrap()
        .get::<i64, _>("id");
    assert_eq!(failures[0]["kind"], "broken");
    assert_eq!(failures[0]["passed_on"], true);
    assert_eq!(failures[0]["usage_id"], Value::Null);
    assert_eq!(failures[1]["kind"], "broken");
    assert_eq!(failures[1]["passed_on"], false);
    assert_eq!(failures[1]["usage_id"], stream_row);

    let figures = figures_of(&router, &cookie, &broken).await;
    assert_eq!(figures["failures"], 2);
    assert_eq!(figures["passed_on"], 1);
    assert_eq!(
        figures["answered"], 0,
        "a stream that broke off is not an answer"
    );
    assert_eq!(figures["streamed"]["count"], 0);
}

// A failure followed by a connection that was full is still passed on when the request reaches a third; a model
// list that fails has nothing to pass on to; and a failure that cannot be written does not touch the request.
#[tokio::test]
async fn a_failure_before_a_full_connection_is_passed_on_and_a_lost_write_changes_nothing() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    let (_, dead) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 0 }),
    )
    .await;
    let (_, full) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "full", "base_url": base_url, "priority": 1, "max_concurrent": 1 }),
    )
    .await;
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "up", "base_url": base_url, "priority": 2 }),
    )
    .await;
    let _held = st
        .limits
        .for_connection(full["id"].as_i64().unwrap(), Some(1))
        .try_acquire_owned()
        .unwrap();
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let key = created["key"].as_str().unwrap().to_string();
    let response = gateway_call(
        &router,
        &key,
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let failures = failures_written(&st.pool, 1).await;
    assert_eq!(failures[0]["connection_id"], dead["id"]);
    assert_eq!(failures[0]["passed_on"], true);

    // No list has been read, so the model list asks the first connection, which fails with nothing after it.
    let response = models_call(&router, &key).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let failures = failures_written(&st.pool, 2).await;
    assert_eq!(failures[1]["connection_id"], dead["id"]);
    assert_eq!(failures[1]["passed_on"], false);

    sqlx::query("DROP TABLE connection_failures")
        .execute(&st.pool)
        .await
        .unwrap();
    let response = gateway_call(
        &router,
        &key,
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a lost failure is not the request's"
    );
}

// A database written before the figures gains their table and its indexes on start.
#[tokio::test]
async fn the_failures_table_is_added_to_a_database_written_before_it() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for statement in db::SCHEMA
        .iter()
        .filter(|statement| !statement.contains("connection_failures"))
    {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    db::migrate(&pool).await.unwrap();
    let names: Vec<String> = sqlx::query(
        "SELECT name FROM sqlite_master WHERE tbl_name = 'connection_failures' ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|row| row.get("name"))
    .collect();
    assert_eq!(
        names,
        [
            "connection_failures",
            "idx_failures_connection",
            "idx_failures_created"
        ]
    );
}

// A request whose client leaves while a later connection is still working on it still has the failures it met
// written, from where they were dropped; and a failure of a connection removed while the request ran is left out
// without losing the others.
#[tokio::test]
async fn failures_are_written_when_the_client_leaves_and_a_removed_connection_loses_no_others() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            Json(json!({}))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;
    let mut dead = Vec::new();
    for (name, priority) in [("dead-1", 0), ("dead-2", 1)] {
        let (_, created) = post_json(
            &router,
            "/api/connections",
            &cookie,
            json!({ "name": name, "base_url": "http://127.0.0.1:1", "priority": priority }),
        )
        .await;
        dead.push(created);
    }
    post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "slow", "base_url": format!("http://{addr}"), "priority": 2 }),
    )
    .await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "k" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let mut request = Box::pin(gateway_call(
        &router,
        &key,
        json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }),
    ));
    // The client stays until both dead connections have failed and the slow one has the request, then the first
    // is removed and the client leaves.
    let _ = tokio::time::timeout(std::time::Duration::from_millis(500), &mut request).await;
    let removed = call(
        &router,
        Request::delete(format!("/api/connections/{}", dead[0]["id"]))
            .header("cookie", &cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(removed.status(), StatusCode::OK);
    drop(request);

    let failures = failures_written(&st.pool, 1).await;
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0]["connection_id"], dead[1]["id"]);
    assert_eq!(failures[0]["passed_on"], true);
}

async fn insert_usage(
    pool: &SqlitePool,
    user_id: i64,
    key_id: Option<i64>,
    model: &str,
    status: i64,
    created_at: &str,
) {
    sqlx::query(
        "INSERT INTO usage (api_key_id, user_id, model, prompt_tokens, completion_tokens, total_tokens, \
         latency_ms, status, created_at) VALUES (?, ?, ?, 3, 4, 7, 10, ?, ?)",
    )
    .bind(key_id)
    .bind(user_id)
    .bind(model)
    .bind(status)
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn get_status_json(router: &Router, path: &str, cookie: &str) -> (StatusCode, Value) {
    let response = call(
        router,
        Request::get(path)
            .header("cookie", cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let status = response.status();
    (status, json_body(response).await)
}

// The history is read a page at a time, newest first, each page naming the one after it, and narrowed by key,
// model, outcome and whole UTC days. A member reads its own account's rows only and is refused another's; a
// filter that cannot be read is refused rather than ignored.
#[tokio::test]
async fn the_activity_history_is_paged_filtered_and_scoped_to_the_account() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let ada = get_json(&router, "/api/me", &admin).await["id"]
        .as_i64()
        .unwrap();
    let ben = get_json(&router, "/api/me", &member).await["id"]
        .as_i64()
        .unwrap();
    let (_, key) = post_json(&router, "/api/keys", &admin, json!({ "name": "k" })).await;
    let key_id = key["id"].as_i64().unwrap();

    insert_usage(
        &st.pool,
        ada,
        Some(key_id),
        "m1",
        200,
        "2026-03-01T10:00:00Z",
    )
    .await;
    insert_usage(&st.pool, ada, None, "m2", 502, "2026-03-02T10:00:00Z").await;
    insert_usage(
        &st.pool,
        ada,
        Some(key_id),
        "m1",
        200,
        "2026-03-03T10:00:00Z",
    )
    .await;
    insert_usage(&st.pool, ben, None, "m1", 200, "2026-03-03T11:00:00Z").await;

    let ids = |body: &Value| -> Vec<String> {
        body["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["created_at"].as_str().unwrap()[..10].to_string())
            .collect()
    };
    let (_, first) = get_status_json(&router, "/api/stats/activity?limit=2", &admin).await;
    assert_eq!(ids(&first), ["2026-03-03", "2026-03-03"]);
    let next = first["next"].as_i64().unwrap();
    let (_, second) = get_status_json(
        &router,
        &format!("/api/stats/activity?limit=2&before={next}"),
        &admin,
    )
    .await;
    assert_eq!(ids(&second), ["2026-03-02", "2026-03-01"]);
    assert_eq!(second["next"], Value::Null);

    for (query, expected) in [
        (format!("key={key_id}"), vec!["2026-03-03", "2026-03-01"]),
        ("model=m2".to_string(), vec!["2026-03-02"]),
        ("outcome=error".to_string(), vec!["2026-03-02"]),
        (
            format!("user={ada}&outcome=ok"),
            vec!["2026-03-03", "2026-03-01"],
        ),
        (
            "since=2026-03-02&until=2026-03-02".to_string(),
            vec!["2026-03-02"],
        ),
    ] {
        let (status, body) =
            get_status_json(&router, &format!("/api/stats/activity?{query}"), &admin).await;
        assert_eq!(status, StatusCode::OK, "{query}");
        assert_eq!(ids(&body), expected, "{query}");
    }

    let (_, own) = get_status_json(&router, "/api/stats/activity", &member).await;
    assert_eq!(own["events"].as_array().unwrap().len(), 1);
    assert_eq!(own["events"][0]["user"], "ben");
    let (status, _) =
        get_status_json(&router, &format!("/api/stats/activity?user={ada}"), &member).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    for query in ["key=one", "outcome=maybe", "since=03/01/2026", "before=x"] {
        let (status, body) =
            get_status_json(&router, &format!("/api/stats/activity?{query}"), &admin).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(body["error"]["type"], "invalid_request");
    }
}

// The export is every row that matches, as CSV, with a figure nothing reported left empty, a cell quoted when it
// must be, and a name a spreadsheet would read as a formula written as text.
#[tokio::test]
async fn the_activity_export_is_every_matching_row_as_csv() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    let ada = get_json(&router, "/api/me", &admin).await["id"]
        .as_i64()
        .unwrap();
    for n in 0..1203 {
        insert_usage(
            &st.pool,
            ada,
            None,
            "m1",
            200,
            &format!("2026-03-01T10:{:02}:{:02}Z", n / 60 % 60, n % 60),
        )
        .await;
    }
    insert_usage(
        &st.pool,
        ada,
        None,
        "=cmd,\"x\"",
        500,
        "2026-03-02T10:00:00Z",
    )
    .await;

    let response = call(
        &router,
        Request::get("/api/stats/activity.csv")
            .header("cookie", &admin)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-disposition"]
            .to_str()
            .unwrap()
            .starts_with("attachment")
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    let lines: Vec<&str> = text.split("\r\n").filter(|line| !line.is_empty()).collect();
    assert_eq!(
        lines.len(),
        1 + 1204,
        "every row, across the pages it is read in"
    );
    assert!(lines[0].starts_with("id,created_at,account,key,connection,model,status"));
    assert!(
        lines[1].contains(",\"'=cmd,\"\"x\"\"\",500,"),
        "{}",
        lines[1]
    );
    // No key and no connection, and no cache figure reported: empty cells, not zeros.
    assert!(
        lines[2].contains(",ada,,,m1,200,false,false,3,4,7,,10,,"),
        "{}",
        lines[2]
    );

    // A page larger than the most is read as the most.
    let (_, page) = get_status_json(&router, "/api/stats/activity?limit=600", &admin).await;
    assert_eq!(page["events"].as_array().unwrap().len(), 500);

    let response = call(
        &router,
        Request::get("/api/stats/activity.csv?outcome=error")
            .header("cookie", &admin)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(String::from_utf8_lossy(&bytes).matches("\r\n").count(), 2);
}

async fn csv_lines(router: &Router, path: &str, cookie: &str) -> (StatusCode, Vec<String>) {
    let response = call(
        router,
        Request::get(path)
            .header("cookie", cookie)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).to_string();
    (
        status,
        text.split("\r\n")
            .filter(|line| !line.is_empty())
            .map(String::from)
            .collect(),
    )
}

// The history narrowed by connection, a key deleted through the API reading as none, a day's bound ending at the
// day after it, a limit that cannot be read refused, and a member's export and another account's key id giving
// the member nothing that is not its own.
#[tokio::test]
async fn the_activity_filters_read_a_connection_a_deleted_key_and_the_end_of_a_day() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    no_approval(&router, &admin).await;
    let member = register(&router, "ben").await;
    let ada = get_json(&router, "/api/me", &admin).await["id"]
        .as_i64()
        .unwrap();
    let ben = get_json(&router, "/api/me", &member).await["id"]
        .as_i64()
        .unwrap();
    let (_, connection) = post_json(
        &router,
        "/api/connections",
        &admin,
        json!({ "name": "c", "base_url": "http://127.0.0.1:1" }),
    )
    .await;
    let (_, gone) = post_json(&router, "/api/keys", &admin, json!({ "name": "gone" })).await;
    let gone_id = gone["id"].as_i64().unwrap();
    let (_, kept) = post_json(&router, "/api/keys", &admin, json!({ "name": "kept" })).await;
    let kept_id = kept["id"].as_i64().unwrap();

    insert_usage(
        &st.pool,
        ada,
        Some(gone_id),
        "m1",
        200,
        "2026-03-02T23:59:59Z",
    )
    .await;
    insert_usage(&st.pool, ada, None, "m2", 200, "2026-03-03T00:00:00Z").await;
    insert_usage(&st.pool, ben, None, "m3", 200, "2026-03-03T01:00:00Z").await;
    insert_usage(
        &st.pool,
        ada,
        Some(kept_id),
        "m0",
        200,
        "2026-02-28T00:00:00Z",
    )
    .await;
    sqlx::query("UPDATE usage SET connection_id = ? WHERE model = 'm2'")
        .bind(connection["id"].as_i64().unwrap())
        .execute(&st.pool)
        .await
        .unwrap();
    let (status, _) = send_json(
        &router,
        http::Method::DELETE,
        &format!("/api/keys/{gone_id}"),
        &admin,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let models = |body: &Value| -> Vec<String> {
        body["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["model"].as_str().unwrap().to_string())
            .collect()
    };
    let (_, body) = get_status_json(
        &router,
        &format!("/api/stats/activity?connection={}", connection["id"]),
        &admin,
    )
    .await;
    assert_eq!(models(&body), ["m2"]);
    assert_eq!(body["events"][0]["connection"], "c");
    let (_, body) = get_status_json(&router, "/api/stats/activity?until=2026-03-02", &admin).await;
    assert_eq!(
        models(&body),
        ["m0", "m1"],
        "the first second of the next day is not in it"
    );
    assert_eq!(
        body["events"][1]["key"],
        Value::Null,
        "the key was deleted, the row kept"
    );
    let (_, body) = get_status_json(&router, "/api/stats/activity?since=2026-03-03", &admin).await;
    assert_eq!(models(&body), ["m3", "m2"]);
    let (_, body) = get_status_json(&router, "/api/stats/activity?limit=&before=", &admin).await;
    assert_eq!(
        models(&body),
        ["m0", "m3", "m2", "m1"],
        "a value left empty is left out"
    );
    for query in ["limit=0", "limit=-3", "limit=abc"] {
        let (status, _) =
            get_status_json(&router, &format!("/api/stats/activity?{query}"), &admin).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
    }

    // A member's export is its own rows, and another account's key id narrows its own rows to none rather than
    // reading the other account's.
    let (status, lines) = csv_lines(&router, "/api/stats/activity.csv", &member).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(lines[1].contains(",ben,"), "{}", lines[1]);
    let (status, _) = csv_lines(
        &router,
        &format!("/api/stats/activity.csv?user={ada}"),
        &member,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (_, body) = get_status_json(
        &router,
        &format!("/api/stats/activity?key={kept_id}"),
        &admin,
    )
    .await;
    assert_eq!(models(&body), ["m0"], "the key has a row");
    let (_, body) = get_status_json(
        &router,
        &format!("/api/stats/activity?key={kept_id}"),
        &member,
    )
    .await;
    assert_eq!(body["events"], json!([]), "but not one the member can read");
}
