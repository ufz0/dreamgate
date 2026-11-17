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
use dreamgate::{build_router, db, limits, live, presence, state::State};
use futures_util::stream;
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

    // Far above what any test in this file sends, so a test is not shaped by admission: as a result the main
    // suite never exercises it, including its interaction with the failover test. The tests that check admission
    // build their own state in tests/concurrency.rs. The time bounds are the ones the gateway runs with,
    // because the mock answers long before either of them.
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

async fn mock_upstream() -> String {
    let app = Router::new()
        .route("/v1/models", get(mock_models))
        .route("/v1/chat/completions", post(mock_chat));
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
    let round = tokio::spawn(dreamgate::registry::health_checker(st.clone()));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let health: String = sqlx::query_scalar("SELECT health FROM connections WHERE id = ?")
            .bind(id)
            .fetch_one(&st.pool)
            .await
            .unwrap();
        if health == "ok" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the connection still reads {health} after the first round"
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
    // need it. The key it carries is a secret, so that part goes to an admin alone.
    let member_view = get_json(&router, "/api/connections", &member).await;
    assert_eq!(
        member_view["connections"][0]["name"], "test",
        "the connection itself is still readable"
    );
    assert!(member_view["connections"][0]["api_key"].is_null());
    let admin_view = get_json(&router, "/api/connections", &admin).await;
    assert_eq!(admin_view["connections"][0]["api_key"], "upstream-secret");

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
