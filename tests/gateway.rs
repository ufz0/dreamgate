use std::time::Duration;

use axum::{
    body::Body,
    extract::Json,
    http::{self, header, HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use dreamgate::{build_router, db, live, presence, state::State};
use http_body_util::BodyExt;
use serde_json::{json, Value};
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
    State {
        pool,
        http: reqwest::Client::new(),
        hub: live::hub(),
        presence: Default::default(),
    }
}

// An OpenAI-compatible upstream with three behaviours: it reports usage, reports none, or answers
// with a reasoning section beside the answer.
async fn mock_chat(Json(body): Json<Value>) -> Response {
    let model = body.get("model").and_then(Value::as_str).unwrap_or("");
    let reports_usage = model == "reports-usage";
    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
        let mut chunks = String::new();
        if model == "reasons" {
            chunks.push_str("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think hard\"}}]}\n\n");
        }
        chunks.push_str("data: {\"choices\":[{\"delta\":{\"content\":\"hello world\"}}]}\n\n");
        if reports_usage {
            chunks.push_str("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7}}\n\n");
        }
        chunks.push_str("data: [DONE]\n\n");
        let mut out = Response::new(Body::empty());
        out.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        *out.body_mut() = Body::from(chunks);
        return out;
    }
    let message = if model == "reasons" {
        json!({ "content": "hello world", "reasoning_content": "think hard" })
    } else {
        json!({ "content": "hello world" })
    };
    let payload = if reports_usage {
        json!({ "choices": [{ "message": message }], "usage": { "prompt_tokens": 11, "completion_tokens": 7 } })
    } else {
        json!({ "choices": [{ "message": message }] })
    };
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

async fn send_json(router: &Router, method: http::Method, path: &str, cookie: &str, body: Value) -> (StatusCode, Value) {
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
    let response = call(router, Request::get(path).header("cookie", cookie).body(Body::empty()).unwrap()).await;
    json_body(response).await
}

async fn register(router: &Router, username: &str) -> String {
    let response = call(
        router,
        Request::post("/api/register")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "username": username, "password": "test-password" }).to_string()))
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
    let (status, _) = send_json(router, http::Method::PATCH, "/api/settings", admin, json!({ "open_registration": true })).await;
    assert_eq!(status, StatusCode::OK);
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
            "SELECT connection_id, model, prompt_tokens, completion_tokens, total_tokens, status, estimated, streamed \
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
    let closed = post_json(&router, "/api/register", "", json!({ "username": "ben", "password": "test-password" })).await;
    assert_eq!(closed.0, StatusCode::FORBIDDEN);

    open_registration(&router, &admin).await;
    let member = register(&router, "ben").await;
    assert_eq!(get_json(&router, "/api/me", &member).await["role"], "user");
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
    assert_eq!(after["open_registration"], false, "closed by default after the first account");
    assert_eq!(after["has_accounts"], true);

    let (status, _) = send_json(&router, http::Method::PATCH, "/api/settings", &admin, json!({ "open_registration": true })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(get_json(&router, "/api/settings", &admin).await["open_registration"], true);

    let member = register(&router, "ben").await;
    let (status, _) = send_json(&router, http::Method::PATCH, "/api/settings", &member, json!({ "open_registration": false })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(get_json(&router, "/api/settings", &member).await["open_registration"], true, "the admin's setting stands");
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

    let (status, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    assert_eq!(status, StatusCode::OK);
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["prompt_tokens"], 11);
    assert_eq!(usage[0]["completion_tokens"], 7);
    assert_eq!(usage[0]["total_tokens"], 18);
    assert_eq!(usage[0]["estimated"], false);
    assert_eq!(usage[0]["connection_id"], connection["id"]);
    assert_eq!(usage[0]["model"], "reports-usage", "the key's connection default model is applied");
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
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["estimated"], true);
    assert!(usage[0]["prompt_tokens"].as_i64().unwrap() > 0);
    assert_eq!(usage[0]["completion_tokens"], 4, "two words in the answer, two subword pieces each");
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
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(
        &router,
        &key,
        json!({ "stream": true, "messages": [{ "role": "user", "content": "hi" }] }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-type").unwrap(), "text/event-stream");

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
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
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
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["estimated"], true);
    assert_eq!(usage[0]["completion_tokens"], 7, "the answer and the reasoning together");
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
    post_json(&router, "/api/connections", &cookie, json!({ "name": "test", "base_url": base_url })).await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
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
    post_json(&router, "/api/connections", &cookie, json!({ "name": "test", "base_url": base_url })).await;
    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
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
    post_json(&router, "/api/connections", &cookie, json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 0 })).await;
    let (_, healthy) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "live", "base_url": base_url, "default_model": "reports-usage", "priority": 1 }),
    )
    .await;

    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "test key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::OK);

    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["connection_id"], healthy["id"]);
    assert_eq!(usage[0]["completion_tokens"], 7);
}

#[tokio::test]
async fn per_key_rate_limit_blocks_the_third_request() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(&router, "/api/connections", &cookie, json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" })).await;

    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "limited", "rpm_limit": 2 })).await;
    let key = created["key"].as_str().unwrap().to_string();

    for _ in 0..2 {
        let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn key_limits_must_be_positive() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let cookie = register(&router, "ada").await;

    // A limit of 0 would refuse every request, so it is rejected rather than stored.
    let (status, _) = post_json(&router, "/api/keys", &cookie, json!({ "name": "zero", "token_cap": 0 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = post_json(&router, "/api/keys", &cookie, json!({ "name": "negative", "rpm_limit": -1 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = post_json(&router, "/api/keys", &cookie, json!({ "name": "unset limits" })).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn the_keys_list_is_the_callers_own_and_only_an_admin_can_read_the_others() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    let member = register(&router, "ben").await;

    post_json(&router, "/api/keys", &admin, json!({ "name": "admin key" })).await;
    post_json(&router, "/api/keys", &member, json!({ "name": "member key" })).await;

    // The Keys page shows one table, so the main list is always the caller's own and another
    // account's keys come from a read an admin can make on their own.
    let own = get_json(&router, "/api/keys", &admin).await;
    assert_eq!(own["keys"].as_array().unwrap().len(), 1, "an admin's own list is their own");
    assert_eq!(own["keys"][0]["name"], "admin key");

    let others = get_json(&router, "/api/keys/others", &admin).await;
    assert_eq!(others["keys"].as_array().unwrap().len(), 1);
    assert_eq!(others["keys"][0]["owner"], "ben", "the other list names who a key belongs to");

    let denied = get_json(&router, "/api/keys/others", &member).await;
    assert_eq!(denied["error"]["message"], "only an admin can see keys from other accounts");
}

#[tokio::test]
async fn per_key_token_cap_blocks_the_next_request() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "ada").await;
    post_json(&router, "/api/connections", &cookie, json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" })).await;

    let (_, created) = post_json(&router, "/api/keys", &cookie, json!({ "name": "capped", "token_cap": 20 })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::OK);
    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["total_tokens"], 18);

    // Lower the cap below what the first call already used, so the next call is refused.
    let (status, _) = send_json(&router, http::Method::PATCH, &format!("/api/keys/{}", created["id"]), &cookie, json!({ "token_cap": 10 })).await;
    assert_eq!(status, StatusCode::OK);
    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn account_daily_limit_blocks_the_next_request() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(&router, "/api/connections", &admin, json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" })).await;

    // The key has no cap of its own, so only the account limit can stop it.
    let (_, created) = post_json(&router, "/api/keys", &admin, json!({ "name": "uncapped" })).await;
    let key = created["key"].as_str().unwrap().to_string();
    gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    let usage = recorded_usage(&st.pool).await;
    assert_eq!(usage[0]["total_tokens"], 18);

    let me = get_json(&router, "/api/me", &admin).await;
    // The limit has to sit below what the first call already used, otherwise the account is still under it.
    let (status, body) = send_json(&router, http::Method::PATCH, &format!("/api/users/{}", me["id"]), &admin, json!({ "daily_token_cap": 15 })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["daily_token_cap"]));

    // 18 tokens are already counted for today, so the next request is refused.
    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn account_daily_limit_counts_only_the_current_day() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    post_json(&router, "/api/connections", &admin, json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" })).await;

    let (_, created) = post_json(&router, "/api/keys", &admin, json!({ "name": "uncapped" })).await;
    let key = created["key"].as_str().unwrap().to_string();
    gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;

    let me = get_json(&router, "/api/me", &admin).await;
    let (status, _) = send_json(&router, http::Method::PATCH, &format!("/api/users/{}", me["id"]), &admin, json!({ "daily_token_cap": 15 })).await;
    assert_eq!(status, StatusCode::OK);

    // The same tokens dated to an earlier day are outside the window the limit counts.
    sqlx::query("UPDATE usage SET created_at = datetime('now', '-1 day') WHERE user_id = ?")
        .bind(me["id"].as_i64().unwrap())
        .execute(&st.pool)
        .await
        .unwrap();
    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
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
    let member = register(&router, "ben").await;
    let member_id = get_json(&router, "/api/me", &member).await;
    let path = format!("/api/users/{}", member_id["id"]);

    let (status, _) = send_json(&router, http::Method::PATCH, &path, &member, json!({ "daily_token_cap": 100 })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, body) = send_json(&router, http::Method::PATCH, &path, &admin, json!({ "daily_token_cap": 100 })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["daily_token_cap"]));

    // An empty value clears the limit, which COALESCE would not allow.
    let (status, body) = send_json(&router, http::Method::PATCH, &path, &admin, json!({ "daily_token_cap": null })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["daily_token_cap"]));
    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    let ben = summary["users"].as_array().unwrap().iter().find(|user| user["username"] == "ben").unwrap();
    assert!(ben["daily_token_cap"].is_null(), "the limit was cleared, not left at 100");

    let (status, _) = send_json(&router, http::Method::PATCH, &path, &admin, json!({ "daily_token_cap": 0 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Accounts without any usage are listed too, so a limit can be set before the first request.
    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(summary["users"].as_array().unwrap().len(), 2);
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
    let member = register(&router, "ben").await;
    let member_id = get_json(&router, "/api/me", &member).await["id"].as_i64().unwrap();

    post_json(&router, "/api/connections", &admin, json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" })).await;
    let (_, created) = post_json(&router, "/api/keys", &member, json!({ "name": "member key" })).await;
    let key = created["key"].as_str().unwrap().to_string();

    let (status, body) = send_json(&router, http::Method::PATCH, &format!("/api/users/{member_id}"), &admin, json!({ "enabled": false })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["enabled"]));

    // Disabling signs the account out, so the session it was already using stops working.
    let response = call(&router, Request::get("/api/me").header("cookie", &member).body(Body::empty()).unwrap()).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (status, _) = post_json(&router, "/api/login", "", json!({ "username": "ben", "password": "test-password" })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The key is refused too, before any limit is looked at.
    let response = gateway_call(&router, &key, json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(json_body(response).await["error"]["type"], "account_disabled");
}

#[tokio::test]
async fn an_admin_can_rename_an_account_and_set_a_new_password() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    open_registration(&router, &admin).await;
    let member = register(&router, "ben").await;
    let member_id = get_json(&router, "/api/me", &member).await["id"].as_i64().unwrap();
    let path = format!("/api/users/{member_id}");

    let (status, body) = send_json(&router, http::Method::PATCH, &path, &admin, json!({ "username": "bob" })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["username"]));

    // A username that is taken is refused rather than silently overwritten, and an empty one
    // is refused before anything is written.
    let (status, _) = send_json(&router, http::Method::PATCH, &path, &admin, json!({ "username": "ada" })).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = send_json(&router, http::Method::PATCH, &path, &admin, json!({ "username": " " })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Only the field in the body is applied, so setting a password leaves the limit alone.
    send_json(&router, http::Method::PATCH, &path, &admin, json!({ "daily_token_cap": 100 })).await;
    let (status, body) = send_json(&router, http::Method::PATCH, &path, &admin, json!({ "password": "another-password" })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], json!(["password"]));

    let (status, _) = post_json(&router, "/api/login", "", json!({ "username": "bob", "password": "another-password" })).await;
    assert_eq!(status, StatusCode::OK);

    let summary = get_json(&router, "/api/stats/summary", &admin).await;
    let bob = summary["users"].as_array().unwrap().iter().find(|user| user["username"] == "bob").unwrap();
    assert_eq!(bob["daily_token_cap"], 100, "the password change did not clear the limit");
}

#[tokio::test]
async fn deleting_an_account_takes_its_keys_and_its_usage_with_it() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let admin = register(&router, "ada").await;
    let admin_id = get_json(&router, "/api/me", &admin).await["id"].as_i64().unwrap();
    open_registration(&router, &admin).await;
    let member = register(&router, "ben").await;
    let member_id = get_json(&router, "/api/me", &member).await["id"].as_i64().unwrap();
    let path = format!("/api/users/{member_id}");

    post_json(&router, "/api/connections", &admin, json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" })).await;
    let (_, created) = post_json(&router, "/api/keys", &member, json!({ "name": "member key" })).await;
    gateway_call(&router, created["key"].as_str().unwrap(), json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;
    assert_eq!(usage_count(&st.pool, member_id).await, 1);

    // A member is not an admin, and an admin cannot remove the account they are signed in with.
    let (status, _) = send_json(&router, http::Method::DELETE, &path, &member, json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send_json(&router, http::Method::DELETE, &format!("/api/users/{admin_id}"), &admin, json!({})).await;
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
        "created_at,daily_token_cap,enabled,id,password_hash,role,username"
    );

    // The account keeps its name and its limit; only the column names changed, and the email is
    // gone with no place left for it.
    let row = sqlx::query("SELECT username, role, daily_token_cap, enabled FROM users").fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<String, _>("username"), "ada");
    assert_eq!(row.get::<i64, _>("daily_token_cap"), 500, "the stored limit still applies");
    assert_eq!(row.get::<i64, _>("enabled"), 1, "an existing account is enabled by default");

    // Running it again on a database that is already current changes nothing.
    db::migrate(&pool).await.unwrap();
    assert_eq!(
        user_columns(&pool).await.join(","),
        "created_at,daily_token_cap,enabled,id,password_hash,role,username"
    );

    // Rebuilding the table must not take the rows that belong to the account with it.
    assert_eq!(usage_count(&pool, 1).await, 1);
    assert_eq!(count(&pool, "api_keys").await, 1);
    assert_eq!(count(&pool, "sessions").await, 1);

    // Foreign keys are left on, so removing an account still takes its keys, usage and sessions.
    sqlx::query("DELETE FROM users WHERE id = 1").execute(&pool).await.unwrap();
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
        "created_at,daily_token_cap,enabled,id,password_hash,role,username"
    );
    let row = sqlx::query("SELECT username, daily_token_cap, enabled FROM users").fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<String, _>("username"), "ada");
    assert_eq!(row.get::<i64, _>("daily_token_cap"), 500);
    assert_eq!(count(&pool, "sessions").await, 1, "the sign-in for this account is still there");
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

#[tokio::test]
async fn the_summary_separates_the_callers_own_figures_from_the_platform_total() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;

    let admin = register(&router, "ada").await;
    post_json(&router, "/api/connections", &admin, json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" })).await;
    let (_, admin_key) = post_json(&router, "/api/keys", &admin, json!({ "name": "admin key" })).await;
    gateway_call(&router, admin_key["key"].as_str().unwrap(), json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;

    open_registration(&router, &admin).await;
    let member = register(&router, "ben").await;
    let (_, member_key) = post_json(&router, "/api/keys", &member, json!({ "name": "member key" })).await;
    gateway_call(&router, member_key["key"].as_str().unwrap(), json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;

    // The summary reports the caller's own figures and the platform total separately, so the
    // page does not have to infer which one it is showing.
    let admin_summary = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(admin_summary["self"]["requests"], 1, "an admin's own figures are their own");
    assert_eq!(admin_summary["self"]["total_tokens"], 18);
    assert_eq!(admin_summary["global"]["requests"], 2);
    assert_eq!(admin_summary["global"]["total_tokens"], 36);
    assert_eq!(admin_summary["hourly"].as_array().unwrap().len(), 1, "the charts are per account");
    assert_eq!(admin_summary["models"].as_array().unwrap().len(), 1);
    assert_eq!(admin_summary["users"].as_array().unwrap().len(), 2);

    let member_summary = get_json(&router, "/api/stats/summary", &member).await;
    assert_eq!(member_summary["self"]["requests"], 1, "{member_summary}");
    assert_eq!(member_summary["self"]["total_tokens"], 18);
    assert_eq!(member_summary["global"]["requests"], 2, "the platform total is not scoped");
    assert_eq!(member_summary["users"].as_array().unwrap().len(), 0, "a member does not see other accounts");

    let recent = get_json(&router, "/api/stats/recent", &member).await;
    assert_eq!(recent["events"].as_array().unwrap().len(), 1);
    assert_eq!(recent["events"][0]["user"], "ben");
}

#[tokio::test]
async fn presence_counts_an_account_while_its_request_is_running() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let admin = register(&router, "ada").await;
    let admin_id = get_json(&router, "/api/me", &admin).await["id"].as_i64().unwrap();

    // The gateway holds the same guard for the length of a request, so holding it here is what
    // a running request looks like: generating and online at the same time.
    let active = presence::track(&st, admin_id);
    let during = get_json(&router, "/api/presence", &admin).await;
    assert_eq!(during["online"], 1);
    assert_eq!(during["generating"], 1);
    assert_eq!(during["users"][0]["username"], "ada");

    drop(active);
    let after = get_json(&router, "/api/presence", &admin).await;
    assert_eq!(after["generating"], 0, "the guard is released when the request ends");
    assert_eq!(after["online"], 1, "still inside the five minute window");

    // Names are scoped like the rest of the stats: a member sees the counts, not the accounts.
    open_registration(&router, &admin).await;
    let member = register(&router, "ben").await;
    let seen = get_json(&router, "/api/presence", &member).await;
    assert_eq!(seen["users"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn endpoint_urls_follow_the_openai_convention() {
    assert_eq!(dreamgate::registry::endpoint("http://localhost:11434", "models"), "http://localhost:11434/v1/models");
    assert_eq!(dreamgate::registry::endpoint("https://api.example.com/v1/", "models"), "https://api.example.com/v1/models");
}
