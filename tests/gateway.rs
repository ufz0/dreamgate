use std::time::Duration;

use axum::{
    body::Body,
    extract::Json,
    http::{self, header, HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use dreamgate::{build_router, db, live, state::State};
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
    }
}

// An OpenAI-compatible upstream with two behaviours: it reports usage, or reports none.
async fn mock_chat(Json(body): Json<Value>) -> Response {
    let reports_usage = body.get("model").and_then(Value::as_str) == Some("reports-usage");
    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) {
        let mut chunks = String::from("data: {\"choices\":[{\"delta\":{\"content\":\"hello world\"}}]}\n\n");
        if reports_usage {
            chunks.push_str("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7}}\n\n");
        }
        chunks.push_str("data: [DONE]\n\n");
        let mut out = Response::new(Body::empty());
        out.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        *out.body_mut() = Body::from(chunks);
        return out;
    }
    let payload = if reports_usage {
        json!({ "choices": [{ "message": { "content": "hello world" } }], "usage": { "prompt_tokens": 11, "completion_tokens": 7 } })
    } else {
        json!({ "choices": [{ "message": { "content": "hello world" } }] })
    };
    Json(payload).into_response()
}

async fn mock_models() -> Json<Value> {
    Json(json!({ "object": "list", "data": [{ "id": "test-model" }] }))
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

async fn register(router: &Router, email: &str, name: &str) -> String {
    let response = call(
        router,
        Request::post("/api/register")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "email": email, "name": name, "password": "test-password" }).to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
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

    let first = post_json(&router, "/api/register", "", json!({ "email": "a@example.com", "name": "Ada", "password": "test-password" })).await;
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(first.1["role"], "admin");

    let second = post_json(&router, "/api/register", "", json!({ "email": "b@example.com", "name": "Ben", "password": "test-password" })).await;
    assert_eq!(second.1["role"], "user");
}

#[tokio::test]
async fn records_usage_reported_by_the_upstream() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "admin@example.com", "Ada").await;

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
    let cookie = register(&router, "admin@example.com", "Ada").await;

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
    let cookie = register(&router, "admin@example.com", "Ada").await;

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
async fn fails_over_to_a_healthy_connection() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "admin@example.com", "Ada").await;

    // Nothing listens on port 1, so the first attempt fails at the transport level.
    post_json(&router, "/api/connections", &cookie, json!({ "name": "dead", "base_url": "http://127.0.0.1:1", "priority": 0 })).await;
    let (_, healthy) = post_json(
        &router,
        "/api/connections",
        &cookie,
        json!({ "name": "live", "base_url": base_url, "default_model": "reports-usage", "priority": 1 }),
    )
    .await;

    let (_, created) = post_json(&router, &format!("/api/keys"), &cookie, json!({ "name": "test key" })).await;
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
    let cookie = register(&router, "admin@example.com", "Ada").await;
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
async fn per_key_token_cap_blocks_the_next_request() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;
    let cookie = register(&router, "admin@example.com", "Ada").await;
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
async fn summary_totals_are_scoped_to_the_caller() {
    let st = test_state().await;
    let router = build_router(st.clone());
    let base_url = mock_upstream().await;

    let admin = register(&router, "admin@example.com", "Ada").await;
    post_json(&router, "/api/connections", &admin, json!({ "name": "test", "base_url": base_url, "default_model": "reports-usage" })).await;
    let (_, admin_key) = post_json(&router, "/api/keys", &admin, json!({ "name": "admin key" })).await;
    gateway_call(&router, admin_key["key"].as_str().unwrap(), json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;

    let member = register(&router, "member@example.com", "Ben").await;
    let (_, member_key) = post_json(&router, "/api/keys", &member, json!({ "name": "member key" })).await;
    gateway_call(&router, member_key["key"].as_str().unwrap(), json!({ "messages": [{ "role": "user", "content": "hi" }] })).await;

    let admin_summary = get_json(&router, "/api/stats/summary", &admin).await;
    assert_eq!(admin_summary["scope"], "all");
    assert_eq!(admin_summary["totals"]["requests"], 2);
    assert_eq!(admin_summary["totals"]["total_tokens"], 36);
    assert_eq!(admin_summary["hourly"].as_array().unwrap().len(), 1, "both requests fall in the current hour");
    assert_eq!(admin_summary["models"].as_array().unwrap().len(), 1);
    assert_eq!(admin_summary["users"].as_array().unwrap().len(), 2);

    let member_summary = get_json(&router, "/api/stats/summary", &member).await;
    assert_eq!(member_summary["scope"].as_str(), Some("self"), "{member_summary}");
    assert_eq!(member_summary["totals"]["requests"], 1);
    assert_eq!(member_summary["totals"]["total_tokens"], 18);
    assert_eq!(member_summary["users"].as_array().unwrap().len(), 0, "a member does not see other accounts");

    let recent = get_json(&router, "/api/stats/recent", &member).await;
    assert_eq!(recent["events"].as_array().unwrap().len(), 1);
    assert_eq!(recent["events"][0]["user"], "Ben");
}

#[tokio::test]
async fn endpoint_urls_follow_the_openai_convention() {
    assert_eq!(dreamgate::registry::endpoint("http://localhost:11434", "models"), "http://localhost:11434/v1/models");
    assert_eq!(dreamgate::registry::endpoint("https://api.example.com/v1/", "models"), "https://api.example.com/v1/models");
}
