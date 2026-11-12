use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::{State as AxumState};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use futures_util::{stream, StreamExt};
use serde_json::{json, Value};
use sqlx::Row;

use crate::api::error;
use crate::presence::{self, Active};
use crate::registry::{self, Connection};
use crate::state::State;
use crate::tokens;
use crate::usage::{self, UsageRow};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
pub struct KeyInfo {
    pub id: i64,
    pub user_id: i64,
    pub name: String,
    pub connection_id: Option<i64>,
    pub model: Option<String>,
    pub enabled: bool,
    pub rpm_limit: Option<i64>,
    pub token_cap: Option<i64>,
}

fn virtual_key(headers: &HeaderMap) -> Option<String> {
    if let Some(rest) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        return Some(rest.trim().to_string());
    }
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
}

async fn lookup_key(st: &State, raw: &str) -> Option<KeyInfo> {
    let row = sqlx::query(
        "SELECT id, user_id, name, connection_id, model, enabled, rpm_limit, token_cap \
         FROM api_keys WHERE key = ?",
    )
    .bind(raw)
    .fetch_optional(&st.pool)
    .await
    .ok()
    .flatten()?;
    Some(KeyInfo {
        id: row.get::<i64, _>("id"),
        user_id: row.get::<i64, _>("user_id"),
        name: row.get::<String, _>("name"),
        connection_id: row.get::<Option<i64>, _>("connection_id"),
        model: row.get::<Option<String>, _>("model"),
        enabled: row.get::<i64, _>("enabled") != 0,
        rpm_limit: row.get::<Option<i64>, _>("rpm_limit"),
        token_cap: row.get::<Option<i64>, _>("token_cap"),
    })
}

async fn enforce_limits(st: &State, key: &KeyInfo) -> Option<Response> {
    if let Some(limit) = key.rpm_limit {
        let recent = sqlx::query(
            "SELECT COUNT(*) AS n FROM usage WHERE api_key_id = ? \
             AND strftime('%Y-%m-%d %H:%M:%S', created_at) >= datetime('now', '-1 minute')",
        )
            .bind(key.id)
            .fetch_one(&st.pool)
            .await
            .map(|row| row.get::<i64, _>("n"))
            .unwrap_or(0);
        if recent >= limit {
            return Some(error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit",
                format!("key '{}' is limited to {limit} requests per minute", key.name),
            ));
        }
    }
    if let Some(cap) = key.token_cap {
        let used = sqlx::query("SELECT COALESCE(SUM(total_tokens), 0) AS used FROM usage WHERE api_key_id = ?")
            .bind(key.id)
            .fetch_one(&st.pool)
            .await
            .map(|row| row.get::<i64, _>("used"))
            .unwrap_or(0);
        if used >= cap {
            return Some(error(
                StatusCode::TOO_MANY_REQUESTS,
                "token_cap",
                format!("key '{}' has reached its token cap of {cap}", key.name),
            ));
        }
    }
    // The account is checked before its limits, because a disabled account cannot use a key at
    // all. The account limit counts the current day only and resets at 00:00 UTC, matching the
    // UTC text the usage rows are stored as. It is checked after the key cap, so a key cannot
    // be used to work around it.
    let account = sqlx::query("SELECT username, enabled, daily_token_cap FROM users WHERE id = ?")
        .bind(key.user_id)
        .fetch_optional(&st.pool)
        .await
        .ok()
        .flatten();
    if let Some(account) = account {
        if account.get::<i64, _>("enabled") == 0 {
            return Some(error(
                StatusCode::FORBIDDEN,
                "account_disabled",
                format!("account '{}' is disabled", account.get::<String, _>("username")),
            ));
        }
        if let Some(cap) = account.get::<Option<i64>, _>("daily_token_cap") {
            let used = sqlx::query(
                "SELECT COALESCE(SUM(total_tokens), 0) AS used FROM usage \
                 WHERE user_id = ? AND strftime('%Y-%m-%d', created_at) = date('now')",
            )
                .bind(key.user_id)
                .fetch_one(&st.pool)
                .await
                .map(|row| row.get::<i64, _>("used"))
                .unwrap_or(0);
            if used >= cap {
                return Some(error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "token_cap",
                    format!("account '{}' has used its daily token limit of {cap}", account.get::<String, _>("username")),
                ));
            }
        }
    }
    None
}

fn resolve_model(key_model: Option<&str>, conn: &Connection, requested: Option<&str>) -> String {
    key_model
        .map(String::from)
        .or_else(|| conn.default_model.clone())
        .unwrap_or_else(|| requested.unwrap_or("unknown").to_string())
}

fn build_payload(body: &Value, model: &str) -> Value {
    let mut payload = body.clone();
    payload["model"] = Value::String(model.to_string());
    // Upstreams that support stream_options report real usage at the end of the
    // stream; without it the gateway has to estimate completion tokens.
    if body.get("stream").and_then(Value::as_bool).unwrap_or(false) && payload.get("stream_options").is_none() {
        payload["stream_options"] = json!({ "include_usage": true });
    }
    payload
}

// The text a response part carries. Reasoning models put their thinking in reasoning_content
// (or reasoning) beside the answer, and it is part of what the model produced, so both count as
// generated text when the upstream reports no usage.
const TEXT_PARTS: [&str; 3] = ["content", "reasoning_content", "reasoning"];

fn text_of(part: &Value) -> String {
    TEXT_PARTS
        .iter()
        .filter_map(|key| part.get(key).and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join(" ")
}

// Forwarded bytes go to the client unchanged; this only reads complete SSE lines to
// count tokens, so upstreams with non-standard stream bodies still work.
fn scan_line(line: &[u8], generated: &mut String, reported: &mut Option<(i64, i64)>) {
    let Ok(text) = std::str::from_utf8(line) else {
        return;
    };
    let Some(data) = text.strip_prefix("data:").map(str::trim) else {
        return;
    };
    if data == "[DONE]" {
        return;
    }
    let Ok(value) = serde_json::from_str::<Value>(data) else {
        return;
    };
    if let Some(block) = value.get("usage") {
        let prompt = block.get("prompt_tokens").and_then(Value::as_i64);
        let completion = block.get("completion_tokens").and_then(Value::as_i64);
        if prompt.is_some() || completion.is_some() {
            *reported = Some((prompt.unwrap_or(0), completion.unwrap_or(0)));
        }
    }
    for choice in value.get("choices").and_then(Value::as_array).into_iter().flatten() {
        let Some(delta) = choice.get("delta") else {
            continue;
        };
        let text = text_of(delta);
        if text.is_empty() {
            continue;
        }
        if !generated.is_empty() {
            generated.push(' ');
        }
        generated.push_str(&text);
    }
}

fn drain_buffer(buffer: &mut Vec<u8>, generated: &mut String, reported: &mut Option<(i64, i64)>) {
    while let Some(index) = buffer.iter().position(|byte| *byte == b'\n') {
        let line: Vec<u8> = buffer.drain(..=index).collect();
        scan_line(&line[..line.len() - 1], generated, reported);
    }
}

async fn record(st: &State, row: &UsageRow) {
    if let Err(err) = usage::record(st, row).await {
        tracing::warn!("usage record failed: {err}");
    }
}

// What the stream task still needs after the handler has returned, passed as one value rather
// than as eight arguments.
struct StreamContext {
    st: State,
    key: KeyInfo,
    conn: Connection,
    model: String,
    present: Active,
}

fn stream_body(
    ctx: StreamContext,
    upstream: reqwest::Response,
    started: Instant,
    prompt_tokens: i64,
) -> impl futures_util::Stream<Item = Result<Bytes, BoxError>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, BoxError>>(64);
    tokio::spawn(async move {
        let mut buffer: Vec<u8> = Vec::new();
        let mut generated = String::new();
        let mut reported: Option<(i64, i64)> = None;
        let mut incoming = upstream.bytes_stream();
        while let Some(chunk) = incoming.next().await {
            match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                    drain_buffer(&mut buffer, &mut generated, &mut reported);
                    if tx.send(Ok(bytes)).await.is_err() {
                        break; // the client disconnected mid-stream
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(BoxError::from(err))).await;
                    break;
                }
            }
        }
        let (prompt, completion, estimated) = match reported {
            Some((p, c)) => (p, c, false),
            None => (prompt_tokens, tokens::estimate_tokens(&generated), true),
        };
        let row = UsageRow {
            api_key_id: ctx.key.id,
            user_id: ctx.key.user_id,
            connection_id: Some(ctx.conn.id),
            model: Some(ctx.model),
            prompt_tokens: prompt,
            completion_tokens: completion,
            total_tokens: prompt + completion,
            latency_ms: started.elapsed().as_millis() as i64,
            status: 200,
            streamed: true,
            estimated,
            created_at: crate::clock::now(),
        };
        if let Err(err) = usage::record(&ctx.st, &row).await {
            tracing::warn!("usage record failed: {err}");
        }
        // Released here rather than when the handler returned, so a stream is still counted
        // while it is running.
        drop(ctx.present);
    });
    stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|item| (item, rx)) })
}

pub async fn chat_completions(AxumState(st): AxumState<State>, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    let started = Instant::now();
    let Some(raw) = virtual_key(&headers) else {
        return error(StatusCode::UNAUTHORIZED, "invalid_request", "missing Authorization: Bearer <virtual key>");
    };
    let Some(key) = lookup_key(&st, &raw).await else {
        return error(StatusCode::UNAUTHORIZED, "invalid_api_key", "unknown virtual key");
    };
    if !key.enabled {
        return error(StatusCode::FORBIDDEN, "key_disabled", "this key is disabled");
    }
    // Counted from here, so a request that is refused by a limit still marks the account as
    // active; the guard is released when the request finishes, including for a stream.
    let present = presence::track(&st, key.user_id);
    if let Some(blocked) = enforce_limits(&st, &key).await {
        return blocked;
    }

    let candidates = registry::candidates(&st, key.connection_id).await;
    if candidates.is_empty() {
        return error(StatusCode::BAD_GATEWAY, "no_upstream", "no enabled connection is available for this key");
    }
    let wants_stream = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let prompt_tokens = tokens::prompt_estimate(body.get("messages").unwrap_or(&Value::Null));
    let requested = body.get("model").and_then(Value::as_str);
    let mut last_error = String::from("no connection responded");

    for conn in &candidates {
        if conn.health == "down" && candidates.len() > 1 {
            continue;
        }
        let model = resolve_model(key.model.as_deref(), conn, requested);
        let url = registry::endpoint(&conn.base_url, "chat/completions");
        let payload = build_payload(&body, &model);
        let upstream = match st
            .http
            .post(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}", conn.api_key.clone().unwrap_or_default()))
            .json(&payload)
            .send()
            .await
        {
            Ok(upstream) => upstream,
            Err(err) => {
                last_error = format!("{}: {err}", conn.name);
                continue;
            }
        };

        let status = upstream.status();
        if wants_stream && status.is_success() {
            let mut out = Response::new(Body::empty());
            *out.status_mut() = status;
            out.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
            out.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            let ctx = StreamContext {
                st: st.clone(),
                key: key.clone(),
                conn: conn.clone(),
                model,
                present,
            };
            *out.body_mut() = Body::from_stream(stream_body(ctx, upstream, started, prompt_tokens));
            return out;
        }

        let bytes = match upstream.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                last_error = format!("{}: {err}", conn.name);
                continue;
            }
        };
        let parsed = serde_json::from_str::<Value>(&String::from_utf8_lossy(&bytes)).ok();
        let block = parsed.as_ref().and_then(|value| value.get("usage"));
        let completion_text = parsed
            .as_ref()
            .and_then(|value| value.get("choices").and_then(Value::as_array))
            .into_iter()
            .flatten()
            .filter_map(|choice| choice.get("message").map(text_of))
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let (prompt, completion) = match block {
            Some(block) => (
                block.get("prompt_tokens").and_then(Value::as_i64).unwrap_or(prompt_tokens),
                block.get("completion_tokens").and_then(Value::as_i64).unwrap_or(0),
            ),
            None => (prompt_tokens, tokens::estimate_tokens(&completion_text)),
        };
        record(
            &st,
            &UsageRow {
                api_key_id: key.id,
                user_id: key.user_id,
                connection_id: Some(conn.id),
                model: Some(model),
                prompt_tokens: prompt,
                completion_tokens: completion,
                total_tokens: prompt + completion,
                latency_ms: started.elapsed().as_millis() as i64,
                status: status.as_u16() as i64,
                streamed: false,
                estimated: block.is_none(),
                created_at: crate::clock::now(),
            },
        ).await;

        let mut out = Response::new(Body::empty());
        *out.status_mut() = status;
        out.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(if parsed.is_some() { "application/json" } else { "text/plain; charset=utf-8" }),
        );
        *out.body_mut() = Body::from(bytes);
        return out;
    }

    record(
        &st,
        &UsageRow {
            api_key_id: key.id,
            user_id: key.user_id,
            connection_id: None,
            model: key.model.clone().or_else(|| requested.map(String::from)),
            prompt_tokens,
            completion_tokens: 0,
            total_tokens: prompt_tokens,
            latency_ms: started.elapsed().as_millis() as i64,
            status: 502,
            streamed: wants_stream,
            estimated: true,
            created_at: crate::clock::now(),
        },
    ).await;
    error(StatusCode::BAD_GATEWAY, "upstream_unavailable", last_error)
}

pub async fn list_models(AxumState(st): AxumState<State>, headers: HeaderMap) -> Response {
    let Some(raw) = virtual_key(&headers) else {
        return error(StatusCode::UNAUTHORIZED, "invalid_request", "missing Authorization: Bearer <virtual key>");
    };
    let Some(key) = lookup_key(&st, &raw).await else {
        return error(StatusCode::UNAUTHORIZED, "invalid_api_key", "unknown virtual key");
    };
    if !key.enabled {
        return error(StatusCode::FORBIDDEN, "key_disabled", "this key is disabled");
    }
    let Some(conn) = registry::candidates(&st, key.connection_id).await.first().cloned() else {
        return error(StatusCode::BAD_GATEWAY, "no_upstream", "no enabled connection is available for this key");
    };
    let result = st
        .http
        .get(registry::endpoint(&conn.base_url, "models"))
        .header(header::AUTHORIZATION, format!("Bearer {}", conn.api_key.clone().unwrap_or_default()))
        .send()
        .await;
    match result {
        Ok(upstream) => {
            let status = upstream.status();
            let bytes = match upstream.bytes().await {
                Ok(bytes) => bytes,
                Err(err) => return error(StatusCode::BAD_GATEWAY, "upstream_unavailable", format!("{}: {err}", conn.name)),
            };
            let mut out = Response::new(Body::empty());
            *out.status_mut() = status;
            out.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            *out.body_mut() = Body::from(bytes);
            out.into_response()
        }
        Err(err) => error(StatusCode::BAD_GATEWAY, "upstream_unavailable", format!("{}: {err}", conn.name)),
    }
}
