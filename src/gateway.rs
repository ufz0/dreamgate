use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::State as AxumState;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use crate::api::error;
use crate::limits::Admission;
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

async fn lookup_key(st: &State, raw: &str) -> Result<Option<KeyInfo>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT id, user_id, name, connection_id, model, enabled, rpm_limit, token_cap \
         FROM api_keys WHERE key = ?",
    )
    .bind(raw)
    .fetch_optional(&st.pool)
    .await?;
    Ok(row.map(|row| KeyInfo {
        id: row.get::<i64, _>("id"),
        user_id: row.get::<i64, _>("user_id"),
        name: row.get::<String, _>("name"),
        connection_id: row.get::<Option<i64>, _>("connection_id"),
        model: row.get::<Option<String>, _>("model"),
        enabled: row.get::<i64, _>("enabled") != 0,
        rpm_limit: row.get::<Option<i64>, _>("rpm_limit"),
        token_cap: row.get::<Option<i64>, _>("token_cap"),
    }))
}

fn unknown_key() -> Response {
    error(
        StatusCode::UNAUTHORIZED,
        "invalid_api_key",
        "unknown virtual key",
    )
}

// The key, its account and its limits are read on every request, and a read that fails (a write
// holding the database past the busy timeout, a disk that fails) refuses the request. A limit that
// could not be read is not a limit that was not reached: letting the request through would let a
// disabled account, or a key past its cap, reach an upstream whenever the database was slow. The
// answer is 503 because the refusal is the gateway's and passes, so a client that retries is right to.
fn unreadable(err: sqlx::Error) -> Response {
    tracing::error!("a key, its account or its limits could not be read: {err}");
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        "the gateway could not read this key's account and limits; try again",
    )
}

// A disabled account cannot use a key at all. Both gateway endpoints read the account the same
// way, so a key on its own cannot reach an upstream for an account that is turned off. Deleting an
// account deletes its keys with it, so a key whose account is gone is a key that is being deleted,
// and both endpoints answer it the way they answer a key that no longer exists.
async fn account_row(st: &State, user_id: i64) -> Result<Option<SqliteRow>, sqlx::Error> {
    sqlx::query("SELECT username, enabled, daily_token_cap FROM users WHERE id = ?")
        .bind(user_id)
        .fetch_optional(&st.pool)
        .await
}

fn disabled_account(row: &SqliteRow) -> Option<Response> {
    if row.get::<i64, _>("enabled") == 0 {
        return Some(error(
            StatusCode::FORBIDDEN,
            "account_disabled",
            format!("account '{}' is disabled", row.get::<String, _>("username")),
        ));
    }
    None
}

async fn enforce_limits(st: &State, key: &KeyInfo) -> Option<Response> {
    if let Some(limit) = key.rpm_limit {
        let recent = match sqlx::query(
            "SELECT COUNT(*) AS n FROM usage WHERE api_key_id = ? \
             AND strftime('%Y-%m-%d %H:%M:%S', created_at) >= datetime('now', '-1 minute')",
        )
        .bind(key.id)
        .fetch_one(&st.pool)
        .await
        {
            Ok(row) => row.get::<i64, _>("n"),
            Err(err) => return Some(unreadable(err)),
        };
        if recent >= limit {
            return Some(error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit",
                format!(
                    "key '{}' is limited to {limit} requests per minute",
                    key.name
                ),
            ));
        }
    }
    if let Some(cap) = key.token_cap {
        let used = match sqlx::query(
            "SELECT COALESCE(SUM(total_tokens), 0) AS used FROM usage WHERE api_key_id = ?",
        )
        .bind(key.id)
        .fetch_one(&st.pool)
        .await
        {
            Ok(row) => row.get::<i64, _>("used"),
            Err(err) => return Some(unreadable(err)),
        };
        if used >= cap {
            return Some(error(
                StatusCode::TOO_MANY_REQUESTS,
                "token_cap",
                format!(
                    "key '{}' has reached its token cap of {cap} input and output tokens",
                    key.name
                ),
            ));
        }
    }
    // The account is checked before its limits, because a disabled account cannot use a key at
    // all. The account limit counts the current day only and resets at 00:00 UTC, matching the
    // UTC text the usage rows are stored as. It is checked after the key cap, so a key cannot
    // be used to work around it.
    let account = match account_row(st, key.user_id).await {
        Ok(Some(account)) => account,
        Ok(None) => return Some(unknown_key()),
        Err(err) => return Some(unreadable(err)),
    };
    if let Some(blocked) = disabled_account(&account) {
        return Some(blocked);
    }
    if let Some(cap) = account.get::<Option<i64>, _>("daily_token_cap") {
        let used = match sqlx::query(
            "SELECT COALESCE(SUM(total_tokens), 0) AS used FROM usage \
             WHERE user_id = ? AND strftime('%Y-%m-%d', created_at) = date('now')",
        )
        .bind(key.user_id)
        .fetch_one(&st.pool)
        .await
        {
            Ok(row) => row.get::<i64, _>("used"),
            Err(err) => return Some(unreadable(err)),
        };
        if used >= cap {
            return Some(error(
                StatusCode::TOO_MANY_REQUESTS,
                "token_cap",
                format!(
                    "account '{}' has used its daily token limit of {cap} input and output tokens",
                    account.get::<String, _>("username")
                ),
            ));
        }
    }
    None
}

fn resolve_model(
    key_model: Option<&str>,
    conn: &Connection,
    requested: Option<&str>,
) -> Option<String> {
    key_model
        .map(String::from)
        .or_else(|| conn.default_model.clone())
        .or_else(|| requested.map(String::from))
}

fn build_payload(body: &Value, model: Option<&str>) -> Value {
    let mut payload = body.clone();
    // When nothing names a model the body keeps what the client sent, so the upstream answers with its own
    // default rather than with a name nobody chose.
    if let Some(model) = model {
        payload["model"] = Value::String(model.to_string());
    }
    // Upstreams that support stream_options report real usage at the end of the
    // stream; without it the gateway has to estimate completion tokens.
    if body.get("stream").and_then(Value::as_bool).unwrap_or(false)
        && payload.get("stream_options").is_none()
    {
        payload["stream_options"] = json!({ "include_usage": true });
    }
    payload
}

// The text a response part carries. Reasoning models put their thinking in reasoning_content
// (or reasoning) beside the answer, and it is part of what the model produced, so both count as
// generated text when the upstream reports no usage.
const TEXT_PARTS: [&str; 3] = ["content", "reasoning_content", "reasoning"];

// An estimate counts everything the model produced, not only the answer: the reasoning text beside the
// reply, and the arguments of a tool call. The tool name is left out. That is a deliberate omission,
// not a claim that a name is not generated — any real tokenizer would count it — and it is one of the
// ways an estimate sits below what an upstream reports.
fn text_of(part: &Value) -> String {
    let mut pieces: Vec<String> = TEXT_PARTS
        .iter()
        .filter_map(|key| part.get(key).and_then(Value::as_str).map(str::to_string))
        .collect();
    for call in part
        .get("tool_calls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(arguments) = call
            .get("function")
            .and_then(|function| function.get("arguments"))
        else {
            continue;
        };
        // OpenAI sends the arguments as a JSON string and some servers send them already parsed. Both
        // are text the model wrote, so the estimate reads either as the text a tokenizer would see.
        match arguments {
            Value::String(text) => pieces.push(text.to_string()),
            Value::Object(_) | Value::Array(_) => pieces.push(arguments.to_string()),
            _ => {}
        }
    }
    pieces.join(" ")
}

// What an upstream reported about a request. Every field is optional, because not every engine reports
// all of it: a prompt cache only appears from servers that keep one, and timings only from builds that
// report them. A field the upstream did not report stays unset, and is stored as unknown.
#[derive(Default)]
struct Report {
    prompt: Option<i64>,
    completion: Option<i64>,
    cached: Option<i64>,
    prompt_ms: Option<i64>,
    generation_ms: Option<i64>,
}

impl Report {
    // A row is an estimate only when no token counts were reported. A cache hit or a timing on its own
    // does not make the counts figures the upstream gave.
    fn reported(&self) -> bool {
        self.prompt.is_some() || self.completion.is_some()
    }
}

// OpenAI-shaped servers put the token counts in `usage` and the prompt cache hit in
// `prompt_tokens_details`; a llama.cpp build reports the same figures in `timings`, where the cache hit
// is `cache_n`. Either shape is read from wherever it is.
fn read_report(value: &Value, report: &mut Report) {
    if let Some(block) = value.get("usage") {
        keep(
            &mut report.prompt,
            block.get("prompt_tokens").and_then(Value::as_i64),
        );
        keep(
            &mut report.completion,
            block.get("completion_tokens").and_then(Value::as_i64),
        );
        keep(
            &mut report.cached,
            block
                .get("prompt_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
                .and_then(Value::as_i64),
        );
    }
    if let Some(timings) = value.get("timings") {
        keep(
            &mut report.cached,
            timings.get("cache_n").and_then(Value::as_i64),
        );
        keep(
            &mut report.prompt_ms,
            whole_millis(timings.get("prompt_ms")),
        );
        keep(
            &mut report.generation_ms,
            whole_millis(timings.get("predicted_ms")),
        );
    }
}

// A stream that says the same thing twice keeps the last answer, as the counting path did before: the
// final chunk is the complete report, and a chunk before it may only hold part of one.
fn keep(field: &mut Option<i64>, value: Option<i64>) {
    if let Some(number) = value {
        *field = Some(number);
    }
}

// Timings arrive as numbers with decimals and are stored as whole milliseconds. A value that is not a
// number is not a timing, so it stays unknown rather than being read as zero.
fn whole_millis(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_f64).map(|ms| ms.round() as i64)
}

// Forwarded bytes go to the client unchanged; this only reads complete SSE lines to
// count tokens, so upstreams with non-standard stream bodies still work.
fn scan_line(line: &[u8], generated: &mut String, report: &mut Report) {
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
    read_report(&value, report);
    for choice in value
        .get("choices")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
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

fn drain_buffer(buffer: &mut Vec<u8>, generated: &mut String, report: &mut Report) {
    while let Some(index) = buffer.iter().position(|byte| *byte == b'\n') {
        let line: Vec<u8> = buffer.drain(..=index).collect();
        scan_line(&line[..line.len() - 1], generated, report);
    }
}

async fn record(st: &State, row: &UsageRow) {
    if let Err(err) = usage::record(st, row).await {
        tracing::error!("usage record failed: {err}");
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
    admission: Admission,
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
        let mut report = Report::default();
        let mut incoming = upstream.bytes_stream();
        let inflight = presence::start_stream(
            &ctx.st,
            ctx.key.user_id,
            &ctx.key.name,
            &ctx.model,
            prompt_tokens,
        );
        // The deltas are joined with a space and the estimate counts words, so the text added since the last
        // chunk can be estimated on its own and added to the rest: the sum is the estimate of the whole, and
        // the whole is not read again on every chunk.
        let mut estimate = 0i64;
        let mut estimated_upto = 0usize;
        // A stream is bounded by how long it goes quiet, not by a deadline for its whole length: an answer that
        // keeps arriving is not stalled, however long it takes, and cutting one that is still coming is the
        // socket dying on the client's side. Silence for the window is a dead connection, and the request
        // should not hold its slot for one forever.
        let stall = ctx.st.limits.stream_stall();
        loop {
            let chunk = match tokio::time::timeout(stall, incoming.next()).await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(_) => {
                    tracing::warn!(
                        "upstream went quiet for {stall:?} and the stream was closed: {}",
                        ctx.model
                    );
                    // The client has already been sent a 200, so there is no status left to change: the stream
                    // ends as an error rather than as a completed answer, which is what lets a client tell an
                    // answer that was cut from one that finished.
                    let _ = tx
                        .send(Err(BoxError::from("the upstream went quiet")))
                        .await;
                    break;
                }
            };
            match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                    drain_buffer(&mut buffer, &mut generated, &mut report);
                    estimate += tokens::estimate_tokens(&generated[estimated_upto..]);
                    estimated_upto = generated.len();
                    // What the upstream has reported once it has reported it, and the estimate until then.
                    // The generated count is what the page shows moving, so it is only called reported
                    // when the upstream has reported that count itself, not when it has sent the input's.
                    // The input is shown in a column of its own, so it says the same of itself.
                    inflight.update(
                        report.prompt.unwrap_or(prompt_tokens),
                        report.completion.unwrap_or(estimate),
                        report.prompt.is_none(),
                        report.completion.is_none(),
                    );
                    if tx.send(Ok(bytes)).await.is_err() {
                        // The client disconnected mid-stream. The row is still written, with whatever
                        // the upstream had reported up to there; the final usage chunk, which is where a
                        // cache hit and the timings appear, is not.
                        break;
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(BoxError::from(err))).await;
                    break;
                }
            }
        }
        let (prompt, completion, estimated) = if report.reported() {
            (
                report.prompt.unwrap_or(0),
                report.completion.unwrap_or(0),
                false,
            )
        } else {
            (prompt_tokens, estimate, true)
        };
        let stream = inflight.stream();
        let row = UsageRow {
            api_key_id: ctx.key.id,
            user_id: ctx.key.user_id,
            connection_id: Some(ctx.conn.id),
            model: Some(ctx.model),
            prompt_tokens: prompt,
            completion_tokens: completion,
            total_tokens: prompt + completion,
            cached_tokens: report.cached,
            prompt_ms: report.prompt_ms,
            generation_ms: report.generation_ms,
            latency_ms: started.elapsed().as_millis() as i64,
            status: 200,
            streamed: true,
            estimated,
            created_at: crate::clock::now(),
            stream: Some(stream),
        };
        // The stream stays on the list while its row is written, and leaves it in the step that sends the
        // row's event, so a page goes from one to the other without a moment showing neither.
        let event = match usage::write(&ctx.st, &row).await {
            Ok(event) => Some(event),
            Err(err) => {
                tracing::error!("usage record failed: {err}");
                None
            }
        };
        inflight.settle(event);
        // Released here rather than when the handler returned, so a stream is still counted
        // while it is running. The connection slot is given back at the same point, because the request
        // is still in flight for as long as the stream is being read.
        drop(ctx.present);
        drop(ctx.admission);
    });
    stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
}

pub async fn chat_completions(
    AxumState(st): AxumState<State>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let Some(raw) = virtual_key(&headers) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_request",
            "missing Authorization: Bearer <virtual key>",
        );
    };
    let key = match lookup_key(&st, &raw).await {
        Ok(Some(key)) => key,
        Ok(None) => return unknown_key(),
        Err(err) => return unreadable(err),
    };
    if !key.enabled {
        return error(
            StatusCode::FORBIDDEN,
            "key_disabled",
            "this key is disabled",
        );
    }
    // Counted from here, so a request that is refused by a limit still marks the account as
    // active; the guard is released when the request finishes, including for a stream.
    let present = presence::track(&st, key.user_id);
    if let Some(blocked) = enforce_limits(&st, &key).await {
        return blocked;
    }

    let mut candidates = registry::candidates(&st, key.connection_id).await;
    if candidates.is_empty() {
        return error(
            StatusCode::BAD_GATEWAY,
            "no_upstream",
            "no enabled connection is available for this key",
        );
    }
    let wants_stream = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let prompt_tokens = tokens::prompt_estimate(body.get("messages").unwrap_or(&Value::Null));
    let requested = body.get("model").and_then(Value::as_str);
    // The request is admitted as a whole, before anything is asked of a connection, and the slots are held for
    // as long as it runs. Nothing is queued here: a connection that is full is skipped, so one slow
    // connection cannot hold unrelated requests behind it. Admitting once, at the start, is also what keeps a
    // request that has already reached a connection answering as that connection failed: once something has
    // crossed the gate it is recorded, so it cannot be turned into an overload on the next attempt.
    let Some(global) = st.limits.try_global() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded",
            format!(
                "the gateway is at its limit of {} requests at once",
                st.limits.global_limit()
            ),
        );
    };
    let mut last_error = String::from("no connection responded");
    // Connections that were already at their limit when this request arrived. They are remembered so that a
    // request which never reached an upstream can be answered as an overload rather than as a failure of an
    // upstream that was never asked.
    let mut full: Vec<String> = Vec::new();
    let mut attempted = false;

    // A connection whose last check failed is tried last rather than skipped. Health is only checked every 30
    // seconds, so one marked down may be answering again, and skipping every connection that is marked down
    // would send a request nowhere when all of them are. The sort is stable, so the connections that are not
    // down keep their order — the key's own connection first, then priority — the ones that are down follow
    // in that same order, and each is still tried once.
    candidates.sort_by_key(|conn| conn.health == "down");
    for conn in &candidates {
        let Ok(slot) = st
            .limits
            .for_connection(conn.id, conn.max_concurrent)
            .try_acquire_owned()
        else {
            full.push(conn.name.clone());
            continue;
        };
        let model = resolve_model(key.model.as_deref(), conn, requested);
        let url = registry::endpoint(&conn.base_url, "chat/completions");
        let payload = build_payload(&body, model.as_deref());
        // The row and the stream in flight name the model the gateway sent, and say unknown when it sent none.
        // The upstream's answer says which model it used, but that is not read here.
        let model = model.unwrap_or_else(|| String::from("unknown"));
        attempted = true;
        let mut request = st
            .http
            .post(&url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", conn.api_key.clone().unwrap_or_default()),
            )
            .json(&payload);
        // Only a single answer is given a deadline for the whole of it, because for that there is an end. A
        // stream is bounded by how long it goes quiet instead, and the same window is what bounds the wait for
        // its head: a connection that takes the request and never answers is a dead connection, not a slow
        // generation. Cutting a stream that is still producing an answer is what the client feels as the socket
        // dying, and that is what this path used to do.
        if !wants_stream {
            request = request.timeout(st.limits.request_timeout());
        }
        // The error is what the client is told, so the URL is taken out of it first: it names an internal
        // host and port, and a credential when a connection carries one in its address. The connection is
        // named instead.
        let sent = if wants_stream {
            tokio::time::timeout(st.limits.stream_stall(), request.send())
                .await
                .map_err(|_| BoxError::from("nothing arrived within the stall window"))
                .and_then(|result| result.map_err(|err| BoxError::from(err.without_url())))
        } else {
            request
                .send()
                .await
                .map_err(|err| BoxError::from(err.without_url()))
        };
        let upstream = match sent {
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
            out.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream"),
            );
            out.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            // The slots are handed to the stream task, so they are held until the stream ends rather than
            // until this handler returns.
            let ctx = StreamContext {
                st: st.clone(),
                key: key.clone(),
                conn: conn.clone(),
                model,
                present,
                admission: Admission {
                    global,
                    connection: slot,
                },
            };
            *out.body_mut() = Body::from_stream(stream_body(ctx, upstream, started, prompt_tokens));
            return out;
        }

        let bytes = match upstream.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                last_error = format!("{}: {}", conn.name, err.without_url());
                continue;
            }
        };
        let parsed = serde_json::from_str::<Value>(&String::from_utf8_lossy(&bytes)).ok();
        let mut report = Report::default();
        if let Some(value) = parsed.as_ref() {
            read_report(value, &mut report);
        }
        let completion_text = parsed
            .as_ref()
            .and_then(|value| value.get("choices").and_then(Value::as_array))
            .into_iter()
            .flatten()
            .filter_map(|choice| choice.get("message").map(text_of))
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        let (prompt, completion, estimated) = if report.reported() {
            (
                report.prompt.unwrap_or(prompt_tokens),
                report.completion.unwrap_or(0),
                false,
            )
        } else {
            (
                prompt_tokens,
                tokens::estimate_tokens(&completion_text),
                true,
            )
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
                cached_tokens: report.cached,
                prompt_ms: report.prompt_ms,
                generation_ms: report.generation_ms,
                latency_ms: started.elapsed().as_millis() as i64,
                status: status.as_u16() as i64,
                streamed: false,
                estimated,
                created_at: crate::clock::now(),
                stream: None,
            },
        )
        .await;

        let mut out = Response::new(Body::empty());
        *out.status_mut() = status;
        out.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(if parsed.is_some() {
                "application/json"
            } else {
                "text/plain; charset=utf-8"
            }),
        );
        *out.body_mut() = Body::from(bytes);
        return out;
    }

    // A request that was never sent because every connection it could use was already at its limit is not
    // recorded as one that crossed the gate. It is answered as an overload, with the limits it ran into.
    if !attempted && !full.is_empty() {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "connection_full",
            format!(
                "every connection available to this key is at its limit of requests at once: {}",
                full.join(", ")
            ),
        );
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
            // Nothing reached an engine, so there is nothing it could have reported about the request.
            cached_tokens: None,
            prompt_ms: None,
            generation_ms: None,
            latency_ms: started.elapsed().as_millis() as i64,
            status: 502,
            streamed: wants_stream,
            estimated: true,
            created_at: crate::clock::now(),
            stream: None,
        },
    )
    .await;
    error(StatusCode::BAD_GATEWAY, "upstream_unavailable", last_error)
}

pub async fn list_models(AxumState(st): AxumState<State>, headers: HeaderMap) -> Response {
    let Some(raw) = virtual_key(&headers) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_request",
            "missing Authorization: Bearer <virtual key>",
        );
    };
    let key = match lookup_key(&st, &raw).await {
        Ok(Some(key)) => key,
        Ok(None) => return unknown_key(),
        Err(err) => return unreadable(err),
    };
    if !key.enabled {
        return error(
            StatusCode::FORBIDDEN,
            "key_disabled",
            "this key is disabled",
        );
    }
    match account_row(&st, key.user_id).await {
        Ok(Some(account)) => {
            if let Some(blocked) = disabled_account(&account) {
                return blocked;
            }
        }
        Ok(None) => return unknown_key(),
        Err(err) => return unreadable(err),
    }
    let Some(conn) = registry::candidates(&st, key.connection_id)
        .await
        .first()
        .cloned()
    else {
        return error(
            StatusCode::BAD_GATEWAY,
            "no_upstream",
            "no enabled connection is available for this key",
        );
    };
    // The model list is a single answer, not a stream, so the same deadline covers it as covers any single
    // answer, as the client's total deadline did before this change.
    let result = st
        .http
        .get(registry::endpoint(&conn.base_url, "models"))
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", conn.api_key.clone().unwrap_or_default()),
        )
        .timeout(st.limits.request_timeout())
        .send()
        .await;
    match result {
        Ok(upstream) => {
            let status = upstream.status();
            let bytes = match upstream.bytes().await {
                Ok(bytes) => bytes,
                Err(err) => {
                    return error(
                        StatusCode::BAD_GATEWAY,
                        "upstream_unavailable",
                        format!("{}: {}", conn.name, err.without_url()),
                    );
                }
            };
            let mut out = Response::new(Body::empty());
            *out.status_mut() = status;
            out.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            *out.body_mut() = Body::from(bytes);
            out.into_response()
        }
        // As on the completion path, the connection is named and its address is not.
        Err(err) => error(
            StatusCode::BAD_GATEWAY,
            "upstream_unavailable",
            format!("{}: {}", conn.name, err.without_url()),
        ),
    }
}
