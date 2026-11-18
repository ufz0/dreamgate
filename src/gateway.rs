use std::sync::{Arc, OnceLock};
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::State as AxumState;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use crate::api::{error, logged};
use crate::limits::Admission;
use crate::presence::{self, Active};
use crate::registry::{self, Connection};
use crate::reservations::{Ask, Reading, Refusal, Reservation};
use crate::shutdown::Phase;
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

// A request that arrives while the process is stopping is turned away rather than started, because it
// could not be given the time to finish. 503 tells a client to try again, which reaches the process that
// comes next. The same answer goes to a request the gateway ended because it was stopping.
fn shutting_down() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "shutting_down",
        "the gateway is shutting down; try again in a moment",
    )
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
#[track_caller]
fn unreadable(err: sqlx::Error) -> Response {
    logged(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        "the gateway could not read this key's account and limits; try again",
        err,
    )
}

// What a client is told about a failed upstream is its connection's name and the first line of the
// error, without the address. The log is where an admin looks for why, so it gets the whole chain of
// causes — refused, a name that did not resolve, a certificate, a timeout — which the first line
// leaves out.
fn upstream_failed(st: &State, conn: &Connection, err: &(dyn std::error::Error + 'static)) {
    st.metrics.upstream_failure(&conn.name);
    let mut causes = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        causes.push_str(": ");
        causes.push_str(&cause.to_string());
        source = cause.source();
    }
    tracing::warn!(connection = %conn.name, "upstream failed: {causes}");
}

// A disabled account cannot use a key at all. Every gateway endpoint reads the account the same
// way, so a key on its own cannot reach an upstream for an account that is turned off. Deleting an
// account deletes its keys with it, so a key whose account is gone is a key that is being deleted,
// and every endpoint answers it the way it answers a key that no longer exists.
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

// What the rows say a key and its account have used, read here and then checked together with the requests
// the rows cannot show yet in one step (`reservations`), so requests sent at the same moment cannot all pass on
// the same reading. A limit the rows alone have reached refuses at once, without the reads after it. What comes
// back is the request's hold, settled once its row is written; a refusal is the answer the request is given,
// boxed because an answer is large and is only built when one is refused.
async fn enforce_limits(
    st: &State,
    key: &KeyInfo,
    prompt_tokens: i64,
) -> Result<Reservation, Box<Response>> {
    // The account is checked before its limits, because a disabled account cannot use a key at
    // all. The account limit counts the current day only and resets at 00:00 UTC, matching the
    // UTC text the usage rows are stored as. It is checked after the key cap, so a key cannot
    // be used to work around it.
    let account = match account_row(st, key.user_id).await {
        Ok(Some(account)) => account,
        Ok(None) => return Err(Box::new(unknown_key())),
        Err(err) => return Err(Box::new(unreadable(err))),
    };
    if let Some(blocked) = disabled_account(&account) {
        return Err(Box::new(blocked));
    }
    let refuse = |refusal: Refusal| -> Box<Response> {
        Box::new(match refusal {
            Refusal::RateLimit { limit } => error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit",
                format!(
                    "key '{}' is limited to {limit} requests per minute",
                    key.name
                ),
            ),
            Refusal::KeyCap { cap, in_flight } => error(
                StatusCode::TOO_MANY_REQUESTS,
                "token_cap",
                format!(
                    "key '{}' has reached its token cap of {cap} input and output tokens{}",
                    key.name,
                    in_flight_note(in_flight)
                ),
            ),
            Refusal::DayCap { cap, in_flight } => error(
                StatusCode::TOO_MANY_REQUESTS,
                "token_cap",
                format!(
                    "account '{}' has used its daily token limit of {cap} input and output tokens{}",
                    account.get::<String, _>("username"),
                    in_flight_note(in_flight)
                ),
            ),
            // As with a limit that could not be read: a limit not known to be unreached is not one that was
            // not reached.
            Refusal::Stale => {
                tracing::warn!(
                    "the limits of key '{}' took too long to read to be checked against the requests in flight",
                    key.name
                );
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "unavailable",
                    "this key's limits could not be read in time; try again",
                )
            }
        })
    };
    // Each limit is read together with the highest row id at that moment, in one statement and so in one
    // snapshot, which is how the holds know which requests that reading already counted.
    let read_at = Instant::now();
    let reading = |limit: i64, row: sqlx::sqlite::SqliteRow| Reading {
        limit,
        used: row.get::<i64, _>("used"),
        seen: row.get::<i64, _>("seen"),
    };
    let rpm = match key.rpm_limit {
        Some(limit) => match sqlx::query(
            "SELECT COUNT(*) AS used, (SELECT COALESCE(MAX(id), 0) FROM usage) AS seen \
             FROM usage WHERE api_key_id = ? \
             AND strftime('%Y-%m-%d %H:%M:%S', created_at) >= datetime('now', '-1 minute')",
        )
        .bind(key.id)
        .fetch_one(&st.pool)
        .await
        {
            Ok(row) => Some(reading(limit, row)),
            Err(err) => return Err(Box::new(unreadable(err))),
        },
        None => None,
    };
    if let Some(rpm) = rpm
        && rpm.used >= rpm.limit
    {
        return Err(refuse(Refusal::RateLimit { limit: rpm.limit }));
    }
    let key_cap = match key.token_cap {
        Some(cap) => match sqlx::query(
            "SELECT COALESCE(SUM(total_tokens), 0) AS used, \
             (SELECT COALESCE(MAX(id), 0) FROM usage) AS seen FROM usage WHERE api_key_id = ?",
        )
        .bind(key.id)
        .fetch_one(&st.pool)
        .await
        {
            Ok(row) => Some(reading(cap, row)),
            Err(err) => return Err(Box::new(unreadable(err))),
        },
        None => None,
    };
    if let Some(cap) = key_cap
        && cap.used >= cap.limit
    {
        return Err(refuse(Refusal::KeyCap {
            cap: cap.limit,
            in_flight: 0,
        }));
    }
    let day_cap = match account.get::<Option<i64>, _>("daily_token_cap") {
        Some(cap) => match sqlx::query(
            "SELECT COALESCE(SUM(total_tokens), 0) AS used, \
             (SELECT COALESCE(MAX(id), 0) FROM usage) AS seen FROM usage \
             WHERE user_id = ? AND strftime('%Y-%m-%d', created_at) = date('now')",
        )
        .bind(key.user_id)
        .fetch_one(&st.pool)
        .await
        {
            Ok(row) => Some(reading(cap, row)),
            Err(err) => return Err(Box::new(unreadable(err))),
        },
        None => None,
    };
    if let Some(cap) = day_cap
        && cap.used >= cap.limit
    {
        return Err(refuse(Refusal::DayCap {
            cap: cap.limit,
            in_flight: 0,
        }));
    }
    let ask = Ask {
        key_id: key.id,
        user_id: key.user_id,
        prompt_tokens,
        read_at,
        rpm,
        key_cap,
        day_cap,
    };
    st.reservations.reserve(ask).map_err(refuse)
}

// A cap reached with requests still running counts their input as the gateway estimated it, so the answer
// says how much of what it counted is that estimate rather than a number an upstream reported.
fn in_flight_note(in_flight: i64) -> String {
    if in_flight == 0 {
        String::new()
    } else {
        format!(", counting an estimated {in_flight} input tokens of requests still in flight")
    }
}

// A generation is sent with the key's own model first, then the connection's default, and the model the
// client asks for only when neither is set. An embedding is not: its vectors only mean something next to the
// ones the same model made, and a key's or a connection's model is a model that generates, so the one the
// client names is sent, and the other two fill in only when it names none.
fn resolve_model(
    endpoint: Endpoint,
    key_model: Option<&str>,
    conn: &Connection,
    requested: Option<&str>,
) -> Option<String> {
    if endpoint == Endpoint::Embeddings
        && let Some(requested) = requested
    {
        return Some(requested.to_string());
    }
    key_model
        .map(String::from)
        .or_else(|| conn.default_model.clone())
        .or_else(|| requested.map(String::from))
}

// The OpenAI-compatible endpoints the gateway forwards. They share everything but where a request's input is,
// where an answer's text is, and whether they stream; the rest of the path is the same for all three.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    Chat,
    Completions,
    Embeddings,
}

impl Endpoint {
    fn path(self) -> &'static str {
        match self {
            Endpoint::Chat => "chat/completions",
            Endpoint::Completions => "completions",
            Endpoint::Embeddings => "embeddings",
        }
    }

    // The estimate of what was sent in, used until an upstream reports its own count. A completion's input
    // is its prompt and, for an editor filling in the middle of a file, the text after the cursor as well. Some
    // servers take an embedding's input as chat messages, so those are read when there is no `input`.
    fn prompt_tokens(self, body: &Value) -> i64 {
        let field = |name: &str| body.get(name).unwrap_or(&Value::Null);
        match self {
            Endpoint::Chat => tokens::prompt_estimate(field("messages")),
            Endpoint::Completions => {
                tokens::input_estimate(field("prompt")) + tokens::input_estimate(field("suffix"))
            }
            Endpoint::Embeddings => match body.get("input") {
                Some(input) => tokens::input_estimate(input),
                None => tokens::prompt_estimate(field("messages")),
            },
        }
    }

    // An embedding is not a stream, whatever a request asks for, so nothing about streaming is added to it.
    fn wants_stream(self, body: &Value) -> bool {
        self != Endpoint::Embeddings && body.get("stream").and_then(Value::as_bool).unwrap_or(false)
    }

    // A single answer, read for what it reports and what it generated. An embedding's answer is mostly its
    // vectors, which can be megabytes of numbers nothing here reads, so only the two fields the report comes
    // from are kept and the rest is skipped without being built into a value.
    fn parse_answer(self, bytes: &[u8]) -> Option<Value> {
        if self == Endpoint::Embeddings {
            #[derive(serde::Deserialize)]
            struct Reported {
                usage: Option<Value>,
                timings: Option<Value>,
            }
            let reported = serde_json::from_slice::<Reported>(bytes).ok()?;
            return Some(json!({ "usage": reported.usage, "timings": reported.timings }));
        }
        serde_json::from_str::<Value>(&String::from_utf8_lossy(bytes)).ok()
    }

    // The text a single answer generated, for the estimate when the upstream reports nothing. A chat answer
    // is a message, a completion is text, and an embedding generates no text at all.
    fn answer_text(self, parsed: Option<&Value>) -> String {
        let choices = parsed
            .and_then(|value| value.get("choices").and_then(Value::as_array))
            .into_iter()
            .flatten();
        let texts: Vec<String> = match self {
            Endpoint::Chat => choices
                .filter_map(|choice| choice.get("message").map(text_of))
                .collect(),
            Endpoint::Completions => choices
                .filter_map(|choice| choice.get("text").and_then(Value::as_str))
                .map(str::to_string)
                .collect(),
            Endpoint::Embeddings => Vec::new(),
        };
        texts
            .into_iter()
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn build_payload(body: &Value, model: Option<&str>, wants_stream: bool) -> Value {
    let mut payload = body.clone();
    // When nothing names a model the body keeps what the client sent, so the upstream answers with its own
    // default rather than with a name nobody chose.
    if let Some(model) = model {
        payload["model"] = Value::String(model.to_string());
    }
    // Upstreams that support stream_options report real usage at the end of the
    // stream; without it the gateway has to estimate completion tokens.
    if wants_stream && payload.get("stream_options").is_none() {
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
        // A chat stream sends a delta; a completions stream sends the text itself.
        let text = match choice.get("delta") {
            Some(delta) => text_of(delta),
            None => choice
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        };
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

// The row is written on a task of its own, which holds the request's hold and its slots until the row is in. A
// client that leaves drops the handler and whatever the handler was waiting on: dropped in the middle of the
// write, the row could land without its hold learning of it, which would count the request twice for a minute,
// and the slots would come back before the row, which is what a stop waits for. A row that could not be written
// leaves the hold unsettled, so a request that reached a connection keeps its place in the minute.
async fn record(
    st: &State,
    row: UsageRow,
    mut reservation: Reservation,
    slots: impl Send + 'static,
) {
    let st = st.clone();
    let write = tokio::spawn(async move {
        st.metrics.recorded(&row);
        if let Err(err) = usage::record(&st, &row, |id| reservation.settle(id)).await {
            st.metrics.usage_write_failure();
            tracing::error!("usage record failed: {err}");
        }
        drop(reservation);
        drop(slots);
    });
    let _ = write.await;
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
    reservation: Reservation,
}

fn stream_body(
    mut ctx: StreamContext,
    upstream: reqwest::Response,
    started: Instant,
    prompt_tokens: i64,
) -> impl futures_util::Stream<Item = Result<Bytes, BoxError>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, BoxError>>(64);
    // Why the stream was ended, when it was ended rather than finished. It is not sent down the channel: a
    // client that has stopped reading leaves the channel full, and waiting to hand it the error would keep the
    // row from being written. It is given to the client after whatever is still queued instead, when the
    // channel closes, so a client that is reading sees it in the same place it always did.
    let ending: Arc<OnceLock<String>> = Arc::default();
    let ended = ending.clone();
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
        // A stream still running when the grace window for stopping has passed is ended, so the process
        // can stop with its row written rather than lost. It is read first, so a stream that keeps
        // producing cannot keep the stop waiting.
        let closing = ctx.st.shutdown.reached(Phase::Closing);
        tokio::pin!(closing);
        // As with a stream that went quiet, the client already has a 200, so the stream ends as an error and
        // the client can tell the answer was cut.
        let cut_short = |ctx: &StreamContext| {
            tracing::warn!(
                "the gateway is stopping and the stream was ended: {}",
                ctx.model
            );
            let _ = ended.set(String::from("the gateway is shutting down"));
        };
        loop {
            let next = tokio::select! {
                biased;
                _ = &mut closing => {
                    cut_short(&ctx);
                    break;
                }
                next = tokio::time::timeout(stall, incoming.next()) => next,
            };
            let chunk = match next {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(_) => {
                    ctx.st.metrics.stream_stall();
                    tracing::warn!(
                        "upstream went quiet for {stall:?} and the stream was closed: {}",
                        ctx.model
                    );
                    // The client has already been sent a 200, so there is no status left to change: the stream
                    // ends as an error rather than as a completed answer, which is what lets a client tell an
                    // answer that was cut from one that finished.
                    let _ = ended.set(String::from("the upstream went quiet"));
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
                    // A client that has stopped reading without leaving fills the channel, and the send waits
                    // for it; the stop does not, so a stream held up that way is still ended and recorded.
                    let sent = tokio::select! {
                        biased;
                        _ = &mut closing => {
                            cut_short(&ctx);
                            break;
                        }
                        sent = tx.send(Ok(bytes)) => sent,
                    };
                    if sent.is_err() {
                        // The client disconnected mid-stream. The row is still written, with whatever
                        // the upstream had reported up to there; the final usage chunk, which is where a
                        // cache hit and the timings appear, is not.
                        break;
                    }
                }
                Err(err) => {
                    let err = err.without_url();
                    upstream_failed(&ctx.st, &ctx.conn, &err);
                    let _ = ended.set(err.to_string());
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
        // row's event, so a page goes from one to the other without a moment showing neither. The row is
        // counted for `/metrics` here as well, because it does not go through `record`.
        ctx.st.metrics.recorded(&row);
        let event = match usage::write(&ctx.st, &row, |id| ctx.reservation.settle(id)).await {
            Ok(event) => Some(event),
            Err(err) => {
                ctx.st.metrics.usage_write_failure();
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
    stream::unfold(Some(rx), move |rx| {
        let ending = ending.clone();
        async move {
            let mut rx = rx?;
            match rx.recv().await {
                Some(item) => Some((item, Some(rx))),
                None => ending
                    .get()
                    .map(|reason| (Err(BoxError::from(reason.clone())), None)),
            }
        }
    })
}

pub async fn chat_completions(
    AxumState(st): AxumState<State>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    forward(st, headers, body, Endpoint::Chat).await
}

pub async fn completions(
    AxumState(st): AxumState<State>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    forward(st, headers, body, Endpoint::Completions).await
}

pub async fn embeddings(
    AxumState(st): AxumState<State>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    forward(st, headers, body, Endpoint::Embeddings).await
}

// A path under /v1 that the gateway does not serve is answered the way an OpenAI-compatible server answers
// one, as JSON a client can read, rather than as the empty answer of the dashboard's file server.
pub async fn not_found(uri: axum::http::Uri) -> Response {
    error(
        StatusCode::NOT_FOUND,
        "not_found",
        format!(
            "{} is not served here; the gateway serves /v1/chat/completions, /v1/completions, \
             /v1/embeddings and /v1/models",
            uri.path()
        ),
    )
}

// A served path asked for with a method it does not take.
pub async fn method_not_allowed(uri: axum::http::Uri) -> Response {
    error(
        StatusCode::METHOD_NOT_ALLOWED,
        "invalid_request",
        format!("{} does not take this method", uri.path()),
    )
}

async fn forward(st: State, headers: HeaderMap, body: Value, endpoint: Endpoint) -> Response {
    let started = Instant::now();
    if st.shutdown.phase() != Phase::Running {
        return shutting_down();
    }
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
    let prompt_tokens = endpoint.prompt_tokens(&body);
    // Dropped without being settled, as on every early answer below, the hold is given back.
    let mut reservation = match enforce_limits(&st, &key, prompt_tokens).await {
        Ok(reservation) => reservation,
        Err(blocked) => return *blocked,
    };

    let candidates = registry::candidates(&st, key.connection_id).await;
    if candidates.is_empty() {
        return error(
            StatusCode::BAD_GATEWAY,
            "no_upstream",
            "no enabled connection is available for this key",
        );
    }
    let wants_stream = endpoint.wants_stream(&body);
    let requested = body.get("model").and_then(Value::as_str);
    // The request is admitted as a whole, before anything is asked of a connection, and the slots are held for
    // as long as it runs. Nothing is queued here: a connection that is full is skipped, so one slow
    // connection cannot hold unrelated requests behind it. Admitting once, at the start, is also what keeps a
    // request that has already reached a connection answering as that connection failed: once something has
    // crossed the gate it is recorded, so it cannot be turned into an overload on the next attempt.
    let Some(global) = st.limits.try_global() else {
        // A gateway that is full while it stops says it is stopping, which is the answer that lasts.
        if st.shutdown.phase() != Phase::Running {
            return shutting_down();
        }
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded",
            format!(
                "the gateway is at its limit of {} requests at once",
                st.limits.global_limit()
            ),
        );
    };
    // The check at the top can be passed just before the process starts to stop, by a request that then waits
    // on the database for its key and its limits. Admitted after the drain has found every slot back, nothing
    // would wait for it or end it, so the phase is read again once the request holds its slot.
    if st.shutdown.phase() != Phase::Running {
        return shutting_down();
    }
    let mut last_error = String::from("no connection responded");
    // Connections that were already at their limit when this request arrived. They are remembered so that a
    // request which never reached an upstream can be answered as an overload rather than as a failure of an
    // upstream that was never asked.
    let mut full: Vec<String> = Vec::new();
    let mut attempted = false;
    // The connection a request was waiting on when the gateway ended it because it was stopping, and the model
    // it was sent there with.
    let mut cut: Option<(i64, String)> = None;

    // The connections come in the order they are tried: the key's own first, then priority, with the ones
    // marked down after the rest (`registry::candidates`). Each is tried once.
    for conn in &candidates {
        let Ok(slot) = st
            .limits
            .for_connection(conn.id, conn.max_concurrent)
            .try_acquire_owned()
        else {
            full.push(conn.name.clone());
            continue;
        };
        let model = resolve_model(endpoint, key.model.as_deref(), conn, requested);
        let url = registry::endpoint(&conn.base_url, endpoint.path());
        let payload = build_payload(&body, model.as_deref(), wants_stream);
        // The row and the stream in flight name the model the gateway sent, and say unknown when it sent none.
        // The upstream's answer says which model it used, but that is not read here.
        let model = model.unwrap_or_else(|| String::from("unknown"));
        attempted = true;
        // From here the upstream is asked, so the request keeps its place in the minute even if its client
        // leaves before a row is written.
        reservation.sent();
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
        let sending = async {
            if wants_stream {
                tokio::time::timeout(st.limits.stream_stall(), request.send())
                    .await
                    .map_err(|_| BoxError::from("nothing arrived within the stall window"))
                    .and_then(|result| result.map_err(|err| BoxError::from(err.without_url())))
            } else {
                request
                    .send()
                    .await
                    .map_err(|err| BoxError::from(err.without_url()))
            }
        };
        // A request still waiting on its answer when the grace window for stopping has passed is ended, so
        // the process can stop with its row written rather than lost.
        let sent = tokio::select! {
            biased;
            sent = sending => sent,
            _ = st.shutdown.reached(Phase::Closing) => {
                cut = Some((conn.id, model.clone()));
                break;
            }
        };
        let upstream = match sent {
            Ok(upstream) => upstream,
            Err(err) => {
                upstream_failed(&st, conn, &*err);
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
            // nginx holds a response back until its buffer fills unless it is told not to, which would turn a
            // stream into one late answer; this header tells it so for this response alone.
            out.headers_mut()
                .insert("x-accel-buffering", HeaderValue::from_static("no"));
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
                reservation,
            };
            *out.body_mut() = Body::from_stream(stream_body(ctx, upstream, started, prompt_tokens));
            return out;
        }

        let read = tokio::select! {
            biased;
            read = upstream.bytes() => read,
            _ = st.shutdown.reached(Phase::Closing) => {
                cut = Some((conn.id, model.clone()));
                break;
            }
        };
        let bytes = match read {
            Ok(bytes) => bytes,
            Err(err) => {
                let err = err.without_url();
                upstream_failed(&st, conn, &err);
                last_error = format!("{}: {err}", conn.name);
                continue;
            }
        };
        let parsed = endpoint.parse_answer(&bytes);
        let mut report = Report::default();
        if let Some(value) = parsed.as_ref() {
            read_report(value, &mut report);
        }
        let completion_text = endpoint.answer_text(parsed.as_ref());
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
            UsageRow {
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
            reservation,
            (global, slot),
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
        UsageRow {
            api_key_id: key.id,
            user_id: key.user_id,
            // The connection that was working on the request when the gateway ended it, with the model it was
            // sent there with, and none when no connection answered.
            connection_id: cut.as_ref().map(|(id, _)| *id),
            model: match &cut {
                Some((_, model)) => Some(model.clone()),
                None => key.model.clone().or_else(|| requested.map(String::from)),
            },
            prompt_tokens,
            completion_tokens: 0,
            total_tokens: prompt_tokens,
            // No answer came back, so there is nothing an engine reported about the request.
            cached_tokens: None,
            prompt_ms: None,
            generation_ms: None,
            latency_ms: started.elapsed().as_millis() as i64,
            status: if cut.is_some() { 503 } else { 502 },
            streamed: wants_stream,
            estimated: true,
            created_at: crate::clock::now(),
            stream: None,
        },
        reservation,
        global,
    )
    .await;
    // Ended by the gateway because it was stopping, not by a connection that failed.
    if cut.is_some() {
        return shutting_down();
    }
    // A connection that was full was passed over for one that then failed, which can be one marked down,
    // tried last. The answer is what that one did, and the full ones are named beside it, so the client is
    // not told only half of why nothing answered.
    if !full.is_empty() {
        last_error = format!(
            "{last_error}; at their limit of requests at once: {}",
            full.join(", ")
        );
    }
    error(StatusCode::BAD_GATEWAY, "upstream_unavailable", last_error)
}

pub async fn list_models(AxumState(st): AxumState<State>, headers: HeaderMap) -> Response {
    if st.shutdown.phase() != Phase::Running {
        return shutting_down();
    }
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
                    let err = err.without_url();
                    upstream_failed(&st, &conn, &err);
                    return error(
                        StatusCode::BAD_GATEWAY,
                        "upstream_unavailable",
                        format!("{}: {err}", conn.name),
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
        Err(err) => {
            let err = err.without_url();
            upstream_failed(&st, &conn, &err);
            error(
                StatusCode::BAD_GATEWAY,
                "upstream_unavailable",
                format!("{}: {err}", conn.name),
            )
        }
    }
}
