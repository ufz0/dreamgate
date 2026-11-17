use serde::Serialize;
use sqlx::Row;

use crate::live::{HubEvent, LiveEvent};
use crate::state::State;

#[derive(Clone, Serialize)]
pub struct UsageRow {
    pub api_key_id: i64,
    pub user_id: i64,
    pub connection_id: Option<i64>,
    pub model: Option<String>,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    // What the upstream said about the request rather than what the gateway worked out. An engine that
    // does not report a cache hit or timings leaves these unset, and they are stored as null so the
    // page can say the figure is unknown instead of showing a number that was not reported.
    pub cached_tokens: Option<i64>,
    pub prompt_ms: Option<i64>,
    pub generation_ms: Option<i64>,
    pub latency_ms: i64,
    pub status: i64,
    pub streamed: bool,
    pub estimated: bool,
    pub created_at: String,
    // The stream whose progress the page was showing, so the row can take its place there. It is not
    // stored: it only means something while the process that numbered the stream is running.
    pub stream: Option<u64>,
}

pub async fn record(st: &State, row: &UsageRow) -> Result<(), sqlx::Error> {
    let event = write(st, row).await?;
    let _ = st.hub.send(HubEvent::Usage(event));
    Ok(())
}

// Writes the row and returns the event a page is sent for it without sending it, for the stream task,
// which sends it in the same step that takes the stream off the list of streams in flight.
pub async fn write(st: &State, row: &UsageRow) -> Result<LiveEvent, sqlx::Error> {
    let id = sqlx::query(
        "INSERT INTO usage (api_key_id, user_id, connection_id, model, prompt_tokens, \
         completion_tokens, total_tokens, cached_tokens, prompt_ms, generation_ms, latency_ms, \
         status, streamed, estimated, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(row.api_key_id)
    .bind(row.user_id)
    .bind(row.connection_id)
    .bind(&row.model)
    .bind(row.prompt_tokens)
    .bind(row.completion_tokens)
    .bind(row.total_tokens)
    .bind(row.cached_tokens)
    .bind(row.prompt_ms)
    .bind(row.generation_ms)
    .bind(row.latency_ms)
    .bind(row.status)
    .bind(row.streamed as i64)
    .bind(row.estimated as i64)
    .bind(&row.created_at)
    .fetch_one(&st.pool)
    .await?
    .get::<i64, _>("id");

    let key_name = sqlx::query("SELECT name FROM api_keys WHERE id = ?")
        .bind(row.api_key_id)
        .fetch_optional(&st.pool)
        .await
        .ok()
        .flatten()
        .and_then(|r| r.try_get::<String, _>("name").ok());

    let user_name = sqlx::query("SELECT username FROM users WHERE id = ?")
        .bind(row.user_id)
        .fetch_optional(&st.pool)
        .await
        .ok()
        .flatten()
        .and_then(|r| r.try_get::<String, _>("username").ok());

    let event = LiveEvent {
        id,
        user_id: row.user_id,
        key: key_name.unwrap_or_else(|| "unknown key".into()),
        user: user_name.unwrap_or_else(|| "unknown user".into()),
        model: row.model.clone().unwrap_or_default(),
        prompt_tokens: row.prompt_tokens,
        completion_tokens: row.completion_tokens,
        total_tokens: row.total_tokens,
        cached_tokens: row.cached_tokens,
        latency_ms: row.latency_ms,
        status: row.status,
        streamed: row.streamed,
        estimated: row.estimated,
        created_at: row.created_at.clone(),
        stream: row.stream,
    };
    Ok(event)
}
