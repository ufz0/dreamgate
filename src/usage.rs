use serde::Serialize;
use sqlx::Row;

use crate::live::LiveEvent;
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
    pub latency_ms: i64,
    pub status: i64,
    pub streamed: bool,
    pub estimated: bool,
    pub created_at: String,
}

pub async fn record(st: &State, row: &UsageRow) -> Result<(), sqlx::Error> {
    let id = sqlx::query(
        "INSERT INTO usage (api_key_id, user_id, connection_id, model, prompt_tokens, \
         completion_tokens, total_tokens, latency_ms, status, streamed, estimated, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(row.api_key_id)
    .bind(row.user_id)
    .bind(row.connection_id)
    .bind(&row.model)
    .bind(row.prompt_tokens)
    .bind(row.completion_tokens)
    .bind(row.total_tokens)
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

    let user_name = sqlx::query("SELECT name FROM users WHERE id = ?")
        .bind(row.user_id)
        .fetch_optional(&st.pool)
        .await
        .ok()
        .flatten()
        .and_then(|r| r.try_get::<String, _>("name").ok());

    let event = LiveEvent {
        id,
        user_id: row.user_id,
        key: key_name.unwrap_or_else(|| "unknown key".into()),
        user: user_name.unwrap_or_else(|| "unknown user".into()),
        model: row.model.clone().unwrap_or_default(),
        prompt_tokens: row.prompt_tokens,
        completion_tokens: row.completion_tokens,
        total_tokens: row.total_tokens,
        latency_ms: row.latency_ms,
        status: row.status,
        streamed: row.streamed,
        created_at: row.created_at.clone(),
    };
    let _ = st.hub.send(event);
    Ok(())
}
