use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::api::error;
use crate::auth::Caller;
use crate::state::State;

#[derive(Deserialize)]
pub struct CreateKey {
    pub name: String,
    pub connection_id: Option<i64>,
    pub model: Option<String>,
    pub rpm_limit: Option<i64>,
    pub token_cap: Option<i64>,
}

#[derive(Deserialize)]
pub struct UpdateKey {
    pub name: Option<String>,
    pub connection_id: Option<i64>,
    pub model: Option<String>,
    pub enabled: Option<bool>,
    pub rpm_limit: Option<i64>,
    pub token_cap: Option<i64>,
}

// One row shape for both lists. The Keys page shows the caller's own keys; an admin can ask for
// the other accounts separately, so a table never mixes two owners.
const KEY_SQL: &str = "SELECT k.id, k.name, k.key, k.model, k.connection_id, k.enabled, k.rpm_limit, k.token_cap, \
        k.created_at, c.name AS connection, u.username AS owner, \
        COUNT(r.id) AS requests, COALESCE(SUM(r.total_tokens), 0) AS tokens \
 FROM api_keys k LEFT JOIN usage r ON r.api_key_id = k.id \
 LEFT JOIN connections c ON c.id = k.connection_id \
 JOIN users u ON u.id = k.user_id {scope} GROUP BY k.id ORDER BY tokens DESC";

async fn key_rows(st: &State, scope: &str, user_id: i64) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    let sql = KEY_SQL.replace("{scope}", scope);
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(user_id).fetch_all(&st.pool).await?;
    Ok(rows
        .iter()
        .map(|row| {
            json!({
                "id": row.get::<i64, _>("id"),
                "name": row.get::<String, _>("name"),
                "key": row.get::<String, _>("key"),
                "model": row.get::<Option<String>, _>("model"),
                "connection_id": row.get::<Option<i64>, _>("connection_id"),
                "connection": row.get::<Option<String>, _>("connection"),
                "owner": row.get::<String, _>("owner"),
                "enabled": row.get::<i64, _>("enabled") != 0,
                "rpm_limit": row.get::<Option<i64>, _>("rpm_limit"),
                "token_cap": row.get::<Option<i64>, _>("token_cap"),
                "requests": row.get::<i64, _>("requests"),
                "tokens": row.get::<i64, _>("tokens"),
                "created_at": row.get::<String, _>("created_at"),
            })
        })
        .collect())
}

pub async fn list(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    match key_rows(&st, "WHERE k.user_id = ?", caller.id).await {
        Ok(keys) => Json(json!({ "keys": keys })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}

// Keys that belong to someone else, for an admin.
pub async fn other_accounts(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can see keys from other accounts");
    }
    match key_rows(&st, "WHERE k.user_id != ?", caller.id).await {
        Ok(keys) => Json(json!({ "keys": keys })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}

pub async fn create(AxumState(st): AxumState<State>, caller: Caller, Json(body): Json<CreateKey>) -> Response {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "name is required");
    }
    // A limit of 0 would refuse every request, so a limit is a positive number or absent.
    if body.rpm_limit.is_some_and(|value| value < 1) || body.token_cap.is_some_and(|value| value < 1) {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "limits must be a positive number, or left unset");
    }
    let key = format!("dreamgate_{}", Uuid::new_v4().simple());
    let result = sqlx::query(
        "INSERT INTO api_keys (user_id, name, key, connection_id, model, rpm_limit, token_cap, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(caller.id)
    .bind(&name)
    .bind(&key)
    .bind(body.connection_id)
    .bind(body.model.as_deref().map(str::trim).filter(|m| !m.is_empty()))
    .bind(body.rpm_limit)
    .bind(body.token_cap)
    .bind(crate::clock::now())
    .fetch_one(&st.pool)
    .await;
    match result {
        Ok(row) => Json(json!({ "id": row.get::<i64, _>("id"), "key": key })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}

pub async fn update(AxumState(st): AxumState<State>, Path(id): Path<i64>, caller: Caller, Json(body): Json<UpdateKey>) -> Response {
    if caller.role != "admin" {
        let owns = sqlx::query("SELECT user_id FROM api_keys WHERE id = ?")
            .bind(id)
            .fetch_optional(&st.pool)
            .await
            .ok()
            .flatten();
        match owns {
            Some(row) if row.get::<i64, _>("user_id") == caller.id => {}
            Some(_) => return error(StatusCode::FORBIDDEN, "forbidden", "you can only change your own key"),
            None => return error(StatusCode::NOT_FOUND, "not_found", "key not found"),
        }
    }
    let result = sqlx::query(
        "UPDATE api_keys SET name = COALESCE(?, name), connection_id = COALESCE(?, connection_id), \
                model = COALESCE(?, model), enabled = COALESCE(?, enabled), \
                rpm_limit = COALESCE(?, rpm_limit), token_cap = COALESCE(?, token_cap) \
         WHERE id = ?",
    )
    .bind(body.name)
    .bind(body.connection_id)
    .bind(body.model)
    .bind(body.enabled.map(|v| v as i64))
    .bind(body.rpm_limit)
    .bind(body.token_cap)
    .bind(id)
    .execute(&st.pool)
    .await;
    match result {
        Ok(_) => Json(json!({ "ok": true })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}

pub async fn delete(AxumState(st): AxumState<State>, Path(id): Path<i64>, caller: Caller) -> Response {
    if caller.role != "admin" {
        let owns = sqlx::query("SELECT user_id FROM api_keys WHERE id = ?")
            .bind(id)
            .fetch_optional(&st.pool)
            .await
            .ok()
            .flatten();
        match owns {
            Some(row) if row.get::<i64, _>("user_id") == caller.id => {}
            Some(_) => return error(StatusCode::FORBIDDEN, "forbidden", "you can only delete your own key"),
            None => return error(StatusCode::NOT_FOUND, "not_found", "key not found"),
        }
    }
    let result = sqlx::query("DELETE FROM api_keys WHERE id = ?").bind(id).execute(&st.pool).await;
    match result {
        Ok(_) => Json(json!({ "ok": true })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}
