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

pub async fn list(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    // An admin sees every key; everyone else sees their own.
    let sql = if caller.role == "admin" {
        "SELECT k.id, k.name, k.key, k.model, k.connection_id, k.enabled, k.rpm_limit, k.token_cap, \
                k.created_at, c.name AS connection, u.username AS owner, \
                COUNT(r.id) AS requests, COALESCE(SUM(r.total_tokens), 0) AS tokens \
         FROM api_keys k LEFT JOIN usage r ON r.api_key_id = k.id \
         LEFT JOIN connections c ON c.id = k.connection_id \
         JOIN users u ON u.id = k.user_id \
         GROUP BY k.id ORDER BY tokens DESC"
    } else {
        "SELECT k.id, k.name, k.key, k.model, k.connection_id, k.enabled, k.rpm_limit, k.token_cap, \
                k.created_at, c.name AS connection, u.username AS owner, \
                COUNT(r.id) AS requests, COALESCE(SUM(r.total_tokens), 0) AS tokens \
         FROM api_keys k LEFT JOIN usage r ON r.api_key_id = k.id \
         LEFT JOIN connections c ON c.id = k.connection_id \
         JOIN users u ON u.id = k.user_id \
         WHERE k.user_id = ? \
         GROUP BY k.id ORDER BY tokens DESC"
    };
    let query = sqlx::query(sql);
    let query = if caller.role == "admin" { query } else { query.bind(caller.id) };
    let rows = match query.fetch_all(&st.pool).await {
        Ok(rows) => rows,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let keys: Vec<serde_json::Value> = rows
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
        .collect();
    Json(json!({ "keys": keys })).into_response()
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
