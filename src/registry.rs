use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;
use std::time::Duration;

use crate::api::error;
use crate::auth::Caller;
use crate::state::State;

#[derive(Clone)]
pub struct Connection {
    pub id: i64,
    pub name: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub default_model: Option<String>,
    pub enabled: bool,
    pub priority: i64,
    pub health: String,
    pub last_checked_at: Option<String>,
}

fn row_to_connection(row: &sqlx::sqlite::SqliteRow) -> Connection {
    Connection {
        id: row.get::<i64, _>("id"),
        name: row.get::<String, _>("name"),
        base_url: row.get::<String, _>("base_url"),
        api_key: row.get::<Option<String>, _>("api_key"),
        default_model: row.get::<Option<String>, _>("default_model"),
        enabled: row.get::<i64, _>("enabled") != 0,
        priority: row.get::<i64, _>("priority"),
        health: row.get::<String, _>("health"),
        last_checked_at: row.get::<Option<String>, _>("last_checked_at"),
    }
}

pub fn endpoint(base_url: &str, path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/v1") {
        format!("{base}/{path}")
    } else {
        format!("{base}/v1/{path}")
    }
}

pub async fn candidates(st: &State, preferred: Option<i64>) -> Vec<Connection> {
    let rows = sqlx::query(
        "SELECT id, name, base_url, api_key, default_model, enabled, priority, health, last_checked_at \
         FROM connections WHERE enabled = 1 ORDER BY priority ASC, id ASC",
    )
    .fetch_all(&st.pool)
    .await
    .unwrap_or_default();
    let mut all: Vec<Connection> = rows.iter().map(row_to_connection).collect();
    if let Some(id) = preferred {
        let position = all.iter().position(|c| c.id == id);
        if let Some(index) = position {
            let preferred_conn = all.remove(index);
            let mut ordered = vec![preferred_conn];
            ordered.extend(all);
            return ordered;
        }
    }
    all
}

pub async fn check(st: &State, connection: &Connection) -> StatusCode {
    let url = endpoint(&connection.base_url, "models");
    let result = st
        .http
        .get(&url)
        .header("Authorization", format!("Bearer {}", connection.api_key.clone().unwrap_or_default()))
        .timeout(Duration::from_secs(10))
        .send()
        .await;
    let status = match result {
        Ok(resp) => resp.status(),
        Err(_) => StatusCode::BAD_GATEWAY,
    };
    let health = if status.is_success() { "ok" } else { "down" };
    let _ = sqlx::query("UPDATE connections SET health = ?, last_checked_at = ? WHERE id = ?")
        .bind(health)
        .bind(crate::clock::now())
        .bind(connection.id)
        .execute(&st.pool)
        .await;
    status
}

#[derive(Deserialize)]
pub struct CreateConnection {
    pub name: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub default_model: Option<String>,
    pub priority: Option<i64>,
}

pub async fn list(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    let rows = sqlx::query(
        "SELECT id, name, base_url, api_key, default_model, enabled, priority, health, last_checked_at \
         FROM connections ORDER BY priority ASC, id ASC",
    )
    .fetch_all(&st.pool)
    .await
    .unwrap_or_default();
    // An upstream key is a secret, so it is returned to an admin alone. The rest of the row is
    // what any signed-in account needs for the gate readout and the model list.
    let admin = caller.role == "admin";
    let out: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let c = row_to_connection(row);
            let api_key = if admin { c.api_key.clone() } else { None };
            json!({
                "id": c.id,
                "name": c.name,
                "base_url": c.base_url,
                "api_key": api_key,
                "default_model": c.default_model,
                "enabled": c.enabled,
                "priority": c.priority,
                "health": c.health,
                "last_checked_at": c.last_checked_at,
            })
        })
        .collect();
    Json(json!({ "connections": out })).into_response()
}

pub async fn create(AxumState(st): AxumState<State>, caller: Caller, Json(body): Json<CreateConnection>) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can add a connection");
    }
    let name = body.name.trim().to_string();
    let base_url = body.base_url.trim().to_string();
    if name.is_empty() || base_url.is_empty() {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "name and base_url are required");
    }
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "base_url must start with http:// or https://");
    }
    let row = sqlx::query(
        "INSERT INTO connections (name, base_url, api_key, default_model, priority, created_at) \
         VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(&name)
    .bind(&base_url)
    .bind(&body.api_key)
    .bind(&body.default_model)
    .bind(body.priority.unwrap_or(0))
    .bind(crate::clock::now())
    .fetch_one(&st.pool)
    .await;
    match row {
        Ok(row) => Json(json!({ "id": row.get::<i64, _>("id") })).into_response(),
        Err(err) => error(StatusCode::CONFLICT, "invalid_request", err.to_string()),
    }
}

pub async fn remove(AxumState(st): AxumState<State>, Path(id): Path<i64>, caller: Caller) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can remove a connection");
    }
    let result = sqlx::query("DELETE FROM connections WHERE id = ?").bind(id).execute(&st.pool).await;
    match result {
        Ok(_) => Json(json!({ "ok": true })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}

pub async fn health_check(AxumState(st): AxumState<State>, Path(id): Path<i64>, caller: Caller) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can check a connection");
    }
    let row = sqlx::query(
        "SELECT id, name, base_url, api_key, default_model, enabled, priority, health, last_checked_at \
         FROM connections WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&st.pool)
    .await;
    let Ok(Some(row)) = row else {
        return error(StatusCode::NOT_FOUND, "not_found", "connection not found");
    };
    let connection = row_to_connection(&row);
    let status = check(&st, &connection).await;
    Json(json!({ "id": connection.id, "health": if status.is_success() { "ok" } else { "down" } })).into_response()
}

pub async fn health_checker(st: State) {
    loop {
        let rows = sqlx::query(
            "SELECT id, name, base_url, api_key, default_model, enabled, priority, health, last_checked_at \
             FROM connections WHERE enabled = 1",
        )
        .fetch_all(&st.pool)
        .await
        .unwrap_or_default();
        for row in &rows {
            let connection = row_to_connection(row);
            let _ = check(&st, &connection).await;
        }
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}
