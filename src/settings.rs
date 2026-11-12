use axum::extract::{State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;

use crate::api::error;
use crate::auth::Caller;
use crate::state::State;

const KEY: &str = "open_registration";

// The first account is always allowed, which is how an instance gets its admin. After that,
// self-registration stays closed until an admin opens it.
pub async fn can_register(st: &State) -> bool {
    let accounts = count_accounts(st).await;
    if accounts == 0 {
        return true;
    }
    is_open(st).await
}

async fn count_accounts(st: &State) -> i64 {
    sqlx::query("SELECT COUNT(*) AS n FROM users")
        .fetch_one(&st.pool)
        .await
        .map(|row| row.get::<i64, _>("n"))
        .unwrap_or(0)
}

async fn is_open(st: &State) -> bool {
    let row = sqlx::query("SELECT value FROM settings WHERE key = ?")
        .bind(KEY)
        .fetch_optional(&st.pool)
        .await
        .ok()
        .flatten();
    row.map(|row| row.get::<String, _>("value") == "1").unwrap_or(false)
}

// Read before anyone is signed in, because the entry screen has to know whether registering
// is possible at all.
pub async fn get(AxumState(st): AxumState<State>) -> Response {
    let accounts = count_accounts(&st).await;
    let open = accounts == 0 || is_open(&st).await;
    Json(json!({ "open_registration": open, "has_accounts": accounts > 0 })).into_response()
}

#[derive(Deserialize)]
pub struct Settings {
    pub open_registration: bool,
}

pub async fn update(AxumState(st): AxumState<State>, caller: Caller, Json(body): Json<Settings>) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can change registration");
    }
    let value = if body.open_registration { "1" } else { "0" };
    let result = sqlx::query(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(KEY)
    .bind(value)
    .execute(&st.pool)
    .await;
    match result {
        Ok(_) => Json(json!({ "ok": true, "open_registration": body.open_registration })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}
