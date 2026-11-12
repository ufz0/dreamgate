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
const APPROVAL_KEY: &str = "require_approval";

// The first account is always allowed, which is how an instance gets its admin. After that,
// self-registration stays closed until an admin opens it.
pub async fn can_register(st: &State) -> bool {
    let accounts = count_accounts(st).await;
    if accounts == 0 {
        return true;
    }
    is_open(st).await
}

// New accounts wait for an admin until the setting is turned off. The first account is never
// held back, because there is no one to approve it.
pub async fn requires_approval(st: &State) -> bool {
    value(st, APPROVAL_KEY, "1").await
}

async fn count_accounts(st: &State) -> i64 {
    sqlx::query("SELECT COUNT(*) AS n FROM users")
        .fetch_one(&st.pool)
        .await
        .map(|row| row.get::<i64, _>("n"))
        .unwrap_or(0)
}

async fn is_open(st: &State) -> bool {
    value(st, KEY, "0").await
}

async fn value(st: &State, key: &str, default: &str) -> bool {
    let row = sqlx::query("SELECT value FROM settings WHERE key = ?")
        .bind(key)
        .fetch_optional(&st.pool)
        .await
        .ok()
        .flatten();
    row.map(|row| row.get::<String, _>("value")).unwrap_or(default.to_string()) == "1"
}

// Read before anyone is signed in, because the entry screen has to know whether registering
// is possible at all, and whether a new account would have to wait.
pub async fn get(AxumState(st): AxumState<State>) -> Response {
    let accounts = count_accounts(&st).await;
    let open = accounts == 0 || is_open(&st).await;
    Json(json!({
        "open_registration": open,
        "has_accounts": accounts > 0,
        "require_approval": accounts > 0 && requires_approval(&st).await,
    }))
    .into_response()
}

#[derive(Deserialize)]
pub struct Settings {
    pub open_registration: Option<bool>,
    pub require_approval: Option<bool>,
}

// Only the field present in the body is applied, so changing one setting does not rewrite the other.
pub async fn update(AxumState(st): AxumState<State>, caller: Caller, Json(body): Json<Settings>) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can change registration");
    }
    if body.open_registration.is_none() && body.require_approval.is_none() {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "nothing to change was given");
    }
    for (key, setting) in [(KEY, body.open_registration), (APPROVAL_KEY, body.require_approval)] {
        let Some(value) = setting else { continue };
        let result = sqlx::query(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(if value { "1" } else { "0" })
        .execute(&st.pool)
        .await;
        if let Err(err) = result {
            return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string());
        }
    }
    Json(json!({
        "ok": true,
        "open_registration": is_open(&st).await,
        "require_approval": requires_approval(&st).await,
    }))
    .into_response()
}
