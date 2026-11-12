use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;

use crate::api::error;
use crate::auth::Caller;
use crate::state::State;

// An admin can make an account without opening self-registration: the username and the limit are
// set here, and the password is chosen by the person when they sign in for the first time. Such an
// account is not waiting for approval, because the admin is the one who approved it by making it.
#[derive(Deserialize)]
pub struct CreateAccount {
    pub username: String,
    pub daily_token_cap: Option<i64>,
}

pub async fn create(AxumState(st): AxumState<State>, caller: Caller, Json(body): Json<CreateAccount>) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can make an account");
    }
    let username = body.username.trim().to_string();
    if username.is_empty() || username.chars().any(char::is_whitespace) {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "username is required and cannot contain spaces");
    }
    if body.daily_token_cap.is_some_and(|cap| cap < 1) {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "the daily limit must be at least 1, or left unset");
    }

    let row = match sqlx::query(
        "INSERT INTO users (username, password_hash, role, daily_token_cap, created_at) VALUES (?, NULL, 'user', ?, ?) RETURNING id",
    )
    .bind(&username)
    .bind(body.daily_token_cap)
    .bind(crate::clock::now())
    .fetch_one(&st.pool)
    .await
    {
        Ok(row) => row,
        // A username that is already taken answers as a database error, so the response says which
        // one it was rather than passing the driver text through.
        Err(_) => return error(StatusCode::CONFLICT, "invalid_request", format!("the username {username} is already taken")),
    };
    let id = row.get::<i64, _>("id");
    Json(json!({ "ok": true, "id": id, "username": username, "role": "user" })).into_response()
}

// One account at a time, and only the fields present in the body are applied, so a request
// that carries a new password does not clear the limit. A limit sent as null clears it.
pub async fn update(AxumState(st): AxumState<State>, Path(id): Path<i64>, caller: Caller, Json(body): Json<Value>) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can change an account");
    }
    let Some(fields) = body.as_object() else {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "expected an object of fields to change");
    };
    if fields.is_empty() {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "nothing to change was given");
    }
    if !(exists(&st, id).await) {
        return error(StatusCode::NOT_FOUND, "not_found", "no account with that id");
    }

    let mut changed: Vec<String> = Vec::new();
    for (field, value) in fields {
        match field.as_str() {
            // Assigning directly rather than COALESCE so a null clears the limit.
            "daily_token_cap" => {
                if !value.is_null() && !value.is_number() {
                    return error(StatusCode::BAD_REQUEST, "invalid_request", "the daily limit must be a number, or null to clear it");
                }
                let cap = value.as_i64();
                if cap.is_some_and(|cap| cap < 1) {
                    return error(StatusCode::BAD_REQUEST, "invalid_request", "the daily limit must be at least 1, or null to clear it");
                }
                match sqlx::query("UPDATE users SET daily_token_cap = ? WHERE id = ?")
                    .bind(cap)
                    .bind(id)
                    .execute(&st.pool)
                    .await
                {
                    Ok(_) => changed.push(field.clone()),
                    Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
                }
            }
            "username" => {
                let Some(username) = value.as_str().map(str::trim) else {
                    return error(StatusCode::BAD_REQUEST, "invalid_request", "username must be text");
                };
                if username.is_empty() || username.chars().any(char::is_whitespace) {
                    return error(StatusCode::BAD_REQUEST, "invalid_request", "username is required and cannot contain spaces");
                }
                // Checked up front because the unique index answers as a database error, and the
                // caller needs to be told which username is taken.
                let taken = match sqlx::query("SELECT COUNT(*) AS n FROM users WHERE username = ? AND id != ?")
                    .bind(username)
                    .bind(id)
                    .fetch_one(&st.pool)
                    .await
                {
                    Ok(row) => row.get::<i64, _>("n") > 0,
                    Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
                };
                if taken {
                    return error(StatusCode::CONFLICT, "conflict", format!("the username {username} is already taken"));
                }
                match sqlx::query("UPDATE users SET username = ? WHERE id = ?").bind(username).bind(id).execute(&st.pool).await {
                    Ok(_) => changed.push(field.clone()),
                    Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
                }
            }
            "password" => {
                let Some(raw) = value.as_str() else {
                    return error(StatusCode::BAD_REQUEST, "invalid_request", "password must be text");
                };
                if raw.len() < 8 {
                    return error(StatusCode::BAD_REQUEST, "invalid_request", "password must be at least 8 characters");
                }
                let hash = match bcrypt::hash(raw, 10) {
                    Ok(hash) => hash,
                    Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
                };
                match sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
                    .bind(&hash)
                    .bind(id)
                    .execute(&st.pool)
                    .await
                {
                    Ok(_) => changed.push(field.clone()),
                    Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
                }
            }
            "enabled" => {
                let Some(enabled) = value.as_bool() else {
                    return error(StatusCode::BAD_REQUEST, "invalid_request", "enabled must be true or false");
                };
                match sqlx::query("UPDATE users SET enabled = ? WHERE id = ?")
                    .bind(enabled as i64)
                    .bind(id)
                    .execute(&st.pool)
                    .await
                {
                    Ok(_) => {
                        // Disabling signs the account out, so its sessions go with it.
                        if !enabled {
                            let _ = sqlx::query("DELETE FROM sessions WHERE user_id = ?").bind(id).execute(&st.pool).await;
                        }
                        changed.push(field.clone());
                    }
                    Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
                }
            }
            "pending" => {
                let Some(pending) = value.as_bool() else {
                    return error(StatusCode::BAD_REQUEST, "invalid_request", "pending must be true or false");
                };
                match sqlx::query("UPDATE users SET pending = ? WHERE id = ?").bind(pending as i64).bind(id).execute(&st.pool).await {
                    Ok(_) => changed.push(field.clone()),
                    Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
                }
            }
            other => return error(StatusCode::BAD_REQUEST, "invalid_request", format!("unknown field '{other}'")),
        }
    }

    Json(json!({ "ok": true, "id": id, "changed": changed })).into_response()
}

pub async fn remove(AxumState(st): AxumState<State>, Path(id): Path<i64>, caller: Caller) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can delete an account");
    }
    // An admin cannot remove the account they are signed in with.
    if caller.id == id {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "you cannot delete your own account");
    }
    if !(exists(&st, id).await) {
        return error(StatusCode::NOT_FOUND, "not_found", "no account with that id");
    }
    // Keys, usage rows and sessions reference the account with ON DELETE CASCADE, so they go
    // with it, including the recorded history for this account.
    match sqlx::query("DELETE FROM users WHERE id = ?").bind(id).execute(&st.pool).await {
        Ok(_) => Json(json!({ "ok": true, "id": id })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}

async fn exists(st: &State, id: i64) -> bool {
    sqlx::query("SELECT COUNT(*) AS n FROM users WHERE id = ?")
        .bind(id)
        .fetch_one(&st.pool)
        .await
        .map(|row| row.get::<i64, _>("n") > 0)
        .unwrap_or(false)
}
