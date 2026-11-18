use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

use crate::api::{error, server_error};
use crate::auth::Caller;
use crate::state::State;

// How long a setup code can be used. Long enough to reach the person by whatever way the admin passes it on,
// short enough that one left in a chat history stops working on its own.
pub const SETUP_CODE_DAYS: i64 = 7;

// A setup code: twenty hex characters, in groups of four so it can be read out and typed. It is drawn from a
// v4 uuid with the version and variant digits left out, so all eighty bits are random; that is far more than
// a sign-in can guess at the few attempts a minute an account is allowed.
fn new_setup_code() -> String {
    let random: Vec<char> = Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .enumerate()
        .filter(|(at, _)| *at != 12 && *at != 16)
        .map(|(_, digit)| digit)
        .take(20)
        .collect();
    random
        .chunks(4)
        .map(|group| group.iter().collect::<String>())
        .collect::<Vec<_>>()
        .join("-")
}

// What is compared is the code without the dashes, spaces or case a person may type it with.
pub fn plain_setup_code(code: &str) -> String {
    code.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

// A new code and what is stored for it: only its bcrypt hash, as for a password, so a copy of the database does
// not hand out the accounts that are waiting for one, and the time it stops working.
async fn issue_setup_code() -> Result<(String, String, String), String> {
    let code = new_setup_code();
    let hash = crate::auth::hash_password(&plain_setup_code(&code)).await?;
    let expires_at = crate::clock::in_days(SETUP_CODE_DAYS);
    Ok((code, hash, expires_at))
}

// An admin can make an account without opening self-registration: the username and the limit are set here,
// and the answer carries a one-time setup code, shown once, with which the person sets their password at their
// first sign-in. Without the code nobody can, so a username that is guessed cannot be claimed. Such an account
// is not waiting for approval, because the admin is the one who approved it by making it.
#[derive(Deserialize)]
pub struct CreateAccount {
    pub username: String,
    pub daily_token_cap: Option<i64>,
}

pub async fn create(
    AxumState(st): AxumState<State>,
    caller: Caller,
    Json(body): Json<CreateAccount>,
) -> Response {
    if caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "only an admin can make an account",
        );
    }
    let username = body.username.trim().to_string();
    if username.is_empty() || username.chars().any(char::is_whitespace) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "username is required and cannot contain spaces",
        );
    }
    if body.daily_token_cap.is_some_and(|cap| cap < 1) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "the daily limit must be at least 1, or left unset",
        );
    }

    let (code, hash, expires_at) = match issue_setup_code().await {
        Ok(issued) => issued,
        Err(err) => return server_error(err),
    };
    let row = match sqlx::query(
        "INSERT INTO users (username, password_hash, role, daily_token_cap, created_at, setup_code_hash, \
         setup_code_expires_at) VALUES (?, NULL, 'user', ?, ?, ?, ?) RETURNING id",
    )
    .bind(&username)
    .bind(body.daily_token_cap)
    .bind(crate::clock::now())
    .bind(&hash)
    .bind(&expires_at)
    .fetch_one(&st.pool)
    .await
    {
        Ok(row) => row,
        // A username that is already taken answers as a database error, so the response says which
        // one it was rather than passing the driver text through. The username is the only unique
        // column on users, so a unique violation can only mean that; a failure for any other reason
        // is not dressed up as a taken name.
        Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
            return error(
                StatusCode::CONFLICT,
                "invalid_request",
                format!("the username {username} is already taken"),
            );
        }
        Err(err) => return server_error(err),
    };
    let id = row.get::<i64, _>("id");
    Json(json!({
        "ok": true,
        "id": id,
        "username": username,
        "role": "user",
        "setup_code": code,
        "setup_code_expires_at": expires_at,
    }))
    .into_response()
}

// A new setup code for an account, replacing any it had. For an account with no password yet it is how the
// person sets one; for one with a password it is a reset that the person carries out, so the admin never
// learns the new password, and the old one keeps working until the code is used.
pub async fn setup_code(
    AxumState(st): AxumState<State>,
    Path(id): Path<i64>,
    caller: Caller,
) -> Response {
    if caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "only an admin can give an account a setup code",
        );
    }
    let (code, hash, expires_at) = match issue_setup_code().await {
        Ok(issued) => issued,
        Err(err) => return server_error(err),
    };
    match sqlx::query(
        "UPDATE users SET setup_code_hash = ?, setup_code_expires_at = ? WHERE id = ? RETURNING username",
    )
    .bind(&hash)
    .bind(&expires_at)
    .bind(id)
    .fetch_optional(&st.pool)
    .await
    {
        Ok(Some(row)) => Json(json!({
            "id": id,
            "username": row.get::<String, _>("username"),
            "setup_code": code,
            "setup_code_expires_at": expires_at,
        }))
        .into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "not_found",
            "no account with that id",
        ),
        Err(err) => server_error(err),
    }
}

// One account at a time, and only the fields present in the body are applied, so a request
// that carries a new password does not clear the limit. A limit sent as null clears it.
pub async fn update(
    AxumState(st): AxumState<State>,
    Path(id): Path<i64>,
    caller: Caller,
    Json(body): Json<Value>,
) -> Response {
    if caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "only an admin can change an account",
        );
    }
    let Some(fields) = body.as_object() else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "expected an object of fields to change",
        );
    };
    if fields.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "nothing to change was given",
        );
    }
    if !(exists(&st, id).await) {
        return error(
            StatusCode::NOT_FOUND,
            "not_found",
            "no account with that id",
        );
    }

    let mut changed: Vec<String> = Vec::new();
    for (field, value) in fields {
        match field.as_str() {
            // Assigning directly rather than COALESCE so a null clears the limit.
            "daily_token_cap" => {
                if !value.is_null() && !value.is_number() {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        "the daily limit must be a number, or null to clear it",
                    );
                }
                let cap = value.as_i64();
                if cap.is_some_and(|cap| cap < 1) {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        "the daily limit must be at least 1, or null to clear it",
                    );
                }
                match sqlx::query("UPDATE users SET daily_token_cap = ? WHERE id = ?")
                    .bind(cap)
                    .bind(id)
                    .execute(&st.pool)
                    .await
                {
                    Ok(_) => changed.push(field.clone()),
                    Err(err) => {
                        return server_error(err);
                    }
                }
            }
            "username" => {
                let Some(username) = value.as_str().map(str::trim) else {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        "username must be text",
                    );
                };
                if username.is_empty() || username.chars().any(char::is_whitespace) {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        "username is required and cannot contain spaces",
                    );
                }
                // Checked up front because the unique index answers as a database error, and the
                // caller needs to be told which username is taken.
                let taken = match sqlx::query(
                    "SELECT COUNT(*) AS n FROM users WHERE username = ? AND id != ?",
                )
                .bind(username)
                .bind(id)
                .fetch_one(&st.pool)
                .await
                {
                    Ok(row) => row.get::<i64, _>("n") > 0,
                    Err(err) => {
                        return server_error(err);
                    }
                };
                if taken {
                    return error(
                        StatusCode::CONFLICT,
                        "conflict",
                        format!("the username {username} is already taken"),
                    );
                }
                match sqlx::query("UPDATE users SET username = ? WHERE id = ?")
                    .bind(username)
                    .bind(id)
                    .execute(&st.pool)
                    .await
                {
                    Ok(_) => changed.push(field.clone()),
                    // The name can still be taken between the check and the write, and that is
                    // answered the same way as when the check finds it.
                    Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
                        return error(
                            StatusCode::CONFLICT,
                            "conflict",
                            format!("the username {username} is already taken"),
                        );
                    }
                    Err(err) => {
                        return server_error(err);
                    }
                }
            }
            "password" => {
                let Some(raw) = value.as_str() else {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        "password must be text",
                    );
                };
                if raw.len() < 8 {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        "password must be at least 8 characters",
                    );
                }
                let hash = match crate::auth::hash_password(raw).await {
                    Ok(hash) => hash,
                    Err(err) => {
                        return server_error(err);
                    }
                };
                // A password the admin sets is the newest word on it, so a code given out before it can no
                // longer replace it. It also ends the account's sign-ins, as a reset with a code does, because an
                // admin sets a password for someone most often when the old one is lost or known to someone else,
                // their own included. The session the admin is using is kept, so the page they set it from stays
                // signed in; for another account it is not one of that account's sessions, so all of them end.
                let mut tx = match st.pool.begin().await {
                    Ok(tx) => tx,
                    Err(err) => return server_error(err),
                };
                if let Err(err) = sqlx::query(
                    "UPDATE users SET password_hash = ?, setup_code_hash = NULL, \
                     setup_code_expires_at = NULL WHERE id = ?",
                )
                .bind(&hash)
                .bind(id)
                .execute(&mut *tx)
                .await
                {
                    return server_error(err);
                }
                if let Err(err) =
                    sqlx::query("DELETE FROM sessions WHERE user_id = ? AND token != ?")
                        .bind(id)
                        .bind(&caller.token)
                        .execute(&mut *tx)
                        .await
                {
                    return server_error(err);
                }
                if let Err(err) = tx.commit().await {
                    return server_error(err);
                }
                changed.push(field.clone());
            }
            "enabled" => {
                let Some(enabled) = value.as_bool() else {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        "enabled must be true or false",
                    );
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
                            let _ = sqlx::query("DELETE FROM sessions WHERE user_id = ?")
                                .bind(id)
                                .execute(&st.pool)
                                .await;
                        }
                        changed.push(field.clone());
                    }
                    Err(err) => {
                        return server_error(err);
                    }
                }
            }
            "pending" => {
                let Some(pending) = value.as_bool() else {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request",
                        "pending must be true or false",
                    );
                };
                match sqlx::query("UPDATE users SET pending = ? WHERE id = ?")
                    .bind(pending as i64)
                    .bind(id)
                    .execute(&st.pool)
                    .await
                {
                    Ok(_) => changed.push(field.clone()),
                    Err(err) => {
                        return server_error(err);
                    }
                }
            }
            other => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    format!("unknown field '{other}'"),
                );
            }
        }
    }

    Json(json!({ "ok": true, "id": id, "changed": changed })).into_response()
}

pub async fn remove(
    AxumState(st): AxumState<State>,
    Path(id): Path<i64>,
    caller: Caller,
) -> Response {
    if caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "only an admin can delete an account",
        );
    }
    // An admin cannot remove the account they are signed in with.
    if caller.id == id {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "you cannot delete your own account",
        );
    }
    if !(exists(&st, id).await) {
        return error(
            StatusCode::NOT_FOUND,
            "not_found",
            "no account with that id",
        );
    }
    // Keys, usage rows and sessions reference the account with ON DELETE CASCADE, so they go
    // with it, including the recorded history for this account.
    match sqlx::query("DELETE FROM users WHERE id = ?")
        .bind(id)
        .execute(&st.pool)
        .await
    {
        Ok(_) => Json(json!({ "ok": true, "id": id })).into_response(),
        Err(err) => server_error(err),
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
