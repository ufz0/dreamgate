use std::time::{Duration, Instant};

use axum::extract::{FromRequestParts, Request, State as AxumState};
use axum::http::header::{SET_COOKIE, HeaderValue};
use axum::http::request::Parts;
use axum::http::uri::Scheme;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::api::error;
use crate::state::State;

pub const COOKIE_NAME: &str = "dg_session";

// The cookie lasts thirty days, and a session row older than the same window is refused, so a
// token that is kept and replayed does not work forever.
const SESSION_MAX_AGE_DAYS: i64 = 30;

// Sign-in is the only part of the API that answers without a session, so attempts per account are
// bounded: an unbounded loop is both open password guessing and a bcrypt hash per call. Ten
// attempts a minute is well past what a person signing in normally does.
const ATTEMPT_WINDOW_SECS: u64 = 60;
const MAX_ATTEMPTS_PER_WINDOW: usize = 10;

#[derive(Clone)]
pub struct Caller {
    pub id: i64,
    pub username: String,
    pub role: String,
}

// The session middleware resolves the account once; handlers take Caller as an
// extractor so they never have to touch the request directly.
impl<S> FromRequestParts<S> for Caller
where
    S: Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Caller>()
            .cloned()
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid_session", "not signed in"))
    }
}

pub fn cookie_value(req: &Request, name: &str) -> Option<String> {
    let header = req.headers().get("cookie")?.to_str().ok()?;
    for part in header.split(';') {
        let (key, value) = part.trim().split_once('=')?;
        if key == name {
            return Some(value.to_string());
        }
    }
    None
}

pub async fn require_session(AxumState(st): AxumState<State>, mut req: Request, next: Next) -> Response {
    let Some(token) = cookie_value(&req, COOKIE_NAME) else {
        return error(StatusCode::UNAUTHORIZED, "invalid_session", "not signed in");
    };
    // The window is read in the lookup rather than trusted from the cookie, because a client can
    // hold on to a token and keep sending it.
    let sql = format!(
        "SELECT u.id, u.username, u.role FROM sessions s JOIN users u ON u.id = s.user_id \
         WHERE s.token = ? AND strftime('%Y-%m-%d %H:%M:%S', s.created_at) >= datetime('now', '-{SESSION_MAX_AGE_DAYS} days')",
    );
    let row = match sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(&token).fetch_optional(&st.pool).await {
        Ok(row) => row,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let Some(row) = row else {
        return error(StatusCode::UNAUTHORIZED, "invalid_session", "session expired");
    };
    req.extensions_mut().insert(Caller {
        id: row.get::<i64, _>("id"),
        username: row.get::<String, _>("username"),
        role: row.get::<String, _>("role"),
    });
    next.run(req).await
}

// The count is kept in the process rather than in the database: it only has to bound a loop, not
// survive a restart.
fn attempt_allowed(st: &State, username: &str) -> bool {
    let mut windows = st.attempts.lock().expect("attempts lock");
    let window = windows.entry(username.to_string()).or_insert((0, Instant::now()));
    if window.1.elapsed() >= Duration::from_secs(ATTEMPT_WINDOW_SECS) {
        window.0 = 0;
        window.1 = Instant::now();
    }
    if window.0 >= MAX_ATTEMPTS_PER_WINDOW {
        return false;
    }
    window.0 += 1;
    true
}

async fn create_session(st: &State, user_id: i64) -> Result<String, sqlx::Error> {
    let token = Uuid::new_v4().simple().to_string();
    sqlx::query("INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)")
        .bind(&token)
        .bind(user_id)
        .bind(crate::clock::now())
        .execute(&st.pool)
        .await?;
    Ok(token)
}

// The Secure flag is only correct when the request really arrived over TLS. On a plain-HTTP
// instance behind a terminating proxy, x-forwarded-proto is what says so; a client can set that
// header itself, so it is read as a hint rather than as proof.
fn served_over_tls(parts: &Parts) -> bool {
    match parts
        .headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
    {
        Some(value) => value.split(',').next().map(str::trim) == Some("https"),
        None => parts.uri.scheme() == Some(&Scheme::HTTPS),
    }
}

fn with_cookie(mut resp: Response, token: &str, secure: bool) -> Response {
    let mut value = format!(
        "{COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
        SESSION_MAX_AGE_DAYS * 86400
    );
    if secure {
        value.push_str("; Secure");
    }
    resp
        .headers_mut()
        .insert(SET_COOKIE, HeaderValue::try_from(value).expect("valid cookie"));
    resp
}

#[derive(Deserialize)]
pub struct RegisterBody {
    pub username: String,
    pub password: String,
}

pub async fn register(AxumState(st): AxumState<State>, parts: Parts, Json(body): Json<RegisterBody>) -> Response {
    let username = body.username.trim().to_string();
    if username.is_empty() || username.chars().any(char::is_whitespace) {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "username is required and cannot contain spaces");
    }
    if body.password.len() < 8 {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "password must be at least 8 characters");
    }
    if !crate::settings::can_register(&st).await {
        return error(
            StatusCode::FORBIDDEN,
            "registration_closed",
            "self-registration is closed. Ask an admin to open it.",
        );
    }

    let count = match sqlx::query("SELECT COUNT(*) AS n FROM users").fetch_one(&st.pool).await {
        Ok(row) => row.get::<i64, _>("n"),
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    // The first account to register owns the instance.
    let role = if count == 0 { "admin" } else { "user" };
    // The first account is never held back, because there is no one to approve it.
    let pending = count > 0 && crate::settings::requires_approval(&st).await;
    let hash = match bcrypt::hash(&body.password, 10) {
        Ok(hash) => hash,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };

    let row = match sqlx::query(
        "INSERT INTO users (username, password_hash, role, created_at, pending) VALUES (?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(&username)
    .bind(&hash)
    .bind(role)
    .bind(crate::clock::now())
    .bind(pending as i64)
    .fetch_one(&st.pool)
    .await
    {
        Ok(row) => row,
        Err(err) => return error(StatusCode::CONFLICT, "invalid_request", err.to_string()),
    };
    let id = row.get::<i64, _>("id");
    if pending {
        // No session yet: the account is signed in after an admin approves it.
        return Json(json!({ "id": id, "username": username, "role": role, "pending": true })).into_response();
    }
    let token = match create_session(&st, id).await {
        Ok(token) => token,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let resp = Json(json!({ "id": id, "username": username, "role": role })).into_response();
    with_cookie(resp, &token, served_over_tls(&parts))
}

#[derive(Deserialize)]
pub struct LoginBody {
    pub username: String,
    pub password: String,
}

pub async fn login(AxumState(st): AxumState<State>, parts: Parts, Json(body): Json<LoginBody>) -> Response {
    let username = body.username.trim().to_string();
    if !attempt_allowed(&st, &username) {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_attempts",
            "too many sign-in attempts for that account, try again in a minute",
        );
    }
    let row = match sqlx::query("SELECT id, username, password_hash, role, enabled, pending FROM users WHERE username = ?")
        .bind(&username)
        .fetch_optional(&st.pool)
        .await
    {
        Ok(row) => row,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    // The same answer for an account that does not exist and a password that does not match, so a
    // sign-in attempt cannot be used to find out which usernames the instance has.
    let Some(row) = row else {
        return error(StatusCode::UNAUTHORIZED, "invalid_credentials", "username or password is not correct");
    };
    if row.get::<i64, _>("pending") == 1 {
        return error(StatusCode::FORBIDDEN, "account_pending", "this account is waiting for an admin to approve it");
    }
    let id = row.get::<i64, _>("id");

    // Checked before anything is written, so a disabled account is not changed by a sign-in that
    // it is not allowed to make.
    if row.get::<i64, _>("enabled") == 0 {
        return error(StatusCode::FORBIDDEN, "account_disabled", "this account is disabled");
    }
    // An account made by an admin has no password until the person signs in for the first time;
    // what they enter then becomes their own password.
    match row.get::<Option<String>, _>("password_hash") {
        Some(hash) => {
            if !bcrypt::verify(&body.password, &hash).unwrap_or(false) {
                return error(StatusCode::UNAUTHORIZED, "invalid_credentials", "username or password is not correct");
            }
        }
        None => {
            if body.password.len() < 8 {
                return error(StatusCode::BAD_REQUEST, "invalid_request", "password must be at least 8 characters");
            }
            let hash = match bcrypt::hash(&body.password, 10) {
                Ok(hash) => hash,
                Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
            };
            if let Err(err) = sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
                .bind(&hash)
                .bind(id)
                .execute(&st.pool)
                .await
            {
                return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string());
            }
        }
    }

    let token = match create_session(&st, id).await {
        Ok(token) => token,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let resp = Json(json!({
        "id": id,
        "username": row.get::<String, _>("username"),
        "role": row.get::<String, _>("role"),
    }))
    .into_response();
    with_cookie(resp, &token, served_over_tls(&parts))
}

pub async fn logout(AxumState(st): AxumState<State>, req: Request) -> Response {
    if let Some(token) = cookie_value(&req, COOKIE_NAME) {
        let _ = sqlx::query("DELETE FROM sessions WHERE token = ?")
            .bind(&token)
            .execute(&st.pool)
            .await;
    }
    let mut resp = Json(json!({ "ok": true })).into_response();
    resp.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::try_from(format!("{COOKIE_NAME}=; Path=/; Max-Age=0")).expect("valid cookie"),
    );
    resp
}

pub async fn me(caller: Caller) -> Response {
    Json(json!({ "id": caller.id, "username": caller.username, "role": caller.role })).into_response()
}
