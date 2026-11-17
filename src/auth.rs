use std::time::{Duration, Instant};

use axum::extract::{FromRequestParts, Request, State as AxumState};
use axum::http::StatusCode;
use axum::http::header::{HeaderValue, SET_COOKIE};
use axum::http::request::Parts;
use axum::http::uri::Scheme;
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::api::{error, server_error};
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

pub async fn require_session(
    AxumState(st): AxumState<State>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(token) = cookie_value(&req, COOKIE_NAME) else {
        return error(StatusCode::UNAUTHORIZED, "invalid_session", "not signed in");
    };
    // The window is read in the lookup rather than trusted from the cookie, because a client can
    // hold on to a token and keep sending it.
    let sql = format!(
        "SELECT u.id, u.username, u.role FROM sessions s JOIN users u ON u.id = s.user_id \
         WHERE s.token = ? AND strftime('%Y-%m-%d %H:%M:%S', s.created_at) >= datetime('now', '-{SESSION_MAX_AGE_DAYS} days')",
    );
    let row = match sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(&token)
        .fetch_optional(&st.pool)
        .await
    {
        Ok(row) => row,
        Err(err) => {
            return server_error(err);
        }
    };
    let Some(row) = row else {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_session",
            "session expired",
        );
    };
    req.extensions_mut().insert(Caller {
        id: row.get::<i64, _>("id"),
        username: row.get::<String, _>("username"),
        role: row.get::<String, _>("role"),
    });
    next.run(req).await
}

// Bcrypt is genuinely CPU-bound: a cost-10 hash takes tens of milliseconds, which is long enough to
// hold the runtime worker an async handler is standing on while other requests are only waiting for an
// upstream. It is moved to the blocking pool, which is the one place this code uses it.
pub async fn hash_password(password: &str) -> Result<String, String> {
    let password = password.to_string();
    let joined = tokio::task::spawn_blocking(move || bcrypt::hash(&password, 10))
        .await
        .map_err(|err| err.to_string())?;
    joined.map_err(|err| err.to_string())
}

pub async fn password_matches(password: &str, hash: &str) -> bool {
    let password = password.to_string();
    let hash = hash.to_string();
    match tokio::task::spawn_blocking(move || bcrypt::verify(&password, &hash)).await {
        Ok(verified) => verified.unwrap_or(false),
        // A blocking task that panics is not a password that matches.
        Err(_) => false,
    }
}

// The count is kept in the process rather than in the database: it only has to bound a loop, not
// survive a restart.
fn attempt_allowed(st: &State, username: &str) -> bool {
    let mut windows = st.attempts.lock().expect("attempts lock");
    let window = windows
        .entry(username.to_string())
        .or_insert((0, Instant::now()));
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
    resp.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::try_from(value).expect("valid cookie"),
    );
    resp
}

fn registration_closed() -> Response {
    error(
        StatusCode::FORBIDDEN,
        "registration_closed",
        "self-registration is closed. Ask an admin to open it.",
    )
}

#[derive(Deserialize)]
pub struct RegisterBody {
    pub username: String,
    pub password: String,
}

pub async fn register(
    AxumState(st): AxumState<State>,
    parts: Parts,
    Json(body): Json<RegisterBody>,
) -> Response {
    let username = body.username.trim().to_string();
    if username.is_empty() || username.chars().any(char::is_whitespace) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "username is required and cannot contain spaces",
        );
    }
    if body.password.len() < 8 {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "password must be at least 8 characters",
        );
    }
    // Only an early answer, so a closed instance does not spend a hash on every attempt: whether the
    // account may be made is decided again by the insert below.
    if !crate::settings::can_register(&st).await {
        return registration_closed();
    }
    let hash = match hash_password(&body.password).await {
        Ok(hash) => hash,
        Err(err) => {
            return server_error(err);
        }
    };

    // Whether the account may be made, its role and whether it waits are all decided by the statement
    // that makes it. Counted before the hash, registrations that arrived together on an empty instance
    // all counted none and were all made admin; a writing statement takes the write lock before it
    // reads, so only the first insert finds no accounts. The first account owns the instance and is
    // never held back, because there is no one to approve it; after it, registration has to be open,
    // and a new account waits unless approval is turned off, with the defaults `settings` reads.
    let row = match sqlx::query(
        "INSERT INTO users (username, password_hash, role, created_at, pending) \
         SELECT ?, ?, CASE WHEN EXISTS (SELECT 1 FROM users) THEN 'user' ELSE 'admin' END, ?, \
                EXISTS (SELECT 1 FROM users) \
                AND COALESCE((SELECT value FROM settings WHERE key = 'require_approval'), '1') = '1' \
         WHERE NOT EXISTS (SELECT 1 FROM users) \
            OR COALESCE((SELECT value FROM settings WHERE key = 'open_registration'), '0') = '1' \
         RETURNING id, role, pending",
    )
    .bind(&username)
    .bind(&hash)
    .bind(crate::clock::now())
    .fetch_optional(&st.pool)
    .await
    {
        Ok(Some(row)) => row,
        // Registration was closed by the time the account would have been made.
        Ok(None) => return registration_closed(),
        // A name that is taken is named rather than answered with the driver text. The username is the
        // only unique column on users, so a unique violation here can only mean that; a failure for any
        // other reason is not dressed up as a taken name.
        Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
            return error(
                StatusCode::CONFLICT,
                "invalid_request",
                format!("the username {username} is already taken"),
            );
        }
        Err(err) => {
            return server_error(err);
        }
    };
    let id = row.get::<i64, _>("id");
    let role = row.get::<String, _>("role");
    if row.get::<i64, _>("pending") != 0 {
        // No session yet: the account is signed in after an admin approves it.
        return Json(json!({ "id": id, "username": username, "role": role, "pending": true }))
            .into_response();
    }
    let token = match create_session(&st, id).await {
        Ok(token) => token,
        Err(err) => {
            return server_error(err);
        }
    };
    let resp = Json(json!({ "id": id, "username": username, "role": role })).into_response();
    with_cookie(resp, &token, served_over_tls(&parts))
}

#[derive(Deserialize)]
pub struct LoginBody {
    pub username: String,
    pub password: String,
}

pub async fn login(
    AxumState(st): AxumState<State>,
    parts: Parts,
    Json(body): Json<LoginBody>,
) -> Response {
    let username = body.username.trim().to_string();
    if !attempt_allowed(&st, &username) {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_attempts",
            "too many sign-in attempts for that account, try again in a minute",
        );
    }
    let row = match sqlx::query(
        "SELECT id, username, password_hash, role, enabled, pending FROM users WHERE username = ?",
    )
    .bind(&username)
    .fetch_optional(&st.pool)
    .await
    {
        Ok(row) => row,
        Err(err) => {
            return server_error(err);
        }
    };
    // The same answer for an account that does not exist and a password that does not match, so a
    // sign-in attempt cannot be used to find out which usernames the instance has.
    let Some(row) = row else {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
            "username or password is not correct",
        );
    };
    if row.get::<i64, _>("pending") == 1 {
        return error(
            StatusCode::FORBIDDEN,
            "account_pending",
            "this account is waiting for an admin to approve it",
        );
    }
    let id = row.get::<i64, _>("id");

    // Checked before anything is written, so a disabled account is not changed by a sign-in that
    // it is not allowed to make.
    if row.get::<i64, _>("enabled") == 0 {
        return error(
            StatusCode::FORBIDDEN,
            "account_disabled",
            "this account is disabled",
        );
    }
    // An account made by an admin has no password until the person signs in for the first time;
    // what they enter then becomes their own password.
    match row.get::<Option<String>, _>("password_hash") {
        Some(hash) => {
            if !password_matches(&body.password, &hash).await {
                return error(
                    StatusCode::UNAUTHORIZED,
                    "invalid_credentials",
                    "username or password is not correct",
                );
            }
        }
        None => {
            if body.password.len() < 8 {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "password must be at least 8 characters",
                );
            }
            let hash = match hash_password(&body.password).await {
                Ok(hash) => hash,
                Err(err) => {
                    return server_error(err);
                }
            };
            // Two first sign-ins at once both read that there is no password. Only the write that
            // still finds none sets it, and the other is answered as what it now is: a password
            // that does not match. The write also has to find the account still enabled and not
            // held, because an admin can change either while the password is being hashed, and a
            // sign-in that lands after that is not let in on what it read before.
            match sqlx::query(
                "UPDATE users SET password_hash = ? \
                 WHERE id = ? AND password_hash IS NULL AND enabled = 1 AND pending = 0",
            )
            .bind(&hash)
            .bind(id)
            .execute(&st.pool)
            .await
            {
                Ok(done) if done.rows_affected() == 1 => {}
                Ok(_) => {
                    return error(
                        StatusCode::UNAUTHORIZED,
                        "invalid_credentials",
                        "username or password is not correct",
                    );
                }
                Err(err) => {
                    return server_error(err);
                }
            }
        }
    }

    let token = match create_session(&st, id).await {
        Ok(token) => token,
        Err(err) => {
            return server_error(err);
        }
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
    Json(json!({ "id": caller.id, "username": caller.username, "role": caller.role }))
        .into_response()
}
