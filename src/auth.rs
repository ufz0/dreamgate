use axum::extract::{FromRequestParts, Request, State as AxumState};
use axum::http::header::{SET_COOKIE, HeaderValue};
use axum::http::request::Parts;
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

#[derive(Clone)]
pub struct Caller {
    pub id: i64,
    pub email: String,
    pub name: String,
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
    let row = match sqlx::query(
        "SELECT u.id, u.email, u.name, u.role FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token = ?",
    )
    .bind(&token)
    .fetch_optional(&st.pool)
    .await
    {
        Ok(row) => row,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let Some(row) = row else {
        return error(StatusCode::UNAUTHORIZED, "invalid_session", "session expired");
    };
    req.extensions_mut().insert(Caller {
        id: row.get::<i64, _>("id"),
        email: row.get::<String, _>("email"),
        name: row.get::<String, _>("name"),
        role: row.get::<String, _>("role"),
    });
    next.run(req).await
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

fn with_cookie(mut resp: Response, token: &str) -> Response {
    let value = format!("{COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000");
    resp
        .headers_mut()
        .insert(SET_COOKIE, HeaderValue::try_from(value).expect("valid cookie"));
    resp
}

#[derive(Deserialize)]
pub struct RegisterBody {
    pub email: String,
    pub name: String,
    pub password: String,
}

pub async fn register(AxumState(st): AxumState<State>, Json(body): Json<RegisterBody>) -> Response {
    let email = body.email.trim().to_lowercase();
    let name = body.name.trim().to_string();
    if email.is_empty() || !email.contains('@') {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "email is required");
    }
    if name.is_empty() {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "name is required");
    }
    if body.password.len() < 8 {
        return error(StatusCode::BAD_REQUEST, "invalid_request", "password must be at least 8 characters");
    }

    let count = match sqlx::query("SELECT COUNT(*) AS n FROM users").fetch_one(&st.pool).await {
        Ok(row) => row.get::<i64, _>("n"),
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    // The first account to register owns the instance.
    let role = if count == 0 { "admin" } else { "user" };
    let hash = match bcrypt::hash(&body.password, 10) {
        Ok(hash) => hash,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };

    let row = match sqlx::query(
        "INSERT INTO users (email, name, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(&email)
    .bind(&name)
    .bind(&hash)
    .bind(role)
    .bind(crate::clock::now())
    .fetch_one(&st.pool)
    .await
    {
        Ok(row) => row,
        Err(err) => return error(StatusCode::CONFLICT, "invalid_request", err.to_string()),
    };
    let id = row.get::<i64, _>("id");
    let token = match create_session(&st, id).await {
        Ok(token) => token,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let resp = Json(json!({ "id": id, "email": email, "name": name, "role": role })).into_response();
    with_cookie(resp, &token)
}

#[derive(Deserialize)]
pub struct LoginBody {
    pub email: String,
    pub password: String,
}

pub async fn login(AxumState(st): AxumState<State>, Json(body): Json<LoginBody>) -> Response {
    let email = body.email.trim().to_lowercase();
    let row = match sqlx::query("SELECT id, name, password_hash, role FROM users WHERE email = ?")
        .bind(&email)
        .fetch_optional(&st.pool)
        .await
    {
        Ok(row) => row,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let Some(row) = row else {
        return error(StatusCode::UNAUTHORIZED, "invalid_credentials", "no account for that email");
    };
    if !bcrypt::verify(&body.password, &row.get::<String, _>("password_hash")).unwrap_or(false) {
        return error(StatusCode::UNAUTHORIZED, "invalid_credentials", "password does not match");
    }
    let id = row.get::<i64, _>("id");
    let token = match create_session(&st, id).await {
        Ok(token) => token,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let resp = Json(json!({
        "id": id,
        "email": email,
        "name": row.get::<String, _>("name"),
        "role": row.get::<String, _>("role"),
    }))
    .into_response();
    with_cookie(resp, &token)
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
    Json(json!({ "id": caller.id, "email": caller.email, "name": caller.name, "role": caller.role })).into_response()
}
