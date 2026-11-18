use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use chrono::{DateTime, Datelike, NaiveDate, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::api::{error, server_error};
use crate::auth::Caller;
use crate::state::State;

#[derive(Deserialize)]
pub struct CreateKey {
    pub name: String,
    pub connection_id: Option<i64>,
    pub model: Option<String>,
    pub rpm_limit: Option<i64>,
    pub token_cap: Option<i64>,
    pub daily_token_cap: Option<i64>,
    pub monthly_token_cap: Option<i64>,
    pub expires_at: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateKey {
    pub name: Option<String>,
    pub connection_id: Option<i64>,
    pub model: Option<String>,
    pub enabled: Option<bool>,
    pub rpm_limit: Option<i64>,
    pub token_cap: Option<i64>,
    // Left out, a day or a month limit stays as it is; null removes it.
    #[serde(default, deserialize_with = "given")]
    pub daily_token_cap: Option<Option<i64>>,
    #[serde(default, deserialize_with = "given")]
    pub monthly_token_cap: Option<Option<i64>>,
    // Left out, the expiry stays as it is; null clears it, so the key no longer expires.
    #[serde(default, deserialize_with = "given")]
    pub expires_at: Option<Option<String>>,
}

// A field that was sent, null included, as against one that was left out.
fn given<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}

// When a key stops working, written the way `clock` writes a moment so the gateway compares the two as text. A
// date alone means the key works through that day in UTC, which is how a date picker means it. A moment that has
// passed is refused rather than stored: a key is stopped now by turning it off.
fn expiry(given: &str) -> Result<String, Box<Response>> {
    let refuse =
        |message: &str| Box::new(error(StatusCode::BAD_REQUEST, "invalid_request", message));
    let given = given.trim();
    let at = if let Ok(day) = NaiveDate::parse_from_str(given, "%Y-%m-%d") {
        match day.succ_opt().and_then(|next| next.and_hms_opt(0, 0, 0)) {
            Some(start) => start.and_utc(),
            None => return Err(refuse("the expiry is not a date that can be read")),
        }
    } else {
        match DateTime::parse_from_rfc3339(given) {
            Ok(at) => at.with_timezone(&Utc),
            Err(_) => {
                return Err(refuse(
                    "the expiry must be a date written YYYY-MM-DD or an RFC 3339 time",
                ));
            }
        }
    };
    if at <= Utc::now() {
        return Err(refuse(
            "the expiry has already passed; turn the key off to stop it now",
        ));
    }
    // A year past 9999 is written with a sign, which sorts before every year of four digits, so it would compare
    // as long passed. A key that is meant never to expire is given no expiry.
    if at.year() > 9999 {
        return Err(refuse(
            "the expiry must be before the year 10000; leave it out for a key that does not expire",
        ));
    }
    Ok(at.to_rfc3339_opts(SecondsFormat::Secs, true))
}

// What an admin is shown of another account's key: the fixed `dreamgate_` prefix and the first and last four of
// its random characters, enough to tell it from the others and to match it against a key someone reports, while
// the ninety random bits of a version 4 UUID between them stay hidden.
fn key_hint(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    if chars.len() <= 18 {
        return "…".to_string();
    }
    let start: String = chars[..14].iter().collect();
    let end: String = chars[chars.len() - 4..].iter().collect();
    format!("{start}…{end}")
}

// One row shape for both lists. The Keys page shows the caller's own keys; an admin can ask for
// the other accounts separately, so a table never mixes two owners. The input and the output are
// reported apart because the page shows them apart; the total stays, because the list is ordered by it
// and a client that read it before still can. When a key was last used is the time of its last recorded
// request, read from the rows rather than written on every request; a request refused before it was recorded,
// and a model list, do not count.
const KEY_SQL: &str = "SELECT k.id, k.name, k.key, k.model, k.connection_id, k.enabled, k.rpm_limit, k.token_cap, \
        k.daily_token_cap, k.monthly_token_cap, \
        COALESCE(SUM(CASE WHEN r.created_at >= ? THEN r.total_tokens END), 0) AS used_today, \
        COALESCE(SUM(CASE WHEN r.created_at >= ? THEN r.total_tokens END), 0) AS used_this_month, \
        k.created_at, k.expires_at, MAX(r.created_at) AS last_used_at, c.name AS connection, u.username AS owner, \
        COUNT(r.id) AS requests, COALESCE(SUM(r.total_tokens), 0) AS tokens, \
        COALESCE(SUM(r.prompt_tokens), 0) AS prompt_tokens, \
        COALESCE(SUM(r.completion_tokens), 0) AS completion_tokens \
 FROM api_keys k LEFT JOIN usage r ON r.api_key_id = k.id \
 LEFT JOIN connections c ON c.id = k.connection_id \
 JOIN users u ON u.id = k.user_id {scope} GROUP BY k.id ORDER BY tokens DESC";

// The key itself is in a row only for its owner. An admin reads every account's keys to manage them, and a
// key is a credential that bills its account, so another account's key is shown by its hint alone.
async fn key_rows(
    st: &State,
    scope: &str,
    user_id: i64,
    reveal: bool,
) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    let sql = KEY_SQL.replace("{scope}", scope);
    // The day and the month in UTC, from their first moment, written as the rows write a moment, the same bounds
    // the gateway reads a key's limits with.
    let now = chrono::Utc::now();
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(format!("{}T00:00:00Z", now.format("%Y-%m-%d")))
        .bind(format!("{}-01T00:00:00Z", now.format("%Y-%m")))
        .bind(user_id)
        .fetch_all(&st.pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let key = row.get::<String, _>("key");
            json!({
                "id": row.get::<i64, _>("id"),
                "name": row.get::<String, _>("name"),
                "key_hint": key_hint(&key),
                "key": reveal.then_some(key),
                "model": row.get::<Option<String>, _>("model"),
                "connection_id": row.get::<Option<i64>, _>("connection_id"),
                "connection": row.get::<Option<String>, _>("connection"),
                "owner": row.get::<String, _>("owner"),
                "enabled": row.get::<i64, _>("enabled") != 0,
                "rpm_limit": row.get::<Option<i64>, _>("rpm_limit"),
                "token_cap": row.get::<Option<i64>, _>("token_cap"),
                "daily_token_cap": row.get::<Option<i64>, _>("daily_token_cap"),
                "monthly_token_cap": row.get::<Option<i64>, _>("monthly_token_cap"),
                "used_today": row.get::<i64, _>("used_today"),
                "used_this_month": row.get::<i64, _>("used_this_month"),
                "requests": row.get::<i64, _>("requests"),
                "tokens": row.get::<i64, _>("tokens"),
                "prompt_tokens": row.get::<i64, _>("prompt_tokens"),
                "completion_tokens": row.get::<i64, _>("completion_tokens"),
                "created_at": row.get::<String, _>("created_at"),
                "expires_at": row.get::<Option<String>, _>("expires_at"),
                "last_used_at": row.get::<Option<String>, _>("last_used_at"),
            })
        })
        .collect())
}

pub async fn list(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    match key_rows(&st, "WHERE k.user_id = ?", caller.id, true).await {
        Ok(keys) => Json(json!({ "keys": keys })).into_response(),
        Err(err) => server_error(err),
    }
}

// Keys that belong to someone else, for an admin.
pub async fn other_accounts(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    if caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "only an admin can see keys from other accounts",
        );
    }
    match key_rows(&st, "WHERE k.user_id != ?", caller.id, false).await {
        Ok(keys) => Json(json!({ "keys": keys })).into_response(),
        Err(err) => server_error(err),
    }
}

fn new_key() -> String {
    format!("dreamgate_{}", Uuid::new_v4().simple())
}

pub async fn create(
    AxumState(st): AxumState<State>,
    caller: Caller,
    Json(body): Json<CreateKey>,
) -> Response {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "name is required",
        );
    }
    // A limit of 0 would refuse every request, so a limit is a positive number or absent.
    if [
        body.rpm_limit,
        body.token_cap,
        body.daily_token_cap,
        body.monthly_token_cap,
    ]
    .iter()
    .any(|limit| limit.is_some_and(|value| value < 1))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "limits must be a positive number, or left unset",
        );
    }
    let expires_at = match body
        .expires_at
        .as_deref()
        .filter(|at| !at.trim().is_empty())
    {
        Some(at) => match expiry(at) {
            Ok(at) => Some(at),
            Err(refused) => return *refused,
        },
        None => None,
    };
    let key = new_key();
    let result = sqlx::query(
        "INSERT INTO api_keys (user_id, name, key, connection_id, model, rpm_limit, token_cap, created_at, \
         expires_at, daily_token_cap, monthly_token_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(caller.id)
    .bind(&name)
    .bind(&key)
    .bind(body.connection_id)
    .bind(body.model.as_deref().map(str::trim).filter(|m| !m.is_empty()))
    .bind(body.rpm_limit)
    .bind(body.token_cap)
    .bind(crate::clock::now())
    .bind(&expires_at)
    .bind(body.daily_token_cap)
    .bind(body.monthly_token_cap)
    .fetch_one(&st.pool)
    .await;
    match result {
        Ok(row) => {
            Json(json!({ "id": row.get::<i64, _>("id"), "key": key, "expires_at": expires_at }))
                .into_response()
        }
        Err(err) => write_failed(&st, body.connection_id, err).await,
    }
}

// A key bound to a connection that does not exist is a mistake in what was sent, not a failure of
// the server: the connection may have been deleted in another tab while the form was open. Which
// reference failed is read rather than assumed, because the account a key belongs to can be deleted
// in the same moment, and that is not the person's mistake.
async fn write_failed(st: &State, connection_id: Option<i64>, err: sqlx::Error) -> Response {
    let reference = matches!(&err, sqlx::Error::Database(db) if db.is_foreign_key_violation());
    if let (true, Some(id)) = (reference, connection_id) {
        let found = sqlx::query("SELECT 1 FROM connections WHERE id = ?")
            .bind(id)
            .fetch_optional(&st.pool)
            .await;
        if matches!(found, Ok(None)) {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "no connection with that id",
            );
        }
    }
    server_error(err)
}

pub async fn update(
    AxumState(st): AxumState<State>,
    Path(id): Path<i64>,
    caller: Caller,
    Json(body): Json<UpdateKey>,
) -> Response {
    if caller.role != "admin" {
        let owns = sqlx::query("SELECT user_id FROM api_keys WHERE id = ?")
            .bind(id)
            .fetch_optional(&st.pool)
            .await
            .ok()
            .flatten();
        match owns {
            Some(row) if row.get::<i64, _>("user_id") == caller.id => {}
            Some(_) => {
                return error(
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    "you can only change your own key",
                );
            }
            None => return error(StatusCode::NOT_FOUND, "not_found", "key not found"),
        }
    }
    // A limit of 0 would refuse every request, as it would when a key is made, so it is refused here too.
    if [
        body.rpm_limit,
        body.token_cap,
        body.daily_token_cap.flatten(),
        body.monthly_token_cap.flatten(),
    ]
    .iter()
    .any(|limit| limit.is_some_and(|value| value < 1))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "limits must be a positive number, or left unset",
        );
    }
    // Read before anything is written, so an expiry that cannot be read changes nothing else either.
    let expires_at = match &body.expires_at {
        None => None,
        Some(None) => Some(None),
        Some(Some(at)) if at.trim().is_empty() => Some(None),
        Some(Some(at)) => match expiry(at) {
            Ok(at) => Some(Some(at)),
            Err(refused) => return *refused,
        },
    };
    // The expiry is written in the same statement as the rest, so a change refused for another field, a binding
    // to a connection that is gone, changes the expiry no more than it changes the name.
    let result = sqlx::query(
        "UPDATE api_keys SET expires_at = CASE WHEN ? THEN ? ELSE expires_at END, \
                name = COALESCE(?, name), connection_id = COALESCE(?, connection_id), \
                model = COALESCE(?, model), enabled = COALESCE(?, enabled), \
                rpm_limit = COALESCE(?, rpm_limit), token_cap = COALESCE(?, token_cap), \
                daily_token_cap = CASE WHEN ? THEN ? ELSE daily_token_cap END, \
                monthly_token_cap = CASE WHEN ? THEN ? ELSE monthly_token_cap END \
         WHERE id = ?",
    )
    .bind(expires_at.is_some())
    .bind(expires_at.flatten())
    .bind(body.name)
    .bind(body.connection_id)
    .bind(body.model)
    .bind(body.enabled.map(|v| v as i64))
    .bind(body.rpm_limit)
    .bind(body.token_cap)
    .bind(body.daily_token_cap.is_some())
    .bind(body.daily_token_cap.flatten())
    .bind(body.monthly_token_cap.is_some())
    .bind(body.monthly_token_cap.flatten())
    .bind(id)
    .execute(&st.pool)
    .await;
    match result {
        Ok(_) => Json(json!({ "ok": true })).into_response(),
        Err(err) => write_failed(&st, body.connection_id, err).await,
    }
}

// A new secret for a key that keeps everything else: its id, its name, its limits, its binding and the usage
// recorded for it. The old secret stops working at once, which is the point of rotating a key that may have
// leaked. The new one is given back to its owner; an admin rotating another account's key is not given it,
// for the same reason the list does not show it, and the owner reads it from their own list.
// The body is an empty JSON object rather than nothing: a form on another page of the same site can post with the
// session cookie but cannot send JSON, so it cannot rotate someone's key out from under their clients.
pub async fn rotate(
    AxumState(st): AxumState<State>,
    Path(id): Path<i64>,
    caller: Caller,
    Json(_): Json<serde_json::Value>,
) -> Response {
    let owner = match sqlx::query("SELECT user_id FROM api_keys WHERE id = ?")
        .bind(id)
        .fetch_optional(&st.pool)
        .await
    {
        Ok(Some(row)) => row.get::<i64, _>("user_id"),
        Ok(None) => return error(StatusCode::NOT_FOUND, "not_found", "key not found"),
        Err(err) => return server_error(err),
    };
    if owner != caller.id && caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "you can only rotate your own key",
        );
    }
    let key = new_key();
    // Only the key that was checked: an id freed by a delete in the meantime can be given to another account's new
    // key, which this must not rotate.
    match sqlx::query("UPDATE api_keys SET key = ? WHERE id = ? AND user_id = ?")
        .bind(&key)
        .bind(id)
        .bind(owner)
        .execute(&st.pool)
        .await
    {
        Ok(done) if done.rows_affected() == 1 => {}
        Ok(_) => return error(StatusCode::NOT_FOUND, "not_found", "key not found"),
        Err(err) => return server_error(err),
    }
    let shown = (owner == caller.id).then_some(key);
    Json(json!({ "id": id, "key": shown, "rotated": true })).into_response()
}

pub async fn delete(
    AxumState(st): AxumState<State>,
    Path(id): Path<i64>,
    caller: Caller,
) -> Response {
    if caller.role != "admin" {
        let owns = sqlx::query("SELECT user_id FROM api_keys WHERE id = ?")
            .bind(id)
            .fetch_optional(&st.pool)
            .await
            .ok()
            .flatten();
        match owns {
            Some(row) if row.get::<i64, _>("user_id") == caller.id => {}
            Some(_) => {
                return error(
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    "you can only delete your own key",
                );
            }
            None => return error(StatusCode::NOT_FOUND, "not_found", "key not found"),
        }
    }
    let result = sqlx::query("DELETE FROM api_keys WHERE id = ?")
        .bind(id)
        .execute(&st.pool)
        .await;
    match result {
        Ok(_) => Json(json!({ "ok": true })).into_response(),
        Err(err) => server_error(err),
    }
}
