use axum::extract::{State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde_json::json;
use sqlx::{Column, Row};

use crate::api::error;
use crate::auth::Caller;
use crate::state::State;

// Usage rows are stored as UTC text, so time filters go through strftime rather than
// raw string comparison. The column must be qualified wherever a join makes user_id
// ambiguous.
fn scope_clause(scoped: bool, column: &str) -> String {
    if scoped {
        format!(" WHERE {column} = ?")
    } else {
        String::new()
    }
}

pub async fn totals(st: &State, scoped: bool, user_id: i64) -> Result<serde_json::Value, sqlx::Error> {
    let sql = format!(
        "SELECT COUNT(*) AS requests, \
                COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens, \
                COALESCE(SUM(completion_tokens), 0) AS completion_tokens, \
                COALESCE(SUM(total_tokens), 0) AS total_tokens, \
                COALESCE(CAST(AVG(latency_ms) AS INTEGER), 0) AS avg_latency_ms, \
                SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS errors, \
                SUM(CASE WHEN strftime('%Y-%m-%d', created_at) = date('now') THEN 1 ELSE 0 END) AS today_requests, \
                COALESCE(SUM(CASE WHEN strftime('%Y-%m-%d', created_at) = date('now') THEN total_tokens ELSE 0 END), 0) AS today_tokens \
         FROM usage{scope}",
        scope = scope_clause(scoped, "usage.user_id")
    );
    let query = if scoped {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(user_id)
    } else {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
    };
    let row = query.fetch_one(&st.pool).await?;
    Ok(json!({
        "requests": row.get::<i64, _>("requests"),
        "prompt_tokens": row.get::<i64, _>("prompt_tokens"),
        "completion_tokens": row.get::<i64, _>("completion_tokens"),
        "total_tokens": row.get::<i64, _>("total_tokens"),
        "avg_latency_ms": row.get::<i64, _>("avg_latency_ms"),
        "errors": row.get::<i64, _>("errors"),
        "today_requests": row.get::<i64, _>("today_requests"),
        "today_tokens": row.get::<i64, _>("today_tokens"),
    }))
}

async fn buckets(st: &State, scoped: bool, user_id: i64, expr: &str, window: &str) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    let sql = format!(
        "SELECT {expr} AS bucket, COUNT(*) AS requests, COALESCE(SUM(total_tokens), 0) AS tokens, \
                SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS errors \
         FROM usage WHERE strftime('%Y-%m-%d %H:%M:%S', created_at) >= datetime('now', '{window}') \
         {scope} \
         GROUP BY bucket ORDER BY bucket",
        expr = expr,
        window = window,
        scope = if scoped { "AND user_id = ?" } else { "" }
    );
    let query = if scoped {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(user_id)
    } else {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
    };
    let rows = query.fetch_all(&st.pool).await?;
    Ok(rows
        .iter()
        .map(|row| {
            json!({
                "bucket": row.get::<String, _>("bucket"),
                "requests": row.get::<i64, _>("requests"),
                "tokens": row.get::<i64, _>("tokens"),
                "errors": row.get::<i64, _>("errors"),
            })
        })
        .collect())
}

async fn grouped(st: &State, scoped: bool, user_id: i64, sql: &str) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    let query = if scoped {
        sqlx::query(sqlx::AssertSqlSafe(sql)).bind(user_id)
    } else {
        sqlx::query(sqlx::AssertSqlSafe(sql))
    };
    let rows = query.fetch_all(&st.pool).await?;
    Ok(rows
        .iter()
        .map(|row| {
            let mut out = serde_json::Map::new();
            for column in row.columns() {
                let name = column.name();
                let value = row.try_get::<i64, _>(name)
                    .map(serde_json::Value::from)
                    .unwrap_or_else(|_| serde_json::Value::String(row.try_get::<String, _>(name).unwrap_or_default()));
                out.insert(name.to_string(), value);
            }
            serde_json::Value::Object(out)
        })
        .collect())
}

async fn build_summary(st: &State, caller: &Caller) -> Result<serde_json::Value, sqlx::Error> {
    let scoped = caller.role != "admin";
    let admin = caller.role == "admin";
    let scope = scope_clause(scoped, "usage.user_id");

    let all = totals(st, scoped, caller.id).await?;
    let hourly = buckets(st, scoped, caller.id, "strftime('%Y-%m-%dT%H:00:00Z', created_at)", "-24 hours").await?;
    let daily = buckets(st, scoped, caller.id, "strftime('%Y-%m-%d', created_at)", "-14 days").await?;
    let models = grouped(
        st,
        scoped,
        caller.id,
        &format!(
            "SELECT COALESCE(model, 'unknown') AS model, COUNT(*) AS requests, COALESCE(SUM(total_tokens), 0) AS tokens \
             FROM usage {scope} GROUP BY model ORDER BY tokens DESC LIMIT 8",
        ),
    )
    .await?;
    let users = if admin {
        grouped(
            st,
            false,
            caller.id,
            "SELECT u.name AS name, u.email AS email, COUNT(r.id) AS requests, COALESCE(SUM(r.total_tokens), 0) AS tokens \
             FROM users u JOIN usage r ON r.user_id = u.id GROUP BY u.id ORDER BY tokens DESC LIMIT 20",
        )
        .await?
    } else {
        Vec::new()
    };

    Ok(json!({
        "scope": if admin { "all" } else { "self" },
        "totals": all,
        "hourly": hourly,
        "daily": daily,
        "models": models,
        "users": users,
    }))
}

pub async fn summary(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    match build_summary(&st, &caller).await {
        Ok(summary) => Json(summary).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}

pub async fn recent(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    let scoped = caller.role != "admin";
    let sql = format!(
        "SELECT r.id, k.name AS key_name, u.name AS user_name, r.model, r.prompt_tokens, \
                r.completion_tokens, r.total_tokens, r.latency_ms, r.status, r.streamed, r.created_at \
         FROM usage r JOIN api_keys k ON k.id = r.api_key_id JOIN users u ON u.id = r.user_id \
         {scope} ORDER BY r.id DESC LIMIT 50",
        scope = if scoped { "WHERE r.user_id = ?" } else { "" }
    );
    let query = if scoped {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(caller.id)
    } else {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
    };
    let rows = match query.fetch_all(&st.pool).await {
        Ok(rows) => rows,
        Err(err) => return error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    };
    let events: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            json!({
                "id": row.get::<i64, _>("id"),
                "key": row.get::<String, _>("key_name"),
                "user": row.get::<String, _>("user_name"),
                "model": row.get::<Option<String>, _>("model"),
                "prompt_tokens": row.get::<i64, _>("prompt_tokens"),
                "completion_tokens": row.get::<i64, _>("completion_tokens"),
                "total_tokens": row.get::<i64, _>("total_tokens"),
                "latency_ms": row.get::<i64, _>("latency_ms"),
                "status": row.get::<i64, _>("status"),
                "streamed": row.get::<i64, _>("streamed") != 0,
                "created_at": row.get::<String, _>("created_at"),
            })
        })
        .collect();
    Json(json!({ "events": events })).into_response()
}
