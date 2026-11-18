use std::collections::HashMap;

use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use chrono::{Duration, SecondsFormat, Utc};
use futures_util::TryStreamExt;
use serde_json::json;
use sqlx::{Column, Row};

use crate::api::{error, server_error};
use crate::auth::Caller;
use crate::state::State;

// Usage rows are stored as UTC text in one shape, `clock::now`'s. The charts read a day or an hour out of it with
// strftime and filter their window the same way, against SQLite's own clock; the connection figures compare the
// text with a moment written in the same shape, which orders as the time does and can use the index on the column.
// The column must be qualified wherever a join makes user_id ambiguous.
fn scope_clause(scoped: bool, column: &str) -> String {
    if scoped {
        format!(" WHERE {column} = ?")
    } else {
        String::new()
    }
}

pub async fn totals(
    st: &State,
    scoped: bool,
    user_id: i64,
) -> Result<serde_json::Value, sqlx::Error> {
    // The figures are kept apart rather than added together: what was sent in and what the model
    // produced are different things, and a prompt cache hit is part of the input, not a second count of
    // it. A timing figure is null when no engine reported one for this instance, and the page leaves the
    // row out rather than showing a zero. Generation speed is measured only over rows whose token counts
    // the upstream reported, so a row counted by the gateway's estimate cannot make a model look faster
    // than it was.
    let sql = format!(
        "SELECT COUNT(*) AS requests, \
                COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens, \
                COALESCE(SUM(completion_tokens), 0) AS completion_tokens, \
                COALESCE(SUM(total_tokens), 0) AS total_tokens, \
                COALESCE(SUM(cached_tokens), 0) AS cached_tokens, \
                COALESCE(CAST(AVG(latency_ms) AS INTEGER), 0) AS avg_latency_ms, \
                SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS errors, \
                SUM(CASE WHEN strftime('%Y-%m-%d', created_at) = date('now') THEN 1 ELSE 0 END) AS today_requests, \
                COALESCE(SUM(CASE WHEN strftime('%Y-%m-%d', created_at) = date('now') THEN total_tokens ELSE 0 END), 0) AS today_tokens, \
                COALESCE(SUM(CASE WHEN strftime('%Y-%m-%d', created_at) = date('now') THEN prompt_tokens ELSE 0 END), 0) AS today_prompt_tokens, \
                COALESCE(SUM(CASE WHEN strftime('%Y-%m-%d', created_at) = date('now') THEN completion_tokens ELSE 0 END), 0) AS today_completion_tokens, \
                COALESCE(SUM(CASE WHEN cached_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS cached_reported, \
                CAST(ROUND(AVG(prompt_ms)) AS INTEGER) AS avg_prompt_ms, \
                CAST(ROUND(AVG(CASE WHEN estimated = 0 AND generation_ms > 0 AND completion_tokens > 0 THEN completion_tokens * 1000.0 / generation_ms END)) AS INTEGER) AS generation_tokens_per_second \
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
        "cached_tokens": row.get::<i64, _>("cached_tokens"),
        // Whether any row reported a cache hit at all. A zero that was reported and a figure that was
        // never reported are different things, and the page says them differently.
        "cached_reported": row.get::<i64, _>("cached_reported"),
        "avg_latency_ms": row.get::<i64, _>("avg_latency_ms"),
        "errors": row.get::<i64, _>("errors"),
        "today_requests": row.get::<i64, _>("today_requests"),
        "today_tokens": row.get::<i64, _>("today_tokens"),
        "today_prompt_tokens": row.get::<i64, _>("today_prompt_tokens"),
        "today_completion_tokens": row.get::<i64, _>("today_completion_tokens"),
        // Null when nothing reported them, so the page can say so instead of showing a figure.
        "avg_prompt_ms": row.get::<Option<i64>, _>("avg_prompt_ms"),
        "generation_tokens_per_second": row.get::<Option<i64>, _>("generation_tokens_per_second"),
    }))
}

async fn buckets(
    st: &State,
    scoped: bool,
    user_id: i64,
    expr: &str,
    window: &str,
) -> Result<Vec<serde_json::Value>, sqlx::Error> {
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

async fn grouped(
    st: &State,
    scoped: bool,
    user_id: i64,
    sql: &str,
) -> Result<Vec<serde_json::Value>, sqlx::Error> {
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
                let value = row
                    .try_get::<i64, _>(name)
                    .map(serde_json::Value::from)
                    .unwrap_or_else(|_| {
                        serde_json::Value::String(
                            row.try_get::<String, _>(name).unwrap_or_default(),
                        )
                    });
                out.insert(name.to_string(), value);
            }
            serde_json::Value::Object(out)
        })
        .collect())
}

// Accounts with no usage still appear, because that is where an admin sets a limit. The day
// figure is what a daily limit is measured against, so it is reported next to it, as the input and
// the output together the way the limit counts them. Every account is listed, with no cap, because this
// is the only list the page has to act on one: an account left off it could not be approved, limited,
// disabled, renamed or deleted. Ordered by username rather than by usage, so a row stays where it is as
// traffic arrives and a new account is found where its name sorts.
async fn accounts(st: &State) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT u.id, u.username, u.enabled, u.pending, u.daily_token_cap, \
                u.password_hash IS NOT NULL AS has_password, u.setup_code_expires_at, COUNT(r.id) AS requests, \
                COALESCE(SUM(r.total_tokens), 0) AS tokens, \
                COALESCE(SUM(r.prompt_tokens), 0) AS prompt_tokens, \
                COALESCE(SUM(r.completion_tokens), 0) AS completion_tokens, \
                COALESCE(SUM(CASE WHEN strftime('%Y-%m-%d', r.created_at) = date('now') THEN r.total_tokens ELSE 0 END), 0) AS used_today \
         FROM users u LEFT JOIN usage r ON r.user_id = u.id GROUP BY u.id ORDER BY u.username",
    )
    .fetch_all(&st.pool)
    .await?;
    Ok(rows
        .iter()
        .map(|row| {
            json!({
                "id": row.get::<i64, _>("id"),
                "username": row.get::<String, _>("username"),
                "enabled": row.get::<i64, _>("enabled") != 0,
                "pending": row.get::<i64, _>("pending") != 0,
                "daily_token_cap": row.get::<Option<i64>, _>("daily_token_cap"),
                // Whether a password is set and until when a setup code works, never the code itself,
                // which is shown once to the admin who asked for it.
                "has_password": row.get::<i64, _>("has_password") != 0,
                "setup_code_expires_at": row.get::<Option<String>, _>("setup_code_expires_at"),
                "requests": row.get::<i64, _>("requests"),
                "tokens": row.get::<i64, _>("tokens"),
                "prompt_tokens": row.get::<i64, _>("prompt_tokens"),
                "completion_tokens": row.get::<i64, _>("completion_tokens"),
                "used_today": row.get::<i64, _>("used_today"),
            })
        })
        .collect())
}

// The overview is the account's own figures plus one figure that is not: the total across
// every account and key. Both are reported separately so the page does not have to guess which
// one it is showing, and the charts are always the caller's own even for an admin.
async fn build_summary(st: &State, caller: &Caller) -> Result<serde_json::Value, sqlx::Error> {
    let admin = caller.role == "admin";

    let self_totals = totals(st, true, caller.id).await?;
    let global = totals(st, false, caller.id).await?;
    let hourly = buckets(
        st,
        true,
        caller.id,
        "strftime('%Y-%m-%dT%H:00:00Z', created_at)",
        "-24 hours",
    )
    .await?;
    let daily = buckets(
        st,
        true,
        caller.id,
        "strftime('%Y-%m-%d', created_at)",
        "-14 days",
    )
    .await?;
    // The eight are the ones with the most tokens, input and output together, and the two sides are
    // reported apart as well, because the page shows them apart.
    let models = grouped(
        st,
        !admin,
        caller.id,
        &format!(
            "SELECT COALESCE(model, 'unknown') AS model, COUNT(*) AS requests, COALESCE(SUM(total_tokens), 0) AS tokens, \
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens, \
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens \
             FROM usage {scope} GROUP BY model ORDER BY tokens DESC LIMIT 8",
            scope = scope_clause(!admin, "usage.user_id"),
        ),
    )
    .await?;
    let users = if admin {
        accounts(st).await?
    } else {
        Vec::new()
    };

    Ok(json!({
        "self": self_totals,
        "global": global,
        "hourly": hourly,
        "daily": daily,
        "models": models,
        "users": users,
    }))
}

pub async fn summary(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    match build_summary(&st, &caller).await {
        Ok(summary) => Json(summary).into_response(),
        Err(err) => server_error(err),
    }
}

pub async fn recent(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    let scoped = caller.role != "admin";
    // A recorded request outlives the key it came in on, so the key is looked up rather than
    // assumed to still be there; the page says which rows have no key left. The estimated flag goes out
    // with the counts, so a row the gateway counted itself is not shown as one the upstream reported.
    let sql = format!(
        "SELECT r.id, k.name AS key_name, u.username AS user_name, r.model, r.prompt_tokens, \
                r.completion_tokens, r.total_tokens, r.latency_ms, r.status, r.streamed, r.estimated, \
                r.created_at \
         FROM usage r LEFT JOIN api_keys k ON k.id = r.api_key_id JOIN users u ON u.id = r.user_id \
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
        Err(err) => {
            return server_error(err);
        }
    };
    let events: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            json!({
                "id": row.get::<i64, _>("id"),
                "key": row.get::<Option<String>, _>("key_name"),
                "user": row.get::<String, _>("user_name"),
                "model": row.get::<Option<String>, _>("model"),
                "prompt_tokens": row.get::<i64, _>("prompt_tokens"),
                "completion_tokens": row.get::<i64, _>("completion_tokens"),
                "total_tokens": row.get::<i64, _>("total_tokens"),
                "latency_ms": row.get::<i64, _>("latency_ms"),
                "status": row.get::<i64, _>("status"),
                "streamed": row.get::<i64, _>("streamed") != 0,
                "estimated": row.get::<i64, _>("estimated") != 0,
                "created_at": row.get::<String, _>("created_at"),
            })
        })
        .collect();
    Json(json!({ "events": events })).into_response()
}

// How far back the figures for each connection look: long enough to cover a night, short enough that what
// they say is about the connection as it is now.
pub const CONNECTION_WINDOW_HOURS: i64 = 24;

// Each connection's last day: what it answered, how many of those answers were errors, how often it failed a
// request it was sent and how often that request was passed on to another connection, and how long its answers
// took. A single answer and a stream are kept apart, because a stream's time is its whole length rather than
// how long the connection took to answer. Admin-only, like the Connections view it is shown in.
pub async fn connections(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    if caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "only an admin can read the connections' figures",
        );
    }
    match connection_figures(&st).await {
        Ok(figures) => Json(figures).into_response(),
        Err(err) => server_error(err),
    }
}

async fn connection_figures(st: &State) -> Result<serde_json::Value, sqlx::Error> {
    // The rows are stored as RFC 3339 text in UTC, the same shape this is written in, so the comparison is one
    // of text and can use the index on the time.
    let since = (Utc::now() - Duration::hours(CONNECTION_WINDOW_HOURS))
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let ids: Vec<i64> = sqlx::query("SELECT id FROM connections ORDER BY priority, id")
        .fetch_all(&st.pool)
        .await?
        .iter()
        .map(|row| row.get("id"))
        .collect();

    // A stream that broke off or went quiet after its head was sent has a row, which its failure names. It is
    // counted as the failure, not as an answer, and its time is not an answer's time: a stall would put the
    // whole stall window into it.
    const ANSWERED: &str = "connection_id IS NOT NULL AND created_at >= ? AND id NOT IN \
         (SELECT usage_id FROM connection_failures WHERE usage_id IS NOT NULL AND created_at >= ?)";
    let mut answered: HashMap<i64, (i64, i64)> = HashMap::new();
    for row in sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT connection_id, COUNT(*) AS answered, \
                SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS errors \
         FROM usage WHERE {ANSWERED} GROUP BY connection_id"
    )))
    .bind(&since)
    .bind(&since)
    .fetch_all(&st.pool)
    .await?
    {
        answered.insert(
            row.get("connection_id"),
            (row.get("answered"), row.get("errors")),
        );
    }

    let mut failed: HashMap<i64, (i64, i64, i64)> = HashMap::new();
    for row in sqlx::query(
        "SELECT connection_id, COUNT(*) AS failures, SUM(passed_on) AS passed_on, \
                SUM(CASE WHEN kind = 'stalled' THEN 1 ELSE 0 END) AS stalled \
         FROM connection_failures WHERE created_at >= ? GROUP BY connection_id",
    )
    .bind(&since)
    .fetch_all(&st.pool)
    .await?
    {
        failed.insert(
            row.get("connection_id"),
            (
                row.get("failures"),
                row.get("passed_on"),
                row.get("stalled"),
            ),
        );
    }

    // SQLite has no percentile, so the latencies of the window are read and the ranks taken here. An answer that
    // was an error is left out: a refusal that comes back at once would make a connection look fast. A day of a
    // busy gateway is many rows, so they are read one at a time and only the number kept, and each connection's
    // are put in order here rather than by SQLite, which would sort the whole day before the first came back.
    let mut latencies: HashMap<(i64, bool), Vec<i64>> = HashMap::new();
    let query = format!(
        "SELECT connection_id, streamed, latency_ms FROM usage WHERE {ANSWERED} AND status < 400"
    );
    let mut rows = sqlx::query(sqlx::AssertSqlSafe(query))
        .bind(&since)
        .bind(&since)
        .fetch(&st.pool);
    while let Some(row) = rows.try_next().await? {
        latencies
            .entry((row.get("connection_id"), row.get::<i64, _>("streamed") != 0))
            .or_default()
            .push(row.get("latency_ms"));
    }
    drop(rows);
    for values in latencies.values_mut() {
        values.sort_unstable();
    }
    let timing = |id: i64, streamed: bool| {
        let sorted = latencies
            .get(&(id, streamed))
            .map_or(&[][..], Vec::as_slice);
        json!({
            "count": sorted.len(),
            "p50_ms": rank(sorted, 0.50),
            "p95_ms": rank(sorted, 0.95),
        })
    };

    let figures: Vec<serde_json::Value> = ids
        .iter()
        .map(|id| {
            let (answered, errors) = answered.get(id).copied().unwrap_or_default();
            let (failures, passed_on, stalled) = failed.get(id).copied().unwrap_or_default();
            json!({
                "id": id,
                "answered": answered,
                "errors": errors,
                "failures": failures,
                "passed_on": passed_on,
                "stalled": stalled,
                "single": timing(*id, false),
                "streamed": timing(*id, true),
            })
        })
        .collect();
    Ok(json!({ "since": since, "hours": CONNECTION_WINDOW_HOURS, "connections": figures }))
}

// The nearest-rank percentile of values in order, and none when there are none, rather than a zero that would
// read as an answer that took no time.
fn rank(sorted: &[i64], share: f64) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let index = ((share * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    Some(sorted[index])
}

#[cfg(test)]
mod tests {
    use super::rank;

    #[test]
    fn a_percentile_is_the_nearest_rank_and_none_for_nothing() {
        assert_eq!(rank(&[], 0.5), None);
        assert_eq!(rank(&[7], 0.95), Some(7));
        let hundred: Vec<i64> = (1..=100).collect();
        assert_eq!(rank(&hundred, 0.50), Some(50));
        assert_eq!(rank(&hundred, 0.95), Some(95));
    }
}
