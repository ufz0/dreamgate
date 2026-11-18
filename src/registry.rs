use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::api::{error, server_error};
use crate::auth::Caller;
use crate::state::State;

#[derive(Clone)]
pub struct Connection {
    pub id: i64,
    pub name: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub default_model: Option<String>,
    pub enabled: bool,
    pub priority: i64,
    pub health: String,
    pub last_checked_at: Option<String>,
    // What the connection says it can take at once. Null means it never said, which is not the same as a
    // limit of one: an unstated capability is admitted by the default rather than by a guess.
    pub max_concurrent: Option<i64>,
    // The models the connection listed at its last check that could read its list. None means no list has been
    // read, which is not the same as a list with nothing in it.
    pub models: Option<Vec<Listed>>,
}

// A model as a connection listed it: its id, and the time it was made when the upstream gave one, which the
// merged model list passes on rather than leave out a field a typed client requires or make one up.
#[derive(Clone, Serialize, Deserialize)]
pub struct Listed {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created: Option<i64>,
}

impl Connection {
    // Whether the connection's list was read and names the model.
    pub fn lists(&self, model: &str) -> bool {
        self.models
            .as_ref()
            .is_some_and(|models| models.iter().any(|listed| same_model(&listed.id, model)))
    }

    // Whether the connection's list was read and does not name the model. A list that was never read says
    // nothing about what the connection serves, so it never counts against it.
    pub fn lists_without(&self, model: &str) -> bool {
        self.models.is_some() && !self.lists(model)
    }
}

// Ollama lists a model under its tag and answers to its name without one, which it reads as `:latest`, so the
// two are the same model here.
pub fn same_model(listed: &str, asked: &str) -> bool {
    listed == asked
        || listed.strip_suffix(":latest") == Some(asked)
        || asked.strip_suffix(":latest") == Some(listed)
}

// The models in an OpenAI-shaped list. A body in any other shape, or with an entry that has no id to read, is not
// a list of nothing or of some of its models but one that was not read, so it gives none: a list read in part
// would move a connection back for the models it left out.
fn listed_models(body: &[u8]) -> Option<Vec<Listed>> {
    let body: Value = serde_json::from_slice(body).ok()?;
    body.get("data")?
        .as_array()?
        .iter()
        .map(|entry| {
            Some(Listed {
                id: entry.get("id")?.as_str()?.to_string(),
                created: entry.get("created").and_then(Value::as_i64),
            })
        })
        .collect()
}

fn row_to_connection(row: &sqlx::sqlite::SqliteRow) -> Connection {
    Connection {
        id: row.get::<i64, _>("id"),
        name: row.get::<String, _>("name"),
        base_url: row.get::<String, _>("base_url"),
        api_key: row.get::<Option<String>, _>("api_key"),
        default_model: row.get::<Option<String>, _>("default_model"),
        enabled: row.get::<i64, _>("enabled") != 0,
        priority: row.get::<i64, _>("priority"),
        health: row.get::<String, _>("health"),
        last_checked_at: row.get::<Option<String>, _>("last_checked_at"),
        max_concurrent: row.get::<Option<i64>, _>("max_concurrent"),
        models: row
            .get::<Option<String>, _>("models")
            .and_then(|text| serde_json::from_str(&text).ok()),
    }
}

pub fn endpoint(base_url: &str, path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/v1") {
        format!("{base}/{path}")
    } else {
        format!("{base}/v1/{path}")
    }
}

// The health each enabled connection's last check left, for a scrape of `/metrics`. A connection that is
// turned off is left out, because no check reaches it and what it holds is stale.
pub async fn health_readings(st: &State) -> Result<Vec<(i64, String, String)>, sqlx::Error> {
    let rows =
        sqlx::query("SELECT id, name, health FROM connections WHERE enabled = 1 ORDER BY id")
            .fetch_all(&st.pool)
            .await?;
    Ok(rows
        .iter()
        .map(|row| (row.get("id"), row.get("name"), row.get("health")))
        .collect())
}

// Writes down that a connection failed a request, for its figures on the Connections view, and gives back the
// row's id so the request can mark it as passed on if it goes on to another connection. A failure that cannot be
// written is logged and otherwise let go: the request it belongs to is answered all the same.
pub async fn record_failure(st: &State, connection_id: i64, kind: &str) -> Option<i64> {
    let written = sqlx::query(
        "INSERT INTO connection_failures (connection_id, kind, created_at) VALUES (?, ?, ?) RETURNING id",
    )
    .bind(connection_id)
    .bind(kind)
    .bind(crate::clock::now())
    .fetch_one(&st.pool)
    .await;
    match written {
        Ok(row) => Some(row.get::<i64, _>("id")),
        Err(err) => {
            tracing::warn!("a connection's failure could not be written down: {err}");
            None
        }
    }
}

// The request went on to another connection after these failures, which is what a failover is.
pub async fn passed_on(st: &State, failures: &[i64]) {
    for id in failures {
        if let Err(err) = sqlx::query("UPDATE connection_failures SET passed_on = 1 WHERE id = ?")
            .bind(id)
            .execute(&st.pool)
            .await
        {
            tracing::warn!("a failover could not be written down: {err}");
        }
    }
}

pub async fn candidates(st: &State, preferred: Option<i64>) -> Vec<Connection> {
    let rows = sqlx::query(
        "SELECT id, name, base_url, api_key, default_model, enabled, priority, health, last_checked_at, \
         max_concurrent, models FROM connections WHERE enabled = 1 ORDER BY priority ASC, id ASC",
    )
    .fetch_all(&st.pool)
    .await
    .unwrap_or_default();
    let mut all: Vec<Connection> = rows.iter().map(row_to_connection).collect();
    if let Some(id) = preferred {
        let position = all.iter().position(|c| c.id == id);
        if let Some(index) = position {
            let preferred_conn = all.remove(index);
            all.insert(0, preferred_conn);
        }
    }
    // A connection whose last check failed is put last rather than left out. Health is only checked
    // every 30 seconds, so one marked down may be answering again, and leaving out every connection
    // that is marked down would send a request nowhere when all of them are. The sort is stable, so
    // the connections that are not down keep their order — the key's own connection first, then
    // priority — and the ones that are down follow in that same order. Every gateway endpoint reads
    // this one order: a request tries each in turn, after moving back the ones whose list does not
    // name its model, and the model list names each model after the first connection that lists it.
    all.sort_by_key(|conn| conn.health == "down");
    all
}

pub async fn check(st: &State, connection: &Connection) -> StatusCode {
    let url = endpoint(&connection.base_url, "models");
    let result = st
        .http
        .get(&url)
        .header(
            "Authorization",
            format!("Bearer {}", connection.api_key.clone().unwrap_or_default()),
        )
        .timeout(Duration::from_secs(10))
        .send()
        .await;
    // The body is read apart from what it says: one that could not be read, because the connection broke off
    // or went quiet while sending it, leaves the last list as it was, and only one that was read and is in
    // another shape is stored as no list.
    let (status, body) = match result {
        Ok(resp) => {
            let status = resp.status();
            let body = if status.is_success() {
                resp.bytes().await.ok()
            } else {
                None
            };
            (status, body)
        }
        Err(_) => (StatusCode::BAD_GATEWAY, None),
    };
    if !status.is_success() {
        // A connection that did not answer keeps the list it gave last: it is tried after the others while it
        // is down, and when it answers again it most likely serves what it served before.
        let _ =
            sqlx::query("UPDATE connections SET health = 'down', last_checked_at = ? WHERE id = ?")
                .bind(crate::clock::now())
                .bind(connection.id)
                .execute(&st.pool)
                .await;
        return status;
    }
    let Some(body) = body else {
        let _ =
            sqlx::query("UPDATE connections SET health = 'ok', last_checked_at = ? WHERE id = ?")
                .bind(crate::clock::now())
                .bind(connection.id)
                .execute(&st.pool)
                .await;
        return status;
    };
    let models = listed_models(&body).and_then(|models| serde_json::to_string(&models).ok());
    let _ = sqlx::query(
        "UPDATE connections SET health = 'ok', last_checked_at = ?, models = ? WHERE id = ?",
    )
    .bind(crate::clock::now())
    .bind(models)
    .bind(connection.id)
    .execute(&st.pool)
    .await;
    status
}

#[derive(Deserialize)]
pub struct CreateConnection {
    pub name: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub default_model: Option<String>,
    pub priority: Option<i64>,
    // What the connection can take at once. Left out means it never said, and the default limit applies.
    pub max_concurrent: Option<i64>,
}

pub async fn list(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    let rows = sqlx::query(
        "SELECT id, name, base_url, api_key, default_model, enabled, priority, health, last_checked_at, \
         max_concurrent, models FROM connections ORDER BY priority ASC, id ASC",
    )
    .fetch_all(&st.pool)
    .await
    .unwrap_or_default();
    // An upstream key is a secret, so it is returned to an admin alone, and so is the address: it
    // names an internal host and port, and can carry a credential of its own in its query or its
    // user part. The rest of the row is what any signed-in account needs for the gate readout and
    // the model list.
    let admin = caller.role == "admin";
    let out: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let c = row_to_connection(row);
            let api_key = if admin { c.api_key.clone() } else { None };
            let base_url = if admin {
                Some(c.base_url.clone())
            } else {
                None
            };
            json!({
                "id": c.id,
                "name": c.name,
                "base_url": base_url,
                "api_key": api_key,
                "default_model": c.default_model,
                "enabled": c.enabled,
                "priority": c.priority,
                "health": c.health,
                "last_checked_at": c.last_checked_at,
                "max_concurrent": c.max_concurrent,
                // The ids alone: the page shows how many and which, and the dates are for the model list.
                "models": c.models.as_ref().map(|models| {
                    models.iter().map(|listed| listed.id.clone()).collect::<Vec<_>>()
                }),
            })
        })
        .collect();
    Json(json!({ "connections": out })).into_response()
}

pub async fn create(
    AxumState(st): AxumState<State>,
    caller: Caller,
    Json(body): Json<CreateConnection>,
) -> Response {
    if caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "only an admin can add a connection",
        );
    }
    let name = body.name.trim().to_string();
    let base_url = body.base_url.trim().to_string();
    if name.is_empty() || base_url.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "name and base_url are required",
        );
    }
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "base_url must start with http:// or https://",
        );
    }
    // A capability of zero would admit nothing, which is not what leaving it out means, so a stated
    // capability has to be a real one.
    if body.max_concurrent.is_some_and(|limit| limit < 1) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "max_concurrent must be positive",
        );
    }
    let row = sqlx::query(
        "INSERT INTO connections (name, base_url, api_key, default_model, priority, max_concurrent, \
         created_at) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(&name)
    .bind(&base_url)
    .bind(&body.api_key)
    .bind(&body.default_model)
    .bind(body.priority.unwrap_or(0))
    .bind(body.max_concurrent)
    .bind(crate::clock::now())
    .fetch_one(&st.pool)
    .await;
    match row {
        Ok(row) => Json(json!({ "id": row.get::<i64, _>("id") })).into_response(),
        // A name that is taken is named rather than answered with the driver text. The name is the
        // only unique column on connections, so a unique violation here can only mean that; a failure
        // for any other reason is not dressed up as a taken name.
        Err(sqlx::Error::Database(db)) if db.is_unique_violation() => error(
            StatusCode::CONFLICT,
            "invalid_request",
            format!("a connection named {name} already exists"),
        ),
        Err(err) => server_error(err),
    }
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
            "only an admin can remove a connection",
        );
    }
    let result = sqlx::query("DELETE FROM connections WHERE id = ?")
        .bind(id)
        .execute(&st.pool)
        .await;
    match result {
        Ok(_) => Json(json!({ "ok": true })).into_response(),
        Err(err) => server_error(err),
    }
}

pub async fn health_check(
    AxumState(st): AxumState<State>,
    Path(id): Path<i64>,
    caller: Caller,
) -> Response {
    if caller.role != "admin" {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "only an admin can check a connection",
        );
    }
    let row = sqlx::query(
        "SELECT id, name, base_url, api_key, default_model, enabled, priority, health, last_checked_at, \
         max_concurrent, models FROM connections WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&st.pool)
    .await;
    let Ok(Some(row)) = row else {
        return error(StatusCode::NOT_FOUND, "not_found", "connection not found");
    };
    let connection = row_to_connection(&row);
    let status = check(&st, &connection).await;
    Json(json!({ "id": connection.id, "health": if status.is_success() { "ok" } else { "down" } }))
        .into_response()
}

// A round of health checks used to take one connection at a time, so a connection that does not answer held
// the health of every other one behind its own timeout. They run at once now, bounded by a small number,
// because a health check is still a request to a connection that may already be working and the round should
// not read as traffic. A slow connection can only delay its own row now.
const MAX_HEALTH_CHECKS_AT_ONCE: usize = 8;

// The rounds run for the life of the process and nothing waits on them, so a panic in one would end
// health checking without a word: every connection would keep the health it had, and one marked down
// would stay at the back of the order. A round that stops is started again after the usual wait instead,
// and the reason is logged. It runs in a JoinSet so that dropping this task, as a test does, takes the
// round with it.
pub async fn health_checker(st: State) {
    loop {
        let mut round = JoinSet::new();
        round.spawn(rounds(st.clone()));
        if let Some(Err(err)) = round.join_next().await {
            tracing::error!("the health check round stopped and is started again: {err}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}

async fn rounds(st: State) {
    loop {
        let rows = sqlx::query(
            "SELECT id, name, base_url, api_key, default_model, enabled, priority, health, last_checked_at, \
             max_concurrent, models FROM connections WHERE enabled = 1",
        )
        .fetch_all(&st.pool)
        .await
        .unwrap_or_default();

        let gate = Arc::new(Semaphore::new(MAX_HEALTH_CHECKS_AT_ONCE));
        let mut running = JoinSet::new();
        for row in &rows {
            let connection = row_to_connection(row);
            let gate = gate.clone();
            let st = st.clone();
            running.spawn(async move {
                let Ok(_slot) = gate.acquire_owned().await else {
                    return;
                };
                let _ = check(&st, &connection).await;
            });
        }
        while running.join_next().await.is_some() {}
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    }
}
