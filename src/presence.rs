use std::collections::HashMap;
use std::time::Instant;

use axum::extract::{State as AxumState};
use axum::response::{IntoResponse, Json, Response};
use serde::Serialize;
use serde_json::json;
use sqlx::Row;

use crate::auth::Caller;
use crate::live::HubEvent;
use crate::state::State;

// A user is online while they have done something through the gateway in the last five
// minutes, and generating while a request of theirs is in flight. Presence lives in the
// process, so it is empty until traffic arrives and is cleared by a restart.
pub const ONLINE_WINDOW_SECS: u64 = 300;

#[derive(Clone, Serialize)]
pub struct PresenceEvent {
    pub online: usize,
    pub generating: usize,
    pub generating_ids: Vec<i64>,
}

#[derive(Default)]
pub struct Presence {
    generating: HashMap<i64, usize>,
    last_seen: HashMap<i64, Instant>,
}

impl Presence {
    fn snapshot(&self) -> PresenceEvent {
        let now = Instant::now();
        let generating = self.generating.iter().filter(|(_, count)| **count > 0).map(|(id, _)| *id).collect::<Vec<_>>();
        PresenceEvent {
            online: self.last_seen.values().filter(|seen| now.duration_since(**seen).as_secs() < ONLINE_WINDOW_SECS).count(),
            generating: generating.len(),
            generating_ids: generating,
        }
    }
}

// Held for the whole request, including the part after the handler returns for a stream.
pub struct Active {
    st: State,
    user: i64,
}

pub fn track(st: &State, user: i64) -> Active {
    count(st, user, 1);
    Active { st: st.clone(), user }
}

impl Drop for Active {
    fn drop(&mut self) {
        count(&self.st, self.user, -1);
    }
}

fn count(st: &State, user: i64, delta: i64) {
    let mut presence = st.presence.lock().expect("presence lock");
    let entry = presence.generating.entry(user).or_insert(0);
    *entry = if delta > 0 {
        *entry + delta as usize
    } else {
        entry.saturating_sub((-delta) as usize)
    };
    presence.last_seen.insert(user, Instant::now());
    let _ = st.hub.send(HubEvent::Presence(presence.snapshot()));
}

pub async fn summary(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    let present = {
        let presence = st.presence.lock().expect("presence lock");
        presence.snapshot()
    };

    // Usernames are only shown to an admin, matching how the rest of the stats are scoped.
    let users = if caller.role == "admin" {
        let rows = sqlx::query("SELECT id, username FROM users")
            .fetch_all(&st.pool)
            .await
            .unwrap_or_default();
        let names: HashMap<i64, String> = rows
            .iter()
            .map(|row| (row.get::<i64, _>("id"), row.get::<String, _>("username")))
            .collect();
        present
            .generating_ids
            .iter()
            .map(|id| json!({ "id": id, "username": names.get(id).cloned().unwrap_or_else(|| format!("account {id}")) }))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    Json(json!({
        "online": present.online,
        "generating": present.generating,
        "generating_ids": present.generating_ids,
        "users": users,
    }))
    .into_response()
}
