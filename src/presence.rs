use std::collections::HashMap;
use std::time::Instant;

use axum::extract::State as AxumState;
use axum::response::{IntoResponse, Json, Response};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::Row;

use crate::auth::Caller;
use crate::live::{HubEvent, LiveEvent};
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
    streams: HashMap<u64, Progress>,
    next_stream: u64,
    // Moves on every change to the streams, so a page can be told only when there is something new
    // without comparing the streams themselves.
    version: u64,
}

// A stream in flight and how far it has got. The counts are the gateway's own estimate, the same one a
// row is given when nothing is reported, until the upstream reports its own, which most only do in their
// last chunk; `estimated` says which of the two the generated count is, and `prompt_estimated` says the
// same of the input, because an upstream can report the input before it reports what it generated.
#[derive(Clone, Serialize)]
pub struct Progress {
    pub stream: u64,
    pub user_id: i64,
    pub key: String,
    pub model: String,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub estimated: bool,
    pub prompt_estimated: bool,
    pub started_at: String,
}

// What anyone signed in is told about a stream, whichever account it belongs to: how much it has
// generated, which is already part of the platform total they see, and the stream it is, so the row that
// settles it can take its share out of that total. Nothing in it says whose stream it is.
#[derive(Serialize)]
struct Share {
    stream: u64,
    completion_tokens: i64,
    estimated: bool,
}

// Held by a stream task while it reads, so a page can show a generation as it happens rather than only
// once its row is written. The task settles it with the row's event, which takes the stream off the list
// and sends the event in one step; dropping it without that, as a panic does, only takes it off.
pub struct InFlight {
    st: State,
    stream: u64,
}

pub fn start_stream(
    st: &State,
    user_id: i64,
    key: &str,
    model: &str,
    prompt_tokens: i64,
) -> InFlight {
    let mut presence = st.presence.lock().expect("presence lock");
    presence.next_stream += 1;
    let stream = presence.next_stream;
    presence.streams.insert(
        stream,
        Progress {
            stream,
            user_id,
            key: key.to_string(),
            model: model.to_string(),
            prompt_tokens,
            completion_tokens: 0,
            estimated: true,
            prompt_estimated: true,
            started_at: crate::clock::now(),
        },
    );
    presence.version += 1;
    InFlight {
        st: st.clone(),
        stream,
    }
}

impl InFlight {
    pub fn stream(&self) -> u64 {
        self.stream
    }

    pub fn update(
        &self,
        prompt_tokens: i64,
        completion_tokens: i64,
        prompt_estimated: bool,
        estimated: bool,
    ) {
        let mut guard = self.st.presence.lock().expect("presence lock");
        let presence = &mut *guard;
        let Some(progress) = presence.streams.get_mut(&self.stream) else {
            return;
        };
        let next = (
            prompt_tokens,
            completion_tokens,
            prompt_estimated,
            estimated,
        );
        if (
            progress.prompt_tokens,
            progress.completion_tokens,
            progress.prompt_estimated,
            progress.estimated,
        ) != next
        {
            (
                progress.prompt_tokens,
                progress.completion_tokens,
                progress.prompt_estimated,
                progress.estimated,
            ) = next;
            presence.version += 1;
        }
    }

    // A page reads the list under the same lock, so it sees either the stream or the row's event waiting
    // for it, never neither and never both: the figure neither dips while the row is written nor counts the
    // two together. A row that could not be written has no event, and the stream only leaves the list.
    pub fn settle(self, event: Option<LiveEvent>) {
        let mut presence = self.st.presence.lock().expect("presence lock");
        if presence.streams.remove(&self.stream).is_some() {
            presence.version += 1;
        }
        if let Some(event) = event {
            let _ = self.st.hub.send(HubEvent::Usage(event));
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut presence = self.st.presence.lock().expect("presence lock");
        if presence.streams.remove(&self.stream).is_some() {
            presence.version += 1;
        }
    }
}

// What a page is sent about the streams in flight, or nothing when they have not changed since `seen`.
// Every stream's share goes to anyone signed in, because together they are part of the platform total;
// the streams themselves, with their key and model, are scoped like the rest of the usage, every account's
// for an admin and otherwise the caller's own. The list is copied out so the lock is not held while it is
// written as JSON.
pub fn progress(st: &State, scoped: bool, user_id: i64, seen: Option<u64>) -> Option<(u64, Value)> {
    let (version, mut all) = {
        let presence = st.presence.lock().expect("presence lock");
        if seen == Some(presence.version) {
            return None;
        }
        (
            presence.version,
            presence.streams.values().cloned().collect::<Vec<_>>(),
        )
    };
    all.sort_by_key(|p| p.stream);
    let shares = all
        .iter()
        .map(|p| Share {
            stream: p.stream,
            completion_tokens: p.completion_tokens,
            estimated: p.estimated,
        })
        .collect::<Vec<_>>();
    let streams = all
        .into_iter()
        .filter(|p| !scoped || p.user_id == user_id)
        .collect::<Vec<_>>();
    Some((
        version,
        json!({ "type": "progress", "shares": shares, "streams": streams }),
    ))
}

impl Presence {
    fn snapshot(&self) -> PresenceEvent {
        let now = Instant::now();
        let generating = self
            .generating
            .iter()
            .filter(|(_, count)| **count > 0)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        PresenceEvent {
            online: self
                .last_seen
                .values()
                .filter(|seen| now.duration_since(**seen).as_secs() < ONLINE_WINDOW_SECS)
                .count(),
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
    Active {
        st: st.clone(),
        user,
    }
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
