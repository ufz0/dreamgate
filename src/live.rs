use axum::body::{Body, Bytes};
use axum::extract::{State as AxumState};
use axum::http::{header, HeaderValue};
use axum::response::{IntoResponse, Response};
use futures_util::{stream, StreamExt};
use serde::Serialize;
use serde_json::json;
use tokio::sync::broadcast;

use crate::auth::Caller;
use crate::presence::PresenceEvent;
use crate::state::{Hub, State};
use crate::stats;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Serialize)]
pub enum HubEvent {
    Usage(LiveEvent),
    Presence(PresenceEvent),
}

#[derive(Clone, Serialize)]
pub struct LiveEvent {
    pub id: i64,
    pub user_id: i64,
    pub key: String,
    pub user: String,
    pub model: String,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub latency_ms: i64,
    pub status: i64,
    pub streamed: bool,
    pub created_at: String,
}

pub fn hub() -> Hub {
    broadcast::channel(256).0
}

pub async fn stream_events(AxumState(st): AxumState<State>, caller: Caller) -> Response {
    let scoped = caller.role != "admin";
    let user_id = caller.id;
    // The page shows both: the account's own figures and the total across every account, so
    // the snapshot has to carry both rather than one role-dependent set.
    let self_totals = stats::totals(&st, true, user_id).await.unwrap_or(json!({}));
    let global = stats::totals(&st, false, user_id).await.unwrap_or(json!({}));
    let snapshot = json!({ "type": "snapshot", "scoped": scoped, "self": self_totals, "global": global }).to_string();
    let receiver = st.hub.subscribe();

    let body = stream::once(async move { Ok::<_, BoxError>(Bytes::from(format!("data: {snapshot}\n\n"))) }).chain(
        stream::unfold(receiver, move |mut receiver| async move {
            loop {
                let event = match receiver.recv().await {
                    Ok(event) => event,
                    Err(_) => return None,
                };
                // Presence is a count, so it goes to everyone; usage is scoped to the account
                // that owns it. Skipping an event must not end the stream for the client.
                match &event {
                    HubEvent::Usage(usage) if scoped && usage.user_id != user_id => continue,
                    HubEvent::Usage(usage) => {
                        let payload = json!({ "type": "usage", "event": usage }).to_string();
                        return Some((Ok::<_, BoxError>(Bytes::from(format!("data: {payload}\n\n"))), receiver));
                    }
                    HubEvent::Presence(presence) => {
                        let payload = json!({ "type": "presence", "presence": presence }).to_string();
                        return Some((Ok::<_, BoxError>(Bytes::from(format!("data: {payload}\n\n"))), receiver));
                    }
                }
            }
        }),
    );

    let mut out = Response::new(Body::empty());
    out.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    out.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    *out.body_mut() = Body::from_stream(body);
    out.into_response()
}
