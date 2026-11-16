use axum::body::{Body, Bytes};
use axum::extract::State as AxumState;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use futures_util::{StreamExt, stream};
use std::time::Duration;

use serde::Serialize;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;

use crate::auth::Caller;
use crate::presence::{self, PresenceEvent};
use crate::state::{Hub, State};
use crate::stats;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// How often a page is told how far the streams in flight have got: often enough that a counter reads as
// moving, and rare enough that a page with nothing else to do is not kept busy repainting.
pub const PROGRESS_EVERY: Duration = Duration::from_millis(250);

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
    // The cache figure is carried so the split on the page moves with the row that arrives; the
    // averages are not, because an average belongs to the stored rows rather than to one event.
    pub cached_tokens: Option<i64>,
    pub latency_ms: i64,
    pub status: i64,
    pub streamed: bool,
    pub created_at: String,
    // The stream this row settles, when it was one; the page drops that stream's progress as it adds the
    // row, so the two are never counted together.
    pub stream: Option<u64>,
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
    let global = stats::totals(&st, false, user_id)
        .await
        .unwrap_or(json!({}));
    let snapshot =
        json!({ "type": "snapshot", "scoped": scoped, "self": self_totals, "global": global })
            .to_string();
    let receiver = st.hub.subscribe();
    // The progress of the streams in flight is read on a tick rather than sent through the hub: a stream
    // changes with every chunk, and the hub would carry each change to every page, so its rate would grow
    // with the number of streams. Read on a tick, a page gets at most four updates a second however many
    // streams are running, and none while nothing changes. A tick that falls behind is not made up.
    let mut ticks = tokio::time::interval(PROGRESS_EVERY);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let body = stream::once(async move {
        Ok::<_, BoxError>(Bytes::from(format!("data: {snapshot}\n\n")))
    })
    .chain(stream::unfold(
        (receiver, ticks, None::<u64>),
        move |(mut receiver, mut ticks, mut seen)| {
            let st = st.clone();
            async move {
                loop {
                    let payload = tokio::select! {
                        event = receiver.recv() => {
                            let Ok(event) = event else {
                                return None;
                            };
                            // Presence is a count, so it goes to everyone; usage is scoped to the account
                            // that owns it. Skipping an event must not end the stream for the client.
                            match event {
                                HubEvent::Usage(usage) if scoped && usage.user_id != user_id => continue,
                                HubEvent::Usage(usage) => json!({ "type": "usage", "event": usage }),
                                HubEvent::Presence(presence) => {
                                    json!({ "type": "presence", "presence": presence })
                                }
                            }
                        }
                        _ = ticks.tick() => match presence::progress(&st, scoped, user_id, seen) {
                            Some((version, payload)) => {
                                seen = Some(version);
                                payload
                            }
                            None => continue,
                        },
                    };
                    return Some((
                        Ok::<_, BoxError>(Bytes::from(format!("data: {payload}\n\n"))),
                        (receiver, ticks, seen),
                    ));
                }
            }
        },
    ));

    let mut out = Response::new(Body::empty());
    out.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    out.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    *out.body_mut() = Body::from_stream(body);
    out.into_response()
}
