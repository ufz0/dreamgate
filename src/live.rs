use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::State as AxumState;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use futures_util::{StreamExt, stream};
use serde::Serialize;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::time::{Interval, MissedTickBehavior};

use crate::auth::Caller;
use crate::presence::{self, PresenceEvent};
use crate::shutdown::Phase;
use crate::state::{Hub, State};
use crate::stats;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// How often a page is told how far the streams in flight have got: often enough that a counter reads as
// moving, and rare enough that a page with nothing else to do is not kept busy repainting.
const PROGRESS_EVERY: Duration = Duration::from_millis(250);

// What one page's stream keeps between events: where it is in the hub, its tick, the version of the
// streams it last read, and the progress it was last sent.
struct Feed {
    st: State,
    receiver: broadcast::Receiver<HubEvent>,
    ticks: Interval,
    seen: Option<u64>,
    sent: Option<String>,
}

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
    // Whether the counts are the gateway's estimate because the upstream reported none. The row has one
    // flag for both, so the page marks the input and the output together, the way it marks a stream's
    // estimate, rather than showing an estimate as a number the upstream gave.
    pub estimated: bool,
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

    let feed = Feed {
        st,
        receiver,
        ticks,
        seen: None,
        sent: None,
    };

    let body =
        stream::once(
            async move { Ok::<_, BoxError>(Bytes::from(format!("data: {snapshot}\n\n"))) },
        )
        .chain(stream::unfold(feed, move |mut feed| async move {
            loop {
                let payload = tokio::select! {
                    // A page's live stream ends when the process starts to stop, because an open stream would
                    // hold the stop for as long as the page stays open; the page reconnects on its own.
                    biased;
                    _ = feed.st.shutdown.reached(Phase::Draining) => return None,
                    // The hub is read before the tick. A stream leaves the list in the same step that sends its
                    // row's event, so progress read without the stream has the row waiting here, and the row
                    // goes first.
                    event = feed.receiver.recv() => {
                        let Ok(event) = event else {
                            return None;
                        };
                        // Presence is a count, so it goes to everyone; usage is scoped to the account that owns it.
                        // Another account's row reaches a member only as its figures, because the platform total a
                        // member sees adds them up anyway: no key, account, model or row id, and the stream it
                        // settles so the page can take that stream's share out of the total.
                        match event {
                            HubEvent::Usage(usage) if scoped && usage.user_id != user_id => json!({
                                "type": "total",
                                "event": {
                                    "prompt_tokens": usage.prompt_tokens,
                                    "completion_tokens": usage.completion_tokens,
                                    "total_tokens": usage.total_tokens,
                                    "cached_tokens": usage.cached_tokens,
                                    "created_at": usage.created_at,
                                    "stream": usage.stream,
                                },
                            }),
                            HubEvent::Usage(usage) => json!({ "type": "usage", "event": usage }),
                            HubEvent::Presence(presence) => {
                                json!({ "type": "presence", "presence": presence })
                            }
                        }
                        .to_string()
                    }
                    _ = feed.ticks.tick() => {
                        let Some((version, payload)) =
                            presence::progress(&feed.st, scoped, user_id, feed.seen)
                        else {
                            continue;
                        };
                        // A row's event that arrived while the list was being read is sent first, and the
                        // progress is read again on the next tick, so it is never sent ahead of the row.
                        if !feed.receiver.is_empty() {
                            continue;
                        }
                        feed.seen = Some(version);
                        // The version moves with every account's streams, so a member's page can read the same
                        // progress twice; it is only sent when it differs from what this page was last sent.
                        let payload = payload.to_string();
                        if feed.sent.as_deref() == Some(payload.as_str()) {
                            continue;
                        }
                        feed.sent = Some(payload.clone());
                        payload
                    }
                };
                return Some((
                    Ok::<_, BoxError>(Bytes::from(format!("data: {payload}\n\n"))),
                    feed,
                ));
            }
        }));

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
