use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sqlx::sqlite::SqlitePool;
use tokio::sync::broadcast;

use crate::limits::Limits;
use crate::live::HubEvent;
use crate::presence::Presence;

pub type Hub = broadcast::Sender<HubEvent>;

#[derive(Clone)]
pub struct State {
    pub pool: SqlitePool,
    pub http: reqwest::Client,
    pub hub: Hub,
    // Both are guarded with a plain mutex rather than a tokio one: the critical section is a few
    // instructions and never spans an await, so there is nothing to hold across, and the presence count
    // is released from a Drop, which cannot await.
    pub presence: Arc<Mutex<Presence>>,
    // Sign-in attempts per account, one window at a time. In the process rather than the
    // database, because it bounds a loop and does not need to survive a restart.
    pub attempts: Arc<Mutex<HashMap<String, (usize, Instant)>>>,
    // Admission for the gateway as a whole and for each connection, so one full connection cannot make
    // unrelated requests wait.
    pub limits: Arc<Limits>,
}
