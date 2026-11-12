use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sqlx::sqlite::SqlitePool;
use tokio::sync::broadcast;

use crate::live::HubEvent;
use crate::presence::Presence;

pub type Hub = broadcast::Sender<HubEvent>;

#[derive(Clone)]
pub struct State {
    pub pool: SqlitePool,
    pub http: reqwest::Client,
    pub hub: Hub,
    pub presence: Arc<Mutex<Presence>>,
    // Sign-in attempts per account, one window at a time. In the process rather than the
    // database, because it bounds a loop and does not need to survive a restart.
    pub attempts: Arc<Mutex<HashMap<String, (usize, Instant)>>>,
}
