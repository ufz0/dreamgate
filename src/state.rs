use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sqlx::sqlite::SqlitePool;
use tokio::sync::broadcast;

use crate::limits::Limits;
use crate::live::HubEvent;
use crate::metrics::Metrics;
use crate::presence::Presence;
use crate::reservations::Reservations;
use crate::shutdown::Shutdown;

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
    // What `/metrics` reports about this run of the process.
    pub metrics: Arc<Metrics>,
    // How far the process is on its way out, read by everything that has to stop taking work or end it.
    pub shutdown: Shutdown,
    // The requests each key and account has in flight, held against their limits until their rows are
    // written.
    pub reservations: Arc<Reservations>,
}
