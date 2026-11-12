use std::sync::{Arc, Mutex};

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
}
