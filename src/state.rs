use sqlx::sqlite::SqlitePool;
use tokio::sync::broadcast;

use crate::live::LiveEvent;

pub type Hub = broadcast::Sender<LiveEvent>;

#[derive(Clone)]
pub struct State {
    pub pool: SqlitePool,
    pub http: reqwest::Client,
    pub hub: Hub,
}
