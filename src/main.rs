use std::path::Path;
use std::time::Duration;

use dreamgate::{build_router, db, limits, live, registry, state};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database =
        std::env::var("DREAMGATE_DB").unwrap_or_else(|_| "data/dreamgate.db".to_string());
    if let Some(parent) = Path::new(&database).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    // A number that does not fit in a u32 is not a pool, so the largest one is used.
    let pool_size = limits::env_number("DREAMGATE_DB_POOL", db::DEFAULT_POOL_SIZE as usize)
        .min(u32::MAX as usize) as u32;
    let pool = db::connect(&database, pool_size).await?;

    let limits = limits::from_env();
    // The request timeout is what it was, but both are named here so they can be set apart. Reqwest's timeout
    // is one deadline from the moment a request starts connecting until its body has finished, so the connect
    // timeout is a cap inside it: a connection that cannot be made should be given up on in seconds, not in
    // the time an inference is allowed to take, because a slot held while waiting to connect is a slot held
    // away from other requests. The number of connections kept idle is bounded by the default per-connection
    // limit, because one client is shared by every connection and cannot know what an individual one stated;
    // keeping more than a connection can take at once is memory for connections nothing uses.
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(limits::env_number(
            "DREAMGATE_CONNECT_TIMEOUT_SECS",
            10,
        ) as u64))
        .timeout(Duration::from_secs(
            limits::env_number("DREAMGATE_REQUEST_TIMEOUT_SECS", 120) as u64,
        ))
        .pool_max_idle_per_host(limits.default_limit())
        .build()?;

    let st = state::State {
        pool,
        http,
        hub: live::hub(),
        presence: Default::default(),
        attempts: Default::default(),
        limits: std::sync::Arc::new(limits),
    };

    tokio::spawn(registry::health_checker(st.clone()));

    let addr = std::env::var("DREAMGATE_ADDR").unwrap_or_else(|_| "0.0.0.0:8787".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound = listener.local_addr()?;
    println!("DreamGate listening on {bound}");
    println!("  gateway:   POST /v1/chat/completions (OpenAI compatible)");
    println!("  dashboard: http://127.0.0.1:{}/", bound.port());

    axum::serve(listener, build_router(st)).await?;
    Ok(())
}
