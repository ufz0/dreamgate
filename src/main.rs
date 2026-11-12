use std::path::Path;

use dreamgate::{build_router, db, live, registry, state};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database = std::env::var("DREAMGATE_DB").unwrap_or_else(|_| "data/dreamgate.db".to_string());
    if let Some(parent) = Path::new(&database).parent() && !parent.as_os_str().is_empty() {
        std::fs::create_dir_all(parent)?;
    }
    let pool = db::connect(&database).await?;

    let st = state::State {
        pool,
        http: reqwest::Client::builder().timeout(std::time::Duration::from_secs(120)).build()?,
        hub: live::hub(),
        presence: Default::default(),
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
