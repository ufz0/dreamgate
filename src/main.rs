use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;

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

    let limits = std::sync::Arc::new(limits::from_env());
    // Built from the limits, with no total deadline: see the note on `limits::Limits::http_client`.
    let http = limits.http_client();

    let st = state::State {
        pool,
        http,
        hub: live::hub(),
        presence: Default::default(),
        attempts: Default::default(),
        limits,
    };

    tokio::spawn(registry::health_checker(st.clone()));

    let addr = std::env::var("DREAMGATE_ADDR").unwrap_or_else(|_| "0.0.0.0:8787".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound = listener.local_addr()?;
    println!("DreamGate listening on {bound}");
    println!("  gateway:   POST /v1/chat/completions (OpenAI compatible)");
    // An address that binds every interface is not one a browser can open, so the line names loopback of
    // the same family for it. A specific address is the only one that answers, so it is named as it is.
    let dashboard = if bound.ip().is_unspecified() {
        let loopback: IpAddr = if bound.is_ipv4() {
            Ipv4Addr::LOCALHOST.into()
        } else {
            Ipv6Addr::LOCALHOST.into()
        };
        SocketAddr::new(loopback, bound.port())
    } else {
        bound
    };
    println!("  dashboard: http://{dashboard}/");

    axum::serve(listener, build_router(st)).await?;
    Ok(())
}
