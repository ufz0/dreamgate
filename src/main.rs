use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;

use dreamgate::{build_router, db, limits, live, metrics, registry, state};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Coloured only on a terminal, and not when NO_COLOR asks for none: written to a file or to the
    // journal, the colour codes are noise in every line.
    let colour = std::io::IsTerminal::is_terminal(&std::io::stdout())
        && std::env::var_os("NO_COLOR").is_none_or(|value| value.is_empty());
    tracing_subscriber::fmt()
        .with_env_filter(log_filter(std::env::var("RUST_LOG").ok().as_deref()))
        .with_ansi(colour)
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
        metrics: std::sync::Arc::new(metrics::Metrics::new(
            std::env::var("DREAMGATE_METRICS_TOKEN").ok(),
        )),
    };

    tokio::spawn(registry::health_checker(st.clone()));

    let addr = std::env::var("DREAMGATE_ADDR").unwrap_or_else(|_| "0.0.0.0:8787".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound = listener.local_addr()?;
    println!("DreamGate listening on {bound}");
    println!("  gateway:   POST /v1/chat/completions (OpenAI compatible)");
    println!("  dashboard: http://{}/", dashboard_address(bound));

    axum::serve(listener, build_router(st)).await?;
    Ok(())
}

// A bind to every interface names no host to connect to: Windows refuses that address, and browsers block
// it even where the system would connect, so the line names loopback, which the bind includes. Loopback is
// taken from the bound family because a [::] bind can be IPv6 only (the default on Windows, and on Linux
// with bindv6only set), where 127.0.0.1 would not answer. A specific address is the only one that answers,
// so it is named as it is.
fn dashboard_address(bound: SocketAddr) -> SocketAddr {
    if !bound.ip().is_unspecified() {
        return bound;
    }
    let loopback: IpAddr = if bound.is_ipv4() {
        Ipv4Addr::LOCALHOST.into()
    } else {
        Ipv6Addr::LOCALHOST.into()
    };
    SocketAddr::new(loopback, bound.port())
}

// Left to the subscriber, an unset RUST_LOG prints errors alone, so a usage row that could not be
// written, which the key caps and daily limits then do not count either, and a stream closed for going
// quiet were never printed. The default is info instead: the warnings print, and the one line a request
// writes at info is its access line; the libraries write nothing at info on every request. RUST_LOG
// decides when it is set, in its usual form; a value that is empty, or has nothing in it that can be
// read, leaves the default.
fn log_filter(rust_log: Option<&str>) -> EnvFilter {
    EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse_lossy(rust_log.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_dashboard_address_is_one_that_answers() {
        for (bound, named) in [
            ("0.0.0.0:8787", "127.0.0.1:8787"),
            ("[::]:8787", "[::1]:8787"),
            ("127.0.0.1:8787", "127.0.0.1:8787"),
            ("[::1]:8787", "[::1]:8787"),
            ("10.0.0.22:8787", "10.0.0.22:8787"),
        ] {
            let bound: SocketAddr = bound.parse().unwrap();
            assert_eq!(
                dashboard_address(bound).to_string(),
                named,
                "bound to {bound}"
            );
        }
    }

    #[test]
    fn the_log_filter_is_info_unless_rust_log_names_another() {
        for (rust_log, filter) in [
            (None, "info"),
            (Some(""), "info"),
            (Some("dreamgate=loud"), "info"),
            (Some("warn"), "warn"),
            (Some("debug"), "debug"),
            (Some("dreamgate=trace"), "dreamgate=trace"),
        ] {
            assert_eq!(
                log_filter(rust_log).to_string(),
                filter,
                "RUST_LOG={rust_log:?}"
            );
        }
    }
}
