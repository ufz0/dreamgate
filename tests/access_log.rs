// The access log on its own: the subscriber that reads it back is the global one, set before any request,
// because a subscriber set for one thread misses the callsites that requests on other threads reached first,
// which made this test fail now and then when it shared a binary with the rest of the suite.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use dreamgate::{build_router, db, limits, live, state::State};
use sqlx::sqlite::SqlitePoolOptions;
use tower::ServiceExt;

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn app() -> (Router, State) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    for statement in db::SCHEMA {
        sqlx::query(*statement).execute(&pool).await.unwrap();
    }
    let limits = limits::Limits::new(
        8,
        8,
        Duration::from_secs(limits::DEFAULT_REQUEST_TIMEOUT_SECS),
        Duration::from_secs(limits::DEFAULT_STREAM_STALL_SECS),
        Duration::from_secs(limits::DEFAULT_CONNECT_TIMEOUT_SECS),
    );
    let st = State {
        pool,
        http: limits.http_client(),
        hub: live::hub(),
        presence: Default::default(),
        attempts: Default::default(),
        limits: Arc::new(limits),
        metrics: Default::default(),
        shutdown: Default::default(),
    };
    (build_router(st.clone()), st)
}

// One line per request with the method, the path, the status and the time it took, a failure as much as a
// success. A key travels in a header and a value can travel in a query, so neither is written. The line is
// one event of its own target, so filtering the crate's other targets cannot leave it without its fields.
#[tokio::test]
async fn each_request_is_one_access_line_without_its_headers_or_query() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_ansi(false)
            .with_env_filter("warn,dreamgate::access=info")
            .finish(),
    )
    .unwrap();

    let (router, _) = app().await;
    let ok = router
        .oneshot(
            Request::get("/health?token=query-secret")
                .header("authorization", "Bearer header-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);

    let (router, st) = app().await;
    st.pool.close().await;
    let failed = router
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(failed.status(), StatusCode::SERVICE_UNAVAILABLE);

    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<&str> = log
        .lines()
        .filter(|line| line.contains("dreamgate::access"))
        .collect();
    assert_eq!(lines.len(), 2, "{log}");
    for (line, status) in lines.iter().zip(["status=200", "status=503"]) {
        for part in ["INFO", "method=GET", "path=/health", status, "latency_ms="] {
            assert!(line.contains(part), "{part} in {line}");
        }
    }
    assert!(!log.contains("query-secret"), "{log}");
    assert!(!log.contains("header-secret"), "{log}");
}
