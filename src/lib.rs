pub mod api;
pub mod auth;
pub mod clock;
pub mod db;
pub mod gateway;
pub mod keys;
pub mod limits;
pub mod live;
pub mod metrics;
pub mod presence;
pub mod registry;
pub mod settings;
pub mod state;
pub mod stats;
pub mod tokens;
pub mod usage;
pub mod users;

use axum::body::Body;
use axum::http::Request;
use axum::{Router, middleware, routing};
use tower_http::services::ServeDir;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::state::State;

pub fn build_router(st: State) -> Router {
    // Registering, signing in and reading whether registration is open cannot require a
    // session, because the entry screen decides before anyone is signed in.
    let public = Router::new()
        .route("/register", routing::post(auth::register))
        .route("/login", routing::post(auth::login))
        .route("/settings", routing::get(settings::get));

    let private = Router::new()
        .route("/logout", routing::post(auth::logout))
        .route("/me", routing::get(auth::me))
        .route("/settings", routing::patch(settings::update))
        .route("/presence", routing::get(presence::summary))
        .route("/stats/summary", routing::get(stats::summary))
        .route("/stats/recent", routing::get(stats::recent))
        .route("/live", routing::get(live::stream_events))
        .route("/keys", routing::get(keys::list).post(keys::create))
        .route("/keys/others", routing::get(keys::other_accounts))
        .route(
            "/keys/{id}",
            routing::patch(keys::update).delete(keys::delete),
        )
        .route("/users", routing::post(users::create))
        .route(
            "/users/{id}",
            routing::patch(users::update).delete(users::remove),
        )
        .route(
            "/connections",
            routing::get(registry::list).post(registry::create),
        )
        .route("/connections/{id}", routing::delete(registry::remove))
        .route(
            "/connections/{id}/check",
            routing::post(registry::health_check),
        )
        .layer(middleware::from_fn_with_state(
            st.clone(),
            auth::require_session,
        ));

    // Every answer the gateway endpoints give is counted, refusals included, because a refusal is
    // deliberately not a usage row and would otherwise be counted nowhere.
    let gateway = Router::new()
        .route("/v1/models", routing::get(gateway::list_models))
        .route(
            "/v1/chat/completions",
            routing::post(gateway::chat_completions),
        )
        .route_layer(middleware::from_fn_with_state(st.clone(), metrics::count));

    // One line per request at info: the method, the path without its query, the status and how long it
    // took. Nothing from the headers, so a key or a session cookie is never written to the log, and no
    // separate line for a failure, because the status is already on the line.
    let access_log = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<Body>| {
            tracing::info_span!(
                "request",
                method = %request.method(),
                path = %request.uri().path()
            )
        })
        .on_response(DefaultOnResponse::new().level(Level::INFO))
        .on_failure(());

    Router::new()
        .route("/health", routing::get(metrics::health))
        .route("/metrics", routing::get(metrics::scrape))
        .merge(gateway)
        .nest("/api", public.merge(private))
        .fallback_service(ServeDir::new("web"))
        .with_state(st)
        .layer(access_log)
}
