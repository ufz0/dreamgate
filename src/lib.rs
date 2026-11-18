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
pub mod reservations;
pub mod settings;
pub mod shutdown;
pub mod state;
pub mod stats;
pub mod tokens;
pub mod usage;
pub mod users;

use axum::extract::DefaultBodyLimit;
use axum::{Router, middleware, routing};
use tower_http::services::ServeDir;

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
        .route("/stats/connections", routing::get(stats::connections))
        .route("/stats/activity", routing::get(stats::activity))
        .route("/stats/activity.csv", routing::get(stats::activity_export))
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
    // Each served path answers a method it does not take, and every other path under /v1, as an
    // OpenAI-compatible server does: with JSON a client can read rather than an empty answer.
    let gateway = Router::new()
        .route(
            "/v1/models",
            routing::get(gateway::list_models).fallback(gateway::method_not_allowed),
        )
        .route(
            "/v1/chat/completions",
            routing::post(gateway::chat_completions).fallback(gateway::method_not_allowed),
        )
        .route(
            "/v1/completions",
            routing::post(gateway::completions).fallback(gateway::method_not_allowed),
        )
        .route(
            "/v1/embeddings",
            routing::post(gateway::embeddings).fallback(gateway::method_not_allowed),
        )
        .route("/v1", routing::any(gateway::not_found))
        .route("/v1/", routing::any(gateway::not_found))
        .route("/v1/{*rest}", routing::any(gateway::not_found))
        .route_layer(middleware::from_fn_with_state(st.clone(), metrics::count));

    Router::new()
        .route("/health", routing::get(metrics::health))
        .route("/metrics", routing::get(metrics::scrape))
        .merge(gateway)
        .nest("/api", public.merge(private))
        .fallback_service(ServeDir::new("web"))
        .with_state(st)
        // The largest request body taken, which is axum's default, stated so that the README and the
        // proxy recipe in it do not change with an upgrade of axum.
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(middleware::from_fn(metrics::access_log))
}

pub const BODY_LIMIT: usize = 2 * 1024 * 1024;
