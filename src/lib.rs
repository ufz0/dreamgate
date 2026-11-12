pub mod api;
pub mod auth;
pub mod clock;
pub mod db;
pub mod gateway;
pub mod keys;
pub mod live;
pub mod registry;
pub mod state;
pub mod stats;
pub mod tokens;
pub mod usage;
pub mod users;

use axum::{middleware, routing, Router};
use tower_http::services::ServeDir;

use crate::state::State;

pub fn build_router(st: State) -> Router {
    // Registering and signing in cannot require a session.
    let public = Router::new()
        .route("/register", routing::post(auth::register))
        .route("/login", routing::post(auth::login));

    let private = Router::new()
        .route("/logout", routing::post(auth::logout))
        .route("/me", routing::get(auth::me))
        .route("/stats/summary", routing::get(stats::summary))
        .route("/stats/recent", routing::get(stats::recent))
        .route("/live", routing::get(live::stream_events))
        .route("/keys", routing::get(keys::list).post(keys::create))
        .route("/keys/{id}", routing::patch(keys::update).delete(keys::delete))
        .route("/users/{id}", routing::patch(users::set_cap))
        .route("/connections", routing::get(registry::list).post(registry::create))
        .route("/connections/{id}", routing::delete(registry::remove))
        .route("/connections/{id}/check", routing::post(registry::health_check))
        .layer(middleware::from_fn_with_state(st.clone(), auth::require_session));

    Router::new()
        .route("/health", routing::get(|| async { "ok" }))
        .route("/v1/models", routing::get(gateway::list_models))
        .route("/v1/chat/completions", routing::post(gateway::chat_completions))
        .nest("/api", public.merge(private))
        .fallback_service(ServeDir::new("web"))
        .with_state(st)
}
