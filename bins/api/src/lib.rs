//! chainscope read API.
//!
//! An axum service over the indexed data. Split into a lib so tests can build the
//! router and drive it through `tower`'s `oneshot` without binding a socket, the
//! same lib/bin split the indexer uses.

use std::time::Duration;

use axum::routing::get;
use axum::Router;
use sqlx::postgres::PgPool;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub mod config;
pub mod db;
pub mod dto;
pub mod error;
pub mod handlers;
pub mod pagination;
pub mod util;

/// Shared, cheaply-cloned state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

/// Build the router. Kept separate from `main` so tests construct the exact app
/// the binary serves.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/status", get(handlers::status))
        .route("/pools", get(handlers::list_pools))
        .route("/pools/{address}", get(handlers::get_pool))
        .route("/pools/{address}/swaps", get(handlers::pool_swaps))
        // A request that outlives this budget is a slow query, not a hang; cut it.
        .layer(TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(10),
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
