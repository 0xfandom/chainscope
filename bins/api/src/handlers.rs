//! HTTP handlers.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;

use crate::db;
use crate::error::ApiError;
use crate::AppState;

/// Liveness + readiness: 200 when the database answers, 503 when it does not.
pub async fn healthz(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    db::ping(&state.pool).await?;
    Ok(StatusCode::OK)
}

/// Ingestion progress: head, finalized, cursors and the lag to head.
pub async fn status(State(state): State<AppState>) -> Result<Json<db::Status>, ApiError> {
    Ok(Json(db::status(&state.pool).await?))
}
