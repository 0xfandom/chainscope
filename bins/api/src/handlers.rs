//! HTTP handlers.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use crate::dto::{Page, PoolDto, SwapDto};
use crate::error::ApiError;
use crate::pagination::{clamp_limit, decode_cursor};
use crate::util::parse_address;
use crate::{db, AppState};

/// Liveness + readiness: 200 when the database answers, 503 when it does not.
pub async fn healthz(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    db::ping(&state.pool).await?;
    Ok(StatusCode::OK)
}

/// Ingestion progress: head, finalized, cursors and the lag to head.
pub async fn status(State(state): State<AppState>) -> Result<Json<db::Status>, ApiError> {
    Ok(Json(db::status(&state.pool).await?))
}

/// The indexed pools.
pub async fn list_pools(State(state): State<AppState>) -> Result<Json<Vec<PoolDto>>, ApiError> {
    Ok(Json(db::list_pools(&state.pool).await?))
}

/// One pool by address.
pub async fn get_pool(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<PoolDto>, ApiError> {
    let addr = parse_address(&address)?;
    db::get_pool(&state.pool, &addr).await?.map(Json).ok_or(ApiError::NotFound)
}

/// Query parameters shared by keyset-paginated endpoints.
#[derive(Debug, Deserialize)]
pub struct PageParams {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

/// A pool's swaps, keyset-paginated, newest-first.
pub async fn pool_swaps(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<Json<Page<SwapDto>>, ApiError> {
    let addr = parse_address(&address)?;
    let after = decode_cursor(&params.cursor)?;
    let limit = clamp_limit(params.limit);
    Ok(Json(db::swaps_page(&state.pool, &addr, after, limit).await?))
}
