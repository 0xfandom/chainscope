//! HTTP handlers.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use crate::dto::{CandleDto, Page, PoolDto, RealizedTradeDto, ScorecardDto, SwapDto};
use crate::error::ApiError;
use crate::pagination::{clamp_limit, decode_bucket, decode_cursor};
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

/// Candle query parameters.
#[derive(Debug, Deserialize)]
pub struct CandleParams {
    /// One of `1m`, `1h`, `1d`. Defaults to `1m`.
    pub resolution: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

/// A pool's OHLCV candles at one resolution, keyset-paginated, newest-first.
pub async fn pool_candles(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<CandleParams>,
) -> Result<Json<Page<CandleDto>>, ApiError> {
    let addr = parse_address(&address)?;
    let resolution = params.resolution.as_deref().unwrap_or("1m");
    let before = decode_bucket(&params.cursor)?;
    let limit = clamp_limit(params.limit);
    Ok(Json(
        db::candles_page(&state.pool, &addr, resolution, before, limit).await?,
    ))
}

/// A wallet's PnL scorecard: rollup, open positions and a recent realised trail.
pub async fn wallet_scorecard(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<ScorecardDto>, ApiError> {
    let addr = parse_address(&address)?;
    db::wallet_scorecard(&state.pool, &addr)
        .await?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

/// A wallet's realised trades, keyset-paginated, newest-first.
pub async fn wallet_trades(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(params): Query<PageParams>,
) -> Result<Json<Page<RealizedTradeDto>>, ApiError> {
    let addr = parse_address(&address)?;
    let after = decode_cursor(&params.cursor)?;
    let limit = clamp_limit(params.limit);
    Ok(Json(db::wallet_trades_page(&state.pool, &addr, after, limit).await?))
}
