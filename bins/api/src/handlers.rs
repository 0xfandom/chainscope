//! HTTP handlers.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
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

/// Render cached JSON bytes as an `application/json` response.
fn json_response(body: Arc<Vec<u8>>) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], (*body).clone()).into_response()
}

/// Prometheus scrape: ingestion progress, lag, and the disk footprint, computed
/// per scrape from `chain_state` and `pg_total_relation_size`.
pub async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    use prometheus::{Encoder, Gauge, Registry, TextEncoder};

    let status = db::status(&state.pool).await?;
    let fp = db::footprint(&state.pool).await?;

    let reg = Registry::new();
    let gauge = |name: &str, help: &str, value: f64| {
        let g = Gauge::new(name, help).unwrap();
        g.set(value);
        let _ = reg.register(Box::new(g));
    };
    gauge("chainscope_head_height", "chain tip last observed", status.head_height.unwrap_or(0) as f64);
    gauge("chainscope_finalized_height", "highest finalized block", status.finalized_height.unwrap_or(0) as f64);
    gauge("chainscope_live_cursor", "highest block written by the live pipeline", status.live_cursor.unwrap_or(0) as f64);
    gauge("chainscope_ingest_lag", "blocks behind the head", status.lag.unwrap_or(0) as f64);
    gauge("chainscope_footprint_raw_bytes", "on-disk bytes of raw events", fp.raw_bytes as f64);
    gauge("chainscope_footprint_aggregate_bytes", "on-disk bytes of aggregates", fp.aggregate_bytes as f64);

    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&reg.gather(), &mut buf)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!(e)))?;
    Ok((
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        buf,
    )
        .into_response())
}

/// Ingestion progress: head, finalized, cursors and the lag to head. Cached — it
/// is small, shared, and polled far more often than it changes.
pub async fn status(State(state): State<AppState>) -> Result<Response, ApiError> {
    let pool = state.pool.clone();
    let body = state
        .cache
        .get_or_compute("status", || async move {
            let s = db::status(&pool).await?;
            serde_json::to_vec(&s).map_err(|e| ApiError::Internal(e.into()))
        })
        .await?;
    Ok(json_response(body))
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

/// Query params for limit-only endpoints.
#[derive(Debug, Deserialize)]
pub struct LimitParams {
    pub limit: Option<u32>,
}

/// The smart-money watchlist: top wallets by realised PnL, wash-excluded.
pub async fn leaderboard(
    State(state): State<AppState>,
    Query(params): Query<LimitParams>,
) -> Result<Response, ApiError> {
    let limit = clamp_limit(params.limit);
    let pool = state.pool.clone();
    let body = state
        .cache
        .get_or_compute(&format!("leaderboard:{limit}"), || async move {
            let rows = db::leaderboard(&pool, limit).await?;
            serde_json::to_vec(&rows).map_err(|e| ApiError::Internal(e.into()))
        })
        .await?;
    Ok(json_response(body))
}

/// Recently discovered pools, keyset-paginated on discovery time.
pub async fn new_pools(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> Result<Json<Page<crate::dto::NewPoolDto>>, ApiError> {
    let before = decode_bucket(&params.cursor)?;
    let limit = clamp_limit(params.limit);
    Ok(Json(db::new_pools_page(&state.pool, before, limit).await?))
}
