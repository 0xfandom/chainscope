//! The API's read-only data access.
//!
//! Its own queries against the shared schema, so the API stays independent of the
//! indexer binary (and its Kafka client). Read-only in intent — nothing here
//! writes.

use serde::Serialize;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use crate::error::ApiError;

/// Open the read pool.
pub async fn connect(url: &str, max_connections: u32) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await?;
    Ok(pool)
}

/// A cheap liveness probe.
pub async fn ping(pool: &PgPool) -> Result<(), ApiError> {
    sqlx::query("SELECT 1")
        .execute(pool)
        .await
        .map_err(|_| ApiError::Unavailable)?;
    Ok(())
}

/// Ingestion progress — the operational heartbeat.
#[derive(Debug, Serialize)]
pub struct Status {
    pub head_height: Option<i64>,
    pub finalized_height: Option<i64>,
    pub live_cursor: Option<i64>,
    pub backfill_cursor: Option<i64>,
    /// How far the live pipeline is behind the head, when both are known.
    pub lag: Option<i64>,
}

/// Read the singleton `chain_state` row.
pub async fn status(pool: &PgPool) -> Result<Status, ApiError> {
    let row = sqlx::query(
        "SELECT head_height, finalized_height, live_cursor, backfill_cursor
           FROM chain_state WHERE id = 1",
    )
    .fetch_one(pool)
    .await?;

    let head_height: Option<i64> = row.get("head_height");
    let live_cursor: Option<i64> = row.get("live_cursor");
    let lag = match (head_height, live_cursor) {
        (Some(h), Some(c)) => Some((h - c).max(0)),
        _ => None,
    };

    Ok(Status {
        head_height,
        finalized_height: row.get("finalized_height"),
        live_cursor,
        backfill_cursor: row.get("backfill_cursor"),
        lag,
    })
}
