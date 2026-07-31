//! Watchlist maintenance: keep the leaderboard live.
//!
//! The leaderboard is a materialised view and the wash flag is recomputed, not
//! toggled (M6). Neither updates itself — so on a timer this stage recomputes the
//! wash flags and refreshes the leaderboard snapshot, so the top-N watchlist the
//! alerter reads is current rather than frozen at the last manual run.
//!
//! It lives in the indexer because the indexer owns the write side and the
//! supervisor; the alerter is a separate process and only reads the result. Both
//! operations are recompute-from-current-state, so running them on every tick
//! just converges — a missed or repeated cycle changes nothing.

use std::time::Duration;

use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use crate::db::{self, WashParams};

pub struct MaintenanceTask {
    pool: PgPool,
    interval: Duration,
    wash: WashParams,
    cancel: CancellationToken,
}

impl MaintenanceTask {
    pub fn new(pool: PgPool, interval: Duration, cancel: CancellationToken) -> Self {
        Self {
            pool,
            interval,
            wash: WashParams::default(),
            cancel,
        }
    }

    /// One cycle: recompute wash flags first, then refresh the leaderboard so the
    /// snapshot already reflects the latest exclusions.
    pub async fn tick(&self) -> anyhow::Result<u64> {
        let excluded = db::flag_wash_trading(&self.pool, &self.wash).await?;
        db::refresh_leaderboard(&self.pool).await?;
        Ok(excluded)
    }

    /// Run until cancelled. A failed cycle logs and waits for the next tick — the
    /// watchlist being briefly stale is not worth bringing the process down.
    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!(interval_ms = self.interval.as_millis() as u64, "maintenance started");
        while !self.cancel.is_cancelled() {
            match self.tick().await {
                Ok(excluded) => tracing::debug!(excluded, "watchlist refreshed"),
                Err(e) => tracing::warn!(error = %e, "maintenance cycle failed; retrying next interval"),
            }
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
        tracing::info!("maintenance stopped");
        Ok(())
    }
}
