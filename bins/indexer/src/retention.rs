//! Retention: keep the disk footprint flat.
//!
//! On a timer, roll the candles up (1m→1h→1d) and then drop the raw day
//! partitions that have fallen out of the window. Downsample *before* prune, so
//! a day's history is compressed into the coarser candles before the raw rows
//! behind it are dropped — never the other way round, which would lose it.
//!
//! A separate supervised stage, like finality and maintenance, because it is
//! periodic database housekeeping the ingest path should not carry.

use std::path::PathBuf;
use std::time::Duration;

use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use crate::db;

pub struct RetentionTask {
    pool: PgPool,
    retain_days: i64,
    /// When set, partitions are streamed to CSV here before being dropped.
    dump_dir: Option<PathBuf>,
    interval: Duration,
    cancel: CancellationToken,
}

impl RetentionTask {
    pub fn new(
        pool: PgPool,
        retain_days: i64,
        dump_dir: Option<PathBuf>,
        interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            pool,
            retain_days,
            dump_dir,
            interval,
            cancel,
        }
    }

    /// One cycle: roll candles up, then drop out-of-window raw partitions.
    pub async fn tick(&self) -> anyhow::Result<Vec<String>> {
        db::downsample(&self.pool).await?;
        db::prune_raw_partitions(&self.pool, self.retain_days, self.dump_dir.as_deref()).await
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!(retain_days = self.retain_days, "retention started");
        while !self.cancel.is_cancelled() {
            match self.tick().await {
                Ok(dropped) if !dropped.is_empty() => {
                    tracing::info!(dropped = ?dropped, "raw partitions pruned")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "retention cycle failed; retrying next interval"),
            }
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
        tracing::info!("retention stopped");
        Ok(())
    }
}
