//! New-pool sniffer: turn the factory's `PoolCreated` into a captured pool.
//!
//! The factory is already in the watched set, so its `PoolCreated` logs already
//! flow through the pipeline — but the transformer drops them (`Ignored`), since
//! a fresh pool is not a swap or a liquidity event to store. This separate stage
//! fetches recent factory logs on a timer and records each new pool as a
//! `pools` row (`is_indexed = false`), with a small risk scorecard. Idempotent —
//! `ON CONFLICT DO NOTHING` — so a rescanned window captures nothing twice, and
//! the stage needs no cursor.
//!
//! Scope note: the USD value of a pool's first liquidity needs the tokens'
//! decimals, which we do not fetch at discovery, so the risk heuristics here use
//! liquidity *presence* signals rather than a dollar figure. Enriching
//! `first_liquidity_usd` once token metadata is fetched is a follow-up.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chainscope_core::source::{ChainSource, SourceError};
use chainscope_core::types::{Address20, RawLog};
use chainscope_eth_source::{decode, DecodedEvent};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use crate::db;

/// How many recent blocks each tick rescans. Wider than a poll gap so nothing
/// slips through; overlap is harmless because capture is idempotent.
const SNIFFER_WINDOW: u64 = 250;

/// A conservative risk scorecard from what we know at discovery. Not a verdict —
/// a set of flags to eyeball.
pub fn risk_flags(has_liquidity: bool, distinct_lps: usize) -> serde_json::Value {
    let mut flags = vec!["fresh"];
    if !has_liquidity {
        flags.push("no_liquidity");
    } else if distinct_lps <= 1 {
        flags.push("single_lp");
    }
    serde_json::json!({ "flags": flags })
}

pub struct Sniffer {
    source: Arc<dyn ChainSource>,
    pool: PgPool,
    factory: Address20,
    interval: Duration,
    cancel: CancellationToken,
}

impl Sniffer {
    pub fn new(
        source: Arc<dyn ChainSource>,
        pool: PgPool,
        factory: Address20,
        interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            source,
            pool,
            factory,
            interval,
            cancel,
        }
    }

    /// Fetch the recent window and capture any new pools in it.
    pub async fn tick(&self) -> anyhow::Result<usize> {
        let head = self.source.latest_block().await?;
        let from = head.saturating_sub(SNIFFER_WINDOW);
        let logs = self.source.fetch_logs(from, head).await?;
        self.capture_from(&logs).await
    }

    /// Decode `PoolCreated` from the factory's logs and upsert each pool, reading
    /// any first-liquidity signals from `Mint`s to that pool in the same window.
    pub async fn capture_from(&self, logs: &[RawLog]) -> anyhow::Result<usize> {
        let mut captured = 0;
        for log in logs {
            if log.address != self.factory {
                continue;
            }
            let Some(DecodedEvent::PoolCreated(pc)) = decode(log) else {
                continue;
            };
            let pool_addr = pc.pool.into_array();

            // Distinct liquidity providers that minted into this pool in the window.
            let lps: HashSet<Address20> = logs
                .iter()
                .filter_map(|l| match decode(l) {
                    Some(DecodedEvent::Mint(m)) if l.address == pool_addr => Some(m.owner.into_array()),
                    _ => None,
                })
                .collect();
            let flags = risk_flags(!lps.is_empty(), lps.len());

            let new = db::capture_new_pool(
                &self.pool,
                &pool_addr,
                &pc.token0.into_array(),
                &pc.token1.into_array(),
                pc.fee.to::<u32>() as i32,
                pc.tickSpacing.as_i32(),
                log.block_number as i64,
                &flags,
            )
            .await?;
            if new {
                captured += 1;
                tracing::info!(pool = %hex::encode(pool_addr), "new pool captured");
            }
        }
        Ok(captured)
    }

    /// Poll until cancelled. A source hiccup logs and waits; discovery is not
    /// worth bringing the process down.
    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!("sniffer started");
        while !self.cancel.is_cancelled() {
            match self.tick().await {
                Ok(n) if n > 0 => tracing::info!(captured = n, "new pools"),
                Ok(_) => {}
                Err(e) => {
                    if matches!(e.downcast_ref::<SourceError>(), Some(SourceError::Fatal(_))) {
                        return Err(e);
                    }
                    tracing::warn!(error = %e, "sniffer poll failed; retrying next interval");
                }
            }
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
        tracing::info!("sniffer stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::risk_flags;

    #[test]
    fn risk_reflects_liquidity() {
        assert_eq!(risk_flags(false, 0)["flags"], serde_json::json!(["fresh", "no_liquidity"]));
        assert_eq!(risk_flags(true, 1)["flags"], serde_json::json!(["fresh", "single_lp"]));
        assert_eq!(risk_flags(true, 3)["flags"], serde_json::json!(["fresh"]));
    }
}
