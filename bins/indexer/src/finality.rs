//! The finality tracker: keep the line between reorg-eligible and frozen.
//!
//! Ethereum only rewrites its recent history. A block deep enough past the tip
//! is *finalised* — consensus can never reorganise it away — while the blocks
//! just behind the head are still provisional and can be replaced by a reorg.
//! Everything else in M4 turns on knowing where that line is: reorg detection
//! (#39) walks backwards only as far as it, and the backfill (M3) ignores reorgs
//! entirely precisely because it works below it.
//!
//! This stage does one small thing on a timer: read the tip and its finality
//! line from the chain, fold them into `chain_state` monotonically, and prune
//! the `blocks` header window down to the still-reorg-eligible band. It writes
//! only the finality columns; the live cursor stays the writer's alone.
//!
//! It is a separate task rather than a hook in the producer because the producer
//! is deliberately database-free — "a second chain is a second impl, not a
//! second architecture", and the same is true of a second reader of the head.
//! Keeping finality here means the producer never grows a pool.

use std::{sync::Arc, time::Duration};

use chainscope_core::source::{ChainSource, SourceError};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use crate::db::{self, FinalityUpdate};

pub struct FinalityTracker {
    source: Arc<dyn ChainSource>,
    pool: PgPool,
    poll_interval: Duration,
    cancel: CancellationToken,
}

impl FinalityTracker {
    pub fn new(
        source: Arc<dyn ChainSource>,
        pool: PgPool,
        poll_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            source,
            pool,
            poll_interval,
            cancel,
        }
    }

    /// One observation: read the tip and its finality line, fold both into
    /// `chain_state`, and prune the now-finalised headers. Returns what changed.
    ///
    /// `finalized` is clamped to `head` before it is stored: a provider that
    /// briefly reports a finalised block ahead of the tip it also reports must
    /// not be allowed to poison the monotonic maximum with a value the chain
    /// will later contradict.
    pub async fn tick(&self) -> anyhow::Result<FinalityUpdate> {
        let head = self.source.latest_block().await?;
        let finalized = self.source.finalized_block().await?.min(head);
        db::advance_finality(&self.pool, head, finalized).await
    }

    /// Poll until cancelled.
    ///
    /// A source hiccup is not fatal here: the tier is advisory between polls, so
    /// a transient failure logs and waits for the next interval rather than
    /// bringing the pipeline down. A `Fatal` source error still surfaces — it
    /// means the source is misconfigured, which the producer would hit too and
    /// which an operator must see.
    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!("finality tracker started");

        while !self.cancel.is_cancelled() {
            match self.tick().await {
                Ok(u) => tracing::debug!(
                    finalized = u.finalized_height,
                    pruned = u.headers_pruned,
                    "finality advanced"
                ),
                Err(e) => {
                    if matches!(e.downcast_ref::<SourceError>(), Some(SourceError::Fatal(_))) {
                        return Err(e);
                    }
                    tracing::warn!(error = %e, "finality poll failed; retrying next interval");
                }
            }

            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(self.poll_interval) => {}
            }
        }

        tracing::info!("finality tracker stopped");
        Ok(())
    }
}
