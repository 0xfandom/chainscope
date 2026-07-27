//! The backfill driver: index history, restartably, up to the finality floor.
//!
//! The live producer (M1 #6) only follows the head; nothing indexes the past.
//! Backfill is a separate concern with different rules, and keeping it a distinct
//! driver is what lets the live path stay simple:
//!
//!   * It is **bounded** — a fixed range `[start, floor]`, not an endless follow.
//!   * It **skips reorg logic entirely**. Everything at or below the finality
//!     floor is already final, so there is no fork to detect; the top of the
//!     range is pinned at `finalized_block` so backfill and the live follower
//!     never touch the same reorg-eligible blocks.
//!   * It records progress in `chain_state.backfill_cursor`, the *contiguous*
//!     done-prefix of history, so a restart re-runs only the incomplete tail.
//!
//! ## How a chunk becomes rows
//!
//! The adaptive chunker (#33) sweeps the range with `eth_getLogs`, which is the
//! cheap way to discover *which* blocks in a wide window actually emitted a log
//! for a pool we watch — usually a small handful even across thousands of blocks.
//! But `eth_getLogs` does not return block timestamps, and `block_time` is both
//! the partition key and the candle bucket, so it has to be the real value, not
//! one derived from the block number. So each active block is fetched once for
//! its authoritative header and logs, decoded through the same `decode_block`
//! the live transformer uses, and the resulting `RowBatch`es for the whole chunk
//! are committed together with the cursor advance — one transaction, all or
//! nothing, exactly as M2 established for the live path.
//!
//! A chunk is atomic: its rows and the cursor move together. If shutdown lands
//! mid-chunk the chunk is abandoned unwritten and the cursor is left where it
//! was, so the whole tail simply re-runs on the next start. The cursor is only
//! ever advanced *after* a chunk is fully written, which is what keeps the
//! done-prefix honest.
//!
//! This driver is sequential. Fanning disjoint sub-ranges across parallel
//! workers is a throughput optimisation the `backfill_cursor` was designed to
//! allow (it tracks the low-water contiguous mark, not the highest worker), but
//! it is not needed for correctness and is left for later; a sequential sweep
//! already satisfies exactly-once, restartability and the finality bound.
//!
//! The bulk `COPY` write path and the stream-then-discard window gating are #35;
//! this uses the per-row insert path so #34 can prove the driver's contract
//! against a recent range whose day partitions already exist.

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Instant,
};

use chainscope_core::{source::ChainSource, types::Address20};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use crate::{chunker::LogChunker, db, transformer::decode_block};

/// The first block to fetch, given the stored cursor and the configured start.
///
/// A stored cursor always wins — that is what makes a restart resume rather than
/// repeat — and resumes at the block *after* it, since the cursor names the last
/// block already done. Pulled out as a pure function so the resume rule is
/// testable without a database. `Some(0)` correctly resumes at 1: block 0 was
/// done, so genesis is not re-fetched.
fn resume_from(cursor: Option<u64>, start_block: u64) -> u64 {
    match cursor {
        Some(done) => done + 1,
        None => start_block,
    }
}

pub struct BackfillDriver {
    source: Arc<dyn ChainSource>,
    pool: PgPool,
    /// Pools plus the factory. A block is "active" when it emitted a log from one
    /// of these; a log from anything else never reaches decoding.
    watched: HashSet<Address20>,
    start_block: u64,
    chunk_size: u64,
    /// Raw retention floor in unix seconds (#35): a block older than this has its
    /// rows folded into the aggregates but not stored raw. `None` keeps every
    /// raw row — the default until the candle aggregator makes discarding safe.
    window_floor: Option<i64>,
    cancel: CancellationToken,
}

impl BackfillDriver {
    pub fn new(
        source: Arc<dyn ChainSource>,
        pool: PgPool,
        watched: impl IntoIterator<Item = Address20>,
        start_block: u64,
        chunk_size: u64,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            source,
            pool,
            watched: watched.into_iter().collect(),
            start_block,
            chunk_size,
            window_floor: None,
            cancel,
        }
    }

    /// Set the raw-retention floor (unix seconds): blocks older than this keep no
    /// raw rows, only aggregates. Defaults to no floor (keep everything).
    pub fn with_window_floor(mut self, floor: Option<i64>) -> Self {
        self.window_floor = floor;
        self
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let cursor = db::load_backfill_cursor(&self.pool).await?;
        let start = resume_from(cursor, self.start_block);

        // The top of the range: the finality floor. Backfill never indexes a
        // reorg-eligible block, so it never fights the live follower.
        let floor = self.source.finalized_block().await?;
        if start > floor {
            tracing::info!(
                start,
                floor,
                "backfill: nothing below the finality floor left to do"
            );
            return Ok(());
        }
        tracing::info!(start, floor, span = floor - start + 1, "backfill started");

        let mut chunker = LogChunker::new(Arc::clone(&self.source), start, floor, self.chunk_size);
        let mut committed_blocks: u64 = 0;
        let mut persisted_rows: u64 = 0;
        let mut discarded_rows: u64 = 0;

        loop {
            if self.cancel.is_cancelled() {
                tracing::info!(
                    committed_blocks,
                    persisted_rows,
                    discarded_rows,
                    "backfill stopped (cancelled)"
                );
                return Ok(());
            }

            let chunk = match chunker.next_chunk().await {
                Ok(Some(c)) => c,
                Ok(None) => {
                    tracing::info!(
                        committed_blocks,
                        persisted_rows,
                        discarded_rows,
                        "backfill complete"
                    );
                    return Ok(());
                }
                // Transient has already been through the failover pool; Fatal is a
                // real problem. Either way, adapting the window will not help — the
                // supervisor surfaces it and a restart resumes from the cursor.
                Err(e) => return Err(e.into()),
            };

            // Which blocks in this chunk actually emitted a watched log. Small
            // even for a wide chunk, because most blocks touch none of our pools.
            let active: BTreeSet<u64> = chunk.logs.iter().map(|l| l.block_number).collect();

            let mut batches = Vec::with_capacity(active.len());
            for number in &active {
                if self.cancel.is_cancelled() {
                    // Abandon the chunk unwritten. The cursor is untouched, so the
                    // whole tail from `start` re-runs next time — never advance
                    // over a block we did not write.
                    tracing::info!("backfill cancelled mid-chunk; abandoning it unwritten");
                    return Ok(());
                }
                let unit = self.source.fetch_block(*number).await?;
                let (batch, _unknown) = decode_block(&unit, &self.watched);
                batches.push(batch);
            }

            // One transaction: the chunk's rows and the cursor advance to the
            // chunk's top move together via bulk COPY. The cursor goes to
            // `chunk.to`, not the last active block, so the empty blocks in the
            // chunk are recorded as done and are not re-scanned. Rows older than
            // the retention window are folded into aggregates (#36) but not stored
            // raw.
            let started = Instant::now();
            let stats =
                db::bulk_write_backfill(&self.pool, &batches, chunk.to, self.window_floor, false)
                    .await?;
            let elapsed = started.elapsed();

            committed_blocks += active.len() as u64;
            persisted_rows += stats.persisted;
            discarded_rows += stats.discarded;
            tracing::info!(
                from = chunk.from,
                to = chunk.to,
                active = active.len(),
                persisted = stats.persisted,
                discarded = stats.discarded,
                width = chunk.width(),
                duration_ms = elapsed.as_millis() as u64,
                "backfill chunk committed (bulk COPY)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_prefers_the_cursor_over_the_configured_start() {
        // Cursor names the last done block; resume at the next one.
        assert_eq!(resume_from(Some(50), 10), 51);
        // No cursor: start where configuration says.
        assert_eq!(resume_from(None, 100), 100);
        // Genesis done means resume at 1, not re-fetch 0.
        assert_eq!(resume_from(Some(0), 100), 1);
        // No cursor and no configured start: block 0.
        assert_eq!(resume_from(None, 0), 0);
    }
}
