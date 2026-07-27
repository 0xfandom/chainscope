//! An `eth_getLogs` fetch that adapts its window to what the provider will serve.
//!
//! Backfill reads logs across millions of blocks, and `eth_getLogs` limits differ
//! wildly — by provider, and by how busy the range is. A fixed window size is
//! wrong in both directions: too small wastes a round trip per handful of blocks,
//! too large is rejected outright on a dense stretch and never makes progress.
//!
//! So the window adapts. It starts at the configured target width; when the
//! provider rejects a request as too wide it **halves and retries the same
//! start**, bisecting until the request is accepted; on success it grows back
//! toward the target. A busy stretch shrinks the window automatically and a quiet
//! stretch lets it widen again.
//!
//! This is the whole reason [`SourceError::RangeTooLarge`] is a distinct variant
//! (M1 #5): it means "the request must change", separate from
//! [`SourceError::Transient`] ("retry the same request") and
//! [`SourceError::BlockNotFound`] ("ask for a different block"). Only
//! `RangeTooLarge` bisects here; a transient failure is left to propagate to the
//! caller's retry/backoff, and the failover pool (#32) has usually already
//! rotated past a flaky endpoint before it ever reaches this code.
//!
//! ## What the chunker guarantees
//!
//! Windows are contiguous and non-overlapping: each accepted window is
//! `[next, hi]` and the next one starts at `hi + 1`. Bisection only ever shrinks
//! the *current* window's upper edge and retries the same lower edge, so no block
//! is fetched twice and none is skipped. Every log in `[from, to]` is therefore
//! yielded exactly once, in ascending order. An empty range, or a range that
//! simply contains no logs, still completes — an empty result is a valid answer,
//! not an error.

use std::sync::Arc;

use chainscope_core::{
    source::{ChainSource, SourceError},
    RawLog,
};

/// One accepted window of the sweep: an inclusive block range and the logs in it.
///
/// The range is carried alongside the logs because the backfill driver (#34)
/// needs to know which blocks a chunk *covers* to advance its cursor — an empty
/// `logs` over a real range still means "these blocks are done".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub from: u64,
    pub to: u64,
    pub logs: Vec<RawLog>,
}

impl Chunk {
    /// Number of blocks this chunk covers. Handy for logging the effective
    /// window width as it adapts.
    pub fn width(&self) -> u64 {
        self.to - self.from + 1
    }
}

/// A cursor over `[from, to]` that yields one accepted [`Chunk`] at a time,
/// adapting its window to the provider.
pub struct LogChunker {
    source: Arc<dyn ChainSource>,
    /// Next block not yet covered. Advances only past an accepted window.
    next: u64,
    /// Inclusive end of the sweep.
    end: u64,
    /// Current window width. Shrinks on rejection, grows back toward target.
    width: u64,
    /// Ceiling the window grows back toward after a shrink.
    target_width: u64,
}

impl LogChunker {
    /// Sweep the inclusive range `[from, to]`, starting at `target_width` blocks
    /// per request. A `target_width` of zero is treated as one — a window must
    /// cover at least a block.
    pub fn new(source: Arc<dyn ChainSource>, from: u64, to: u64, target_width: u64) -> Self {
        let target = target_width.max(1);
        Self {
            source,
            next: from,
            end: to,
            width: target,
            target_width: target,
        }
    }

    /// The current window width, exposed for logging and tests so the adaptation
    /// is observable.
    pub fn current_width(&self) -> u64 {
        self.width
    }

    /// Fetch the next accepted window, or `None` once the whole range is covered.
    ///
    /// Bisects on `RangeTooLarge` and grows back on success. Any other error —
    /// transient or fatal — propagates unchanged; adapting the window would not
    /// help either.
    pub async fn next_chunk(&mut self) -> Result<Option<Chunk>, SourceError> {
        if self.next > self.end {
            return Ok(None);
        }

        loop {
            let hi = self.next.saturating_add(self.width - 1).min(self.end);

            match self.source.fetch_logs(self.next, hi).await {
                Ok(logs) => {
                    let from = self.next;
                    self.next = hi + 1;
                    // Grow back toward the target for the next window. Geometric
                    // recovery: a stretch that forced a shrink widens again as
                    // soon as it thins out, without overshooting the ceiling.
                    self.width = self.width.saturating_mul(2).min(self.target_width);
                    return Ok(Some(Chunk { from, to: hi, logs }));
                }

                Err(SourceError::RangeTooLarge { .. }) => {
                    if self.width <= 1 {
                        // Already down to a single block and still rejected: the
                        // provider will not serve even one block's logs. Bisection
                        // has no move left, so surface it rather than loop forever.
                        return Err(SourceError::RangeTooLarge {
                            from: self.next,
                            to: hi,
                        });
                    }
                    self.width = (self.width / 2).max(1);
                    // Retry the same `next` with the narrower window.
                }

                // Transient goes to the caller's backoff; Fatal / BlockNotFound
                // are not made better by a different window.
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chainscope_core::types::{BlockUnit, Hash32};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A source that rejects a query returning more than `max_logs` results —
    /// which is how real providers cap `eth_getLogs`, by result count. Logs sit
    /// at the block numbers in `logs_at`, one per block, and each log's
    /// `log_index` is set to its block number so a test can recover exactly which
    /// blocks were covered.
    struct DensitySource {
        logs_at: BTreeSet<u64>,
        max_logs: usize,
        queries: AtomicUsize,
    }

    impl DensitySource {
        fn new(logs_at: impl IntoIterator<Item = u64>, max_logs: usize) -> Arc<Self> {
            Arc::new(Self {
                logs_at: logs_at.into_iter().collect(),
                max_logs,
                queries: AtomicUsize::new(0),
            })
        }

        fn query_count(&self) -> usize {
            self.queries.load(Ordering::SeqCst)
        }
    }

    fn log_for(block: u64) -> RawLog {
        RawLog {
            address: [0u8; 20],
            topics: vec![],
            data: vec![],
            block_number: block,
            tx_hash: [0u8; 32],
            // block number encoded here too so the coverage test can read it back
            // without depending on the fields under test.
            log_index: block as u32,
        }
    }

    #[async_trait]
    impl ChainSource for DensitySource {
        async fn latest_block(&self) -> Result<u64, SourceError> {
            unreachable!()
        }
        async fn finalized_block(&self) -> Result<u64, SourceError> {
            unreachable!()
        }
        async fn fetch_block(&self, _: u64) -> Result<BlockUnit, SourceError> {
            unreachable!()
        }
        async fn fetch_logs(&self, from: u64, to: u64) -> Result<Vec<RawLog>, SourceError> {
            self.queries.fetch_add(1, Ordering::SeqCst);
            let hits: Vec<u64> = self.logs_at.range(from..=to).copied().collect();
            if hits.len() > self.max_logs {
                return Err(SourceError::RangeTooLarge { from, to });
            }
            Ok(hits.into_iter().map(log_for).collect())
        }
        async fn block_hash(&self, _: u64) -> Result<Hash32, SourceError> {
            unreachable!()
        }
        fn finality_depth(&self) -> u64 {
            64
        }
    }

    /// Drain a chunker to exhaustion, returning every chunk in order.
    async fn drain(mut c: LogChunker) -> Vec<Chunk> {
        let mut out = Vec::new();
        while let Some(chunk) = c.next_chunk().await.unwrap() {
            out.push(chunk);
        }
        out
    }

    fn covered_blocks(chunks: &[Chunk]) -> Vec<u64> {
        chunks
            .iter()
            .flat_map(|c| c.logs.iter().map(|l| l.log_index as u64))
            .collect()
    }

    /// The core acceptance property: a range the provider will not serve whole is
    /// fetched by automatic bisection, with every log returned exactly once and
    /// in ascending order.
    #[tokio::test]
    async fn a_dense_range_is_bisected_and_every_log_returned_once_in_order() {
        // Logs packed into [40, 60]; the provider caps at 5 results per query, so
        // any window spanning much of that zone is rejected and must shrink.
        let src = DensitySource::new(40..=60, 5);
        let chunks = drain(LogChunker::new(src.clone(), 0, 100, 100)).await;

        let blocks = covered_blocks(&chunks);
        let expected: Vec<u64> = (40..=60).collect();
        assert_eq!(blocks, expected, "logs must be exactly the dense zone, in order");

        // Exactly once: no duplicates.
        let unique: BTreeSet<u64> = blocks.iter().copied().collect();
        assert_eq!(unique.len(), blocks.len(), "a log was returned more than once");

        // Windows are contiguous and cover the whole range with no gap.
        assert_eq!(chunks.first().unwrap().from, 0);
        assert_eq!(chunks.last().unwrap().to, 100);
        for pair in chunks.windows(2) {
            assert_eq!(pair[1].from, pair[0].to + 1, "windows must be contiguous");
        }
    }

    /// The window shrinks on rejection and recovers on success: the dense zone is
    /// crossed with narrow windows while the sparse ends use wide ones.
    #[tokio::test]
    async fn the_window_shrinks_on_rejection_and_recovers() {
        let src = DensitySource::new(40..=60, 5);
        let chunks = drain(LogChunker::new(src, 0, 100, 100)).await;

        let widths: Vec<u64> = chunks.iter().map(Chunk::width).collect();
        let min = *widths.iter().min().unwrap();
        let max = *widths.iter().max().unwrap();
        assert!(min < max, "windows should vary: narrow in the dense zone, wide outside ({widths:?})");
        // A shrink really happened (target was 100).
        assert!(min < 100, "the window must have shrunk below the target in the dense zone");
    }

    /// A range containing no logs still completes — empty is an answer, not an
    /// error — and does it in a single wide query.
    #[tokio::test]
    async fn an_empty_range_completes_in_one_query() {
        let src = DensitySource::new(std::iter::empty(), 5);
        let chunks = drain(LogChunker::new(src.clone(), 10, 20, 100)).await;

        assert_eq!(covered_blocks(&chunks), Vec::<u64>::new(), "no logs expected");
        assert_eq!(src.query_count(), 1, "an empty sparse range needs one query");
        // The range is still fully covered.
        assert_eq!(chunks.first().unwrap().from, 10);
        assert_eq!(chunks.last().unwrap().to, 20);
    }

    /// A degenerate range (from > to) yields nothing without touching the source.
    #[tokio::test]
    async fn a_backwards_range_yields_nothing() {
        let src = DensitySource::new(std::iter::empty(), 5);
        let chunks = drain(LogChunker::new(src.clone(), 50, 40, 100)).await;
        assert!(chunks.is_empty());
        assert_eq!(src.query_count(), 0, "nothing to ask");
    }

    /// If even a single block exceeds the provider's limit, bisection has no move
    /// left and the error is surfaced rather than looping forever.
    #[tokio::test]
    async fn a_single_block_that_is_still_too_large_surfaces_the_error() {
        // A source that rejects everything, even one block.
        struct AlwaysTooLarge;
        #[async_trait]
        impl ChainSource for AlwaysTooLarge {
            async fn latest_block(&self) -> Result<u64, SourceError> {
                unreachable!()
            }
            async fn finalized_block(&self) -> Result<u64, SourceError> {
                unreachable!()
            }
            async fn fetch_block(&self, _: u64) -> Result<BlockUnit, SourceError> {
                unreachable!()
            }
            async fn fetch_logs(&self, from: u64, to: u64) -> Result<Vec<RawLog>, SourceError> {
                Err(SourceError::RangeTooLarge { from, to })
            }
            async fn block_hash(&self, _: u64) -> Result<Hash32, SourceError> {
                unreachable!()
            }
            fn finality_depth(&self) -> u64 {
                64
            }
        }

        let mut c = LogChunker::new(Arc::new(AlwaysTooLarge), 5, 5, 8);
        let err = c.next_chunk().await.unwrap_err();
        assert!(matches!(err, SourceError::RangeTooLarge { from: 5, to: 5 }));
    }

    /// Transient failures are not the chunker's job — they propagate to the
    /// caller's backoff unchanged, without bisecting.
    #[tokio::test]
    async fn a_transient_failure_propagates_without_bisecting() {
        struct FlakySource {
            queries: AtomicUsize,
        }
        #[async_trait]
        impl ChainSource for FlakySource {
            async fn latest_block(&self) -> Result<u64, SourceError> {
                unreachable!()
            }
            async fn finalized_block(&self) -> Result<u64, SourceError> {
                unreachable!()
            }
            async fn fetch_block(&self, _: u64) -> Result<BlockUnit, SourceError> {
                unreachable!()
            }
            async fn fetch_logs(&self, _: u64, _: u64) -> Result<Vec<RawLog>, SourceError> {
                self.queries.fetch_add(1, Ordering::SeqCst);
                Err(SourceError::Transient("mock outage".into()))
            }
            async fn block_hash(&self, _: u64) -> Result<Hash32, SourceError> {
                unreachable!()
            }
            fn finality_depth(&self) -> u64 {
                64
            }
        }

        let src = Arc::new(FlakySource {
            queries: AtomicUsize::new(0),
        });
        let mut c = LogChunker::new(src.clone(), 0, 1000, 500);
        let err = c.next_chunk().await.unwrap_err();
        assert!(matches!(err, SourceError::Transient(_)));
        assert_eq!(src.queries.load(Ordering::SeqCst), 1, "transient must not trigger a bisection loop");
    }
}
