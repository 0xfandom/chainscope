//! The chain boundary.
//!
//! Everything downstream of fetching — cursor, writer, reorg bookkeeping, API —
//! sees only this trait. Nothing below it knows what an RPC endpoint is, what
//! `alloy` is, or that Ethereum exists. A second chain is a second `impl`, not
//! a second architecture.
//!
//! The boundary is enforced by the compiler rather than by discipline: this
//! crate has no chain library in its dependency list, so a type that leaked
//! through would not compile.

use async_trait::async_trait;

use crate::types::{BlockUnit, Hash32, RawLog};

/// What went wrong when talking to the chain.
///
/// The variants exist because the caller does something *different* for each
/// one. A stringly-typed error would force every call site to match on message
/// text to decide whether to retry, shrink, or give up — which is how a
/// provider changing its wording turns into a production incident.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// Timeout, rate limit, 5xx, connection reset. The request was fine; the
    /// world was briefly not. Retry with backoff.
    #[error("transient source failure: {0}")]
    Transient(String),

    /// The provider refused the block range as too wide. Not an error to retry
    /// as-is — the caller must bisect and try smaller. This is the variant that
    /// makes adaptive chunking possible: the fetcher converges on whatever the
    /// provider will actually serve instead of guessing a constant.
    #[error("range {from}..={to} rejected as too large")]
    RangeTooLarge { from: u64, to: u64 },

    /// The node does not have this block. Either it is ahead of the chain tip,
    /// or the node pruned it. Distinct from `Transient` because retrying the
    /// same request forever is the wrong response.
    #[error("block {number} not available from this source")]
    BlockNotFound { number: u64 },

    /// A malformed response, or something that cannot succeed on retry. Halt
    /// and surface it — this is a bug or a misconfiguration, not weather.
    #[error("fatal source failure: {0}")]
    Fatal(String),
}

impl SourceError {
    /// Whether retrying the identical request could succeed.
    ///
    /// `RangeTooLarge` is deliberately false: the request must change first.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

/// A source of blocks and logs.
///
/// Implementations are expected to carry their own address filter, set at
/// construction. The trait does not take a list of addresses per call because
/// "which contracts do we watch" is configuration, not a property of a single
/// fetch, and threading it through every call would put chain-specific address
/// shapes into every downstream signature.
#[async_trait]
pub trait ChainSource: Send + Sync {
    /// Height of the chain tip.
    async fn latest_block(&self) -> Result<u64, SourceError>;

    /// Height below which blocks are irreversible.
    ///
    /// Asked of the chain rather than computed as `latest - finality_depth`,
    /// because finality is something the consensus layer decides and a fixed
    /// depth is only ever an approximation of it. `finality_depth` remains as
    /// the fallback for sources that cannot answer this.
    async fn finalized_block(&self) -> Result<u64, SourceError>;

    /// One block with the logs we care about. The pipeline's unit of work.
    async fn fetch_block(&self, number: u64) -> Result<BlockUnit, SourceError>;

    /// Logs across an inclusive range. The backfill path, where fetching block
    /// by block would mean one round trip per block.
    async fn fetch_logs(&self, from: u64, to: u64) -> Result<Vec<RawLog>, SourceError>;

    /// Hash at a height, without the logs.
    ///
    /// The reorg primitive: walking backwards comparing hashes is the cheapest
    /// possible question, and paying for a block's full log set to answer it
    /// would make reorg detection cost more than ingestion.
    async fn block_hash(&self, number: u64) -> Result<Hash32, SourceError>;

    /// Conservative depth at which this chain is treated as settled, used when
    /// `finalized_block` is unavailable. Ethereum finalises after two epochs,
    /// which is ~64 blocks.
    fn finality_depth(&self) -> u64;
}
