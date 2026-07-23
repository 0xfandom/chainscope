//! The payload types that cross the transport seam.
//!
//! These are deliberately plain data: fixed-size byte arrays and numbers, no
//! `alloy` types, no `sqlx` types, no transport types. That is what lets the
//! same `BlockUnit` travel through an in-memory channel today and through a
//! serialized Kafka record in M5 without changing a single stage.
//!
//! On the byte-array aliases: a 32-byte hash and a 20-byte address are stated
//! as sizes rather than as chain types on purpose. Ethereum block hashes and
//! Solana blockhashes are both 32 bytes, so `Hash32` carries over unchanged. A
//! 20-byte address does not — a Solana pubkey is 32 — so a future non-EVM
//! source will add its own alias rather than bending this one. Keeping them as
//! sizes is what allows this crate to depend on no chain library at all, which
//! is the compile-time proof that the boundary is real.

use bigdecimal::BigDecimal;

/// 32-byte identifier: a block hash, a parent hash, a transaction hash.
pub type Hash32 = [u8; 32];

/// 20-byte EVM address.
pub type Address20 = [u8; 20];

/// One undecoded log, exactly as the chain reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLog {
    pub address: Address20,
    /// `topics[0]` is the event signature; the rest are indexed parameters.
    pub topics: Vec<Hash32>,
    pub data: Vec<u8>,
    pub tx_hash: Hash32,
    /// Position within the block. Block-unique, which is what makes
    /// `(tx_hash, log_index)` a valid natural key.
    pub log_index: u32,
}

/// The unit of work the producer publishes: one block, with its logs.
///
/// A whole block rather than a single log, because reorg handling and cursor
/// advancement are both per-block. Half a block is never a meaningful state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockUnit {
    pub number: u64,
    pub hash: Hash32,
    /// Carried through the pipeline because the reorg check compares it against
    /// the hash of the block we already stored at `number - 1`.
    pub parent_hash: Hash32,
    /// Unix seconds. Becomes `block_time`, which is the partition key.
    pub timestamp: i64,
    pub logs: Vec<RawLog>,
}

/// The unit the transformer publishes: one block's worth of decoded rows.
///
/// Still keyed by block for the same reason as `BlockUnit` — the writer commits
/// exactly one block per transaction, together with the cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct RowBatch {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub block_time: i64,
    pub swaps: Vec<SwapRow>,
    pub liq_events: Vec<LiqRow>,
}

impl RowBatch {
    /// True when a block produced nothing we care about — the common case, since
    /// most blocks touch none of the pools we index. The writer still advances
    /// the cursor for these, or it would re-scan them forever.
    pub fn is_empty(&self) -> bool {
        self.swaps.is_empty() && self.liq_events.is_empty()
    }
}

/// Amounts are `BigDecimal` to match the `NUMERIC` columns. Uniswap deals in
/// int256 and uint160, which overflow every Rust integer, and money is never
/// floating point.
#[derive(Debug, Clone, PartialEq)]
pub struct SwapRow {
    pub tx_hash: Hash32,
    pub log_index: u32,
    pub pool: Address20,
    pub sender: Address20,
    pub recipient: Address20,
    /// Signed: negative is the token leaving the pool.
    pub amount0: BigDecimal,
    pub amount1: BigDecimal,
    pub sqrt_price_x96: BigDecimal,
    pub liquidity: BigDecimal,
    pub tick: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiqKind {
    Mint,
    Burn,
    Collect,
}

impl LiqKind {
    /// Must match the `liq_events_kind_check` constraint in migration 0005.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mint => "mint",
            Self::Burn => "burn",
            Self::Collect => "collect",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiqRow {
    pub tx_hash: Hash32,
    pub log_index: u32,
    pub pool: Address20,
    pub kind: LiqKind,
    pub owner: Address20,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub amount: BigDecimal,
    pub amount0: BigDecimal,
    pub amount1: BigDecimal,
}
