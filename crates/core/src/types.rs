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
use serde::{Deserialize, Serialize};

/// Serialise a `BigDecimal` as its decimal string.
///
/// The wire format (`bincode`) is not self-describing, and `BigDecimal`'s own
/// deserializer reads through `deserialize_any` — which a non-self-describing
/// format cannot answer, so the default impl fails to decode. A fixed string
/// representation sidesteps that and round-trips through any format, at the cost
/// of a few bytes over the raw mantissa. The values are exact either way.
mod bigdecimal_str {
    use bigdecimal::BigDecimal;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::str::FromStr;

    pub fn serialize<S: Serializer>(value: &BigDecimal, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<BigDecimal, D::Error> {
        let text = String::deserialize(d)?;
        BigDecimal::from_str(&text).map_err(serde::de::Error::custom)
    }
}

/// What travels the seam: either a payload, or a correction.
///
/// Phase 1 sent bare payloads. Phase 2 (the log) cannot: a reorg cannot reach
/// back and delete the orphaned block events already appended, so the correction
/// has to be *another event* in the stream — `Revert { from_block }` — that each
/// consumer reads in order and uses to undo its own state above that block. The
/// producer records the wrong turn (the orphan blocks) and the correction after
/// it, and total order within a partition is what lets every consumer converge.
///
/// Generic over the payload so both seams carry it: `Envelope<BlockUnit>` on the
/// producer→transformer topic, `Envelope<RowBatch>` on the transformer→writer
/// topic. The channel transport carries it too, uniformly — though under the
/// channel a reorg is still handled by the producer-side rewind, so only the
/// `Data` arm is ever sent there.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Envelope<T> {
    /// A normal payload: index it forward.
    Data(T),
    /// A reorg correction: every block strictly above `from_block` is orphaned,
    /// and each consumer undoes its own state above it. `from_block` is the fork
    /// point M4 detection finds.
    Revert { from_block: u64 },
}

impl<T> Envelope<T> {
    /// The payload if this is `Data`, else `None`. Convenience for call sites
    /// (and tests) that expect a data event and treat a revert as exceptional.
    pub fn into_data(self) -> Option<T> {
        match self {
            Envelope::Data(payload) => Some(payload),
            Envelope::Revert { .. } => None,
        }
    }

    /// Borrow the payload if this is `Data`, else `None`.
    pub fn as_data(&self) -> Option<&T> {
        match self {
            Envelope::Data(payload) => Some(payload),
            Envelope::Revert { .. } => None,
        }
    }
}

/// 32-byte identifier: a block hash, a parent hash, a transaction hash.
pub type Hash32 = [u8; 32];

/// 20-byte EVM address.
pub type Address20 = [u8; 20];

/// One undecoded log, exactly as the chain reported it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RawLog {
    pub address: Address20,
    /// `topics[0]` is the event signature; the rest are indexed parameters.
    pub topics: Vec<Hash32>,
    pub data: Vec<u8>,
    /// Block that produced this log. On the live path every log in a
    /// `BlockUnit` shares the block's number; it is carried per-log because the
    /// backfill path fetches logs across a *range* with one `eth_getLogs` call
    /// and needs to know which block each log belongs to — that is how the
    /// backfill driver discovers which blocks in a chunk are active.
    pub block_number: u64,
    pub tx_hash: Hash32,
    /// Position within the block. Block-unique, which is what makes
    /// `(tx_hash, log_index)` a valid natural key.
    pub log_index: u32,
}

/// The unit of work the producer publishes: one block, with its logs.
///
/// A whole block rather than a single log, because reorg handling and cursor
/// advancement are both per-block. Half a block is never a meaningful state.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SwapRow {
    pub tx_hash: Hash32,
    pub log_index: u32,
    pub pool: Address20,
    pub sender: Address20,
    pub recipient: Address20,
    /// Signed: negative is the token leaving the pool.
    #[serde(with = "bigdecimal_str")]
    pub amount0: BigDecimal,
    #[serde(with = "bigdecimal_str")]
    pub amount1: BigDecimal,
    #[serde(with = "bigdecimal_str")]
    pub sqrt_price_x96: BigDecimal,
    #[serde(with = "bigdecimal_str")]
    pub liquidity: BigDecimal,
    pub tick: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LiqRow {
    pub tx_hash: Hash32,
    pub log_index: u32,
    pub pool: Address20,
    pub kind: LiqKind,
    pub owner: Address20,
    pub tick_lower: i32,
    pub tick_upper: i32,
    #[serde(with = "bigdecimal_str")]
    pub amount: BigDecimal,
    #[serde(with = "bigdecimal_str")]
    pub amount0: BigDecimal,
    #[serde(with = "bigdecimal_str")]
    pub amount1: BigDecimal,
}
