//! A deterministic synthetic chain for tests.
//!
//! Fixed hashes and parent links, so a test can assert exact block continuity
//! without depending on any network. Every hash folds in a `branch` byte: M1
//! uses a single branch, and M4's reorg tests will construct a second chain
//! with a different branch id, whose hashes above a chosen fork point differ
//! exactly as a real reorg's would. That is the "designed so branch switching
//! can be added later" hook — the shape is here now, unused.

use async_trait::async_trait;
use chainscope_core::{
    source::{ChainSource, SourceError},
    types::{Address20, Hash32, RawLog},
    BlockUnit,
};

/// A fixed base timestamp inside a day for which the migration creates a
/// partition (`CURRENT_DATE ± a week`), so the synthetic chain's swaps have a
/// `swaps` partition to land in. 2026-07-24T00:00:00Z; 150 one-second steps stay
/// within the day.
const BASE_TS: i64 = 1_784_851_200;

/// The Uniswap V3 `Swap` event signature hash (`topics[0]`). A test that wants
/// the synthetic chain's swaps decoded must watch [`SYNTHETIC_POOL`].
const SWAP_SIG: Hash32 = [
    0xc4, 0x20, 0x79, 0xf9, 0x4a, 0x63, 0x50, 0xd7, 0xe6, 0x23, 0x5f, 0x29, 0x17, 0x49, 0x24, 0xf9,
    0x28, 0xcc, 0x2a, 0xc8, 0x18, 0xeb, 0x64, 0xfe, 0xd8, 0x00, 0x4e, 0x11, 0x5f, 0xbc, 0xca, 0x67,
];

/// The pool the synthetic chain's swaps are emitted from. A transformer that
/// should decode them must include this in its watched set.
pub const SYNTHETIC_POOL: Address20 = [0xAB; 20];

const SYNTHETIC_SENDER: Address20 = [0xCD; 20];
const SYNTHETIC_RECIPIENT: Address20 = [0xCE; 20];

/// A signed 256-bit ABI word (two's complement, sign-extended).
fn word_i256(v: i128) -> Hash32 {
    let mut w = if v.is_negative() { [0xff; 32] } else { [0u8; 32] };
    w[16..32].copy_from_slice(&v.to_be_bytes());
    w
}

/// An unsigned ABI word (right-aligned).
fn word_u128(v: u128) -> Hash32 {
    let mut w = [0u8; 32];
    w[16..32].copy_from_slice(&v.to_be_bytes());
    w
}

/// A 20-byte address in an indexed-topic slot (right-aligned in 32 bytes).
fn word_addr(a: Address20) -> Hash32 {
    let mut w = [0u8; 32];
    w[12..32].copy_from_slice(&a);
    w
}

#[derive(Clone)]
pub struct SyntheticChain {
    height: u64,
    branch: u8,
}

impl SyntheticChain {
    pub fn new(height: u64) -> Self {
        Self { height, branch: 0 }
    }

    /// An alternate branch, for M4. Hashes differ from branch 0 everywhere, so a
    /// caller wanting a fork constructs this above the fork point.
    pub fn branched(height: u64, branch: u8) -> Self {
        Self { height, branch }
    }

    /// Deterministic hash: branch in the first byte, block number in the last
    /// eight. Distinct per (branch, number), and `hash_at(n-1)` is exactly the
    /// parent link of block `n`.
    pub fn hash_at(&self, n: u64) -> Hash32 {
        let mut h = [0u8; 32];
        h[0] = self.branch;
        h[24..32].copy_from_slice(&n.to_be_bytes());
        h
    }

    pub fn unit(&self, n: u64) -> BlockUnit {
        BlockUnit {
            number: n,
            hash: self.hash_at(n),
            parent_hash: self.hash_at(n.saturating_sub(1)),
            timestamp: BASE_TS + n as i64,
            logs: self.logs_at(n),
        }
    }

    /// One decodable Uniswap V3 `Swap` per block, from [`SYNTHETIC_POOL`].
    ///
    /// The field values are deterministic and ABI-valid rather than economically
    /// meaningful — the point is that the decoder produces exactly one `SwapRow`
    /// per block, so the crash harness can assert row-level exactly-once, not
    /// just block-level. The `tx_hash` folds in the branch byte (via `hash_at`),
    /// so M4's alternate branch will produce distinct swaps at the same heights,
    /// exactly as a reorg re-including a transaction would.
    fn logs_at(&self, n: u64) -> Vec<RawLog> {
        let mut data = Vec::with_capacity(160);
        data.extend_from_slice(&word_i256(n as i128)); // amount0
        data.extend_from_slice(&word_i256(-(n as i128))); // amount1
        data.extend_from_slice(&word_u128(1_000_000 + n as u128)); // sqrtPriceX96
        data.extend_from_slice(&word_u128(2_000_000 + n as u128)); // liquidity
        data.extend_from_slice(&word_i256((n % 100) as i128)); // tick (small, positive)

        vec![RawLog {
            address: SYNTHETIC_POOL,
            topics: vec![
                SWAP_SIG,
                word_addr(SYNTHETIC_SENDER),
                word_addr(SYNTHETIC_RECIPIENT),
            ],
            data,
            tx_hash: self.hash_at(n),
            log_index: 0,
        }]
    }
}

#[async_trait]
impl ChainSource for SyntheticChain {
    async fn latest_block(&self) -> Result<u64, SourceError> {
        Ok(self.height)
    }

    async fn finalized_block(&self) -> Result<u64, SourceError> {
        Ok(self.height.saturating_sub(64))
    }

    async fn fetch_block(&self, number: u64) -> Result<BlockUnit, SourceError> {
        if number > self.height {
            return Err(SourceError::BlockNotFound { number });
        }
        Ok(self.unit(number))
    }

    async fn fetch_logs(&self, from: u64, to: u64) -> Result<Vec<RawLog>, SourceError> {
        Ok((from..=to.min(self.height)).flat_map(|n| self.logs_at(n)).collect())
    }

    async fn block_hash(&self, number: u64) -> Result<Hash32, SourceError> {
        if number > self.height {
            return Err(SourceError::BlockNotFound { number });
        }
        Ok(self.hash_at(number))
    }

    fn finality_depth(&self) -> u64 {
        64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainscope_eth_source::{map_log, Mapped};

    #[test]
    fn each_synthetic_block_carries_one_decodable_swap() {
        let chain = SyntheticChain::new(10);
        let unit = chain.unit(5);
        assert_eq!(unit.logs.len(), 1, "one log per block");
        match map_log(&unit.logs[0]) {
            Mapped::Swap(s) => {
                assert_eq!(s.pool, SYNTHETIC_POOL);
                assert_eq!(s.tick, 5);
            }
            other => panic!("synthetic log did not decode to a swap: {other:?}"),
        }
    }
}
