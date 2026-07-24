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
    types::{Hash32, RawLog},
    BlockUnit,
};

/// Arbitrary fixed epoch so block timestamps are deterministic.
const BASE_TS: i64 = 1_700_000_000;

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

    /// One deterministic log per block. Unused by the M1 writer, which only
    /// records block headers, but served here so M2's decoder and M4's reorg
    /// tests can rely on the same source.
    fn logs_at(&self, n: u64) -> Vec<RawLog> {
        vec![RawLog {
            address: [self.branch; 20],
            topics: vec![self.hash_at(n)],
            data: n.to_be_bytes().to_vec(),
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
