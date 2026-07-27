//! Reorg detection: is the next block a clean extension, or a fork?
//!
//! Ethereum occasionally replaces its most recent blocks. A block we recorded as
//! `N` with hash `AAA` can, a few seconds later, actually be hash `BBB` with
//! different transactions — a reorganisation. If we write over our chain without
//! noticing, the database keeps trades from the discarded branch (that never
//! happened) and misses the trades from the branch the network kept.
//!
//! This module answers one question before the writer commits block `N`: does
//! `N`'s parent still match the block we recorded at `N-1`?
//!
//!   * **Match** — the chain is a linear extension. Write normally.
//!   * **Mismatch** — a reorg. Walk backwards comparing our recorded hashes
//!     against the node's chain until they agree again; that height is the
//!     **fork point**, and everything above it is orphaned.
//!
//! Detection only *finds* the fork point. Unwinding the orphaned rows and laying
//! down the canonical branch is the writer's job (#46); keeping the two apart
//! makes each testable on its own. The primitive is [`ChainSource::block_hash`]
//! — a hash at a height, no logs — because "does this still match?" is the
//! cheapest question the chain can be asked, and paying for a full block to
//! answer it would make reorg checks cost more than ingestion.

use chainscope_core::{source::ChainSource, BlockUnit};
use sqlx::postgres::PgPool;

use crate::db;

/// What checking an incoming block against our recorded chain found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainCheck {
    /// The block's parent is the block we recorded before it. Write normally.
    Extends,
    /// A reorg: every block above `fork_point` is orphaned and must be rewound
    /// before the canonical branch is written. `fork_point` is the highest block
    /// on which our chain and the node's still agree.
    Forked { fork_point: u64 },
}

/// Check whether `incoming` (block `N`, freshly fetched from the node) extends
/// the chain we have recorded, or forks it.
///
/// The cheap check comes first: `incoming.parent_hash` is the node's hash for
/// `N-1`, and if it equals the hash we stored for `N-1` the chain is intact with
/// no extra round trip. Only a mismatch pays for the backward walk.
///
/// The walk is bounded below by the finalised line ([`db::load_finalized_height`]).
/// A finalised block can never reorg, so reaching that line without finding
/// agreement means the reorg is deeper than the whole pending window — deeper
/// than `finality_depth` was configured to survive. That is a misconfiguration
/// an operator must see, so it surfaces as an error rather than silently writing
/// over blocks we promised were frozen.
pub async fn check_continuity(
    source: &dyn ChainSource,
    pool: &PgPool,
    incoming: &BlockUnit,
) -> anyhow::Result<ChainCheck> {
    let n = incoming.number;

    // The genesis of the chain has no parent to disagree with.
    if n == 0 {
        return Ok(ChainCheck::Extends);
    }
    let parent_number = n - 1;

    // No record of the parent — a fresh follower, or the parent has already been
    // finalised and pruned. A block adjacent to the frozen region cannot have
    // forked below finality, so this is a clean extension.
    let Some(stored_parent) = db::stored_block_hash(pool, parent_number).await? else {
        return Ok(ChainCheck::Extends);
    };

    if stored_parent == incoming.parent_hash {
        return Ok(ChainCheck::Extends);
    }

    // Our N-1 is not the node's parent of N: a reorg. Walk back to the last
    // height where our recorded hash still matches the node's chain.
    let floor = db::load_finalized_height(pool).await?.unwrap_or(0);
    let mut height = parent_number;
    loop {
        if height <= floor {
            return Err(anyhow::anyhow!(
                "reorg reaches the finalised line (block {floor}) without a common ancestor: \
                 the reorg is deeper than finality_depth allows — increase it and re-check the chain"
            ));
        }

        let Some(ours) = db::stored_block_hash(pool, height).await? else {
            // The window is contiguous from the finalised line to the tip, so a
            // hole in it is a bug, not a reorg.
            return Err(anyhow::anyhow!(
                "reorg walk found no recorded header at block {height}, above the finalised line {floor}"
            ));
        };
        let canonical = source.block_hash(height).await?;

        if ours == canonical {
            return Ok(ChainCheck::Forked { fork_point: height });
        }
        height -= 1;
    }
}
