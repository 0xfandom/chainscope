//! Reorg detection (#45), against a real Postgres.
//!
//! Seeds the `blocks` window with the chain we *had* indexed (all branch 0),
//! then checks incoming blocks from a node whose chain has forked — branch 0 at
//! and below a fork point, branch 1 above it, the shared-prefix shape a real
//! reorg has. Asserts detection returns the right verdict:
//!   * a linear extension is recognised with no walk;
//!   * a shallow and a deep reorg both resolve to the correct fork point;
//!   * a reorg reaching past the finalised line surfaces as an error;
//!   * a block whose parent is already finalised (and pruned) extends cleanly.
//!
//! The synthetic chain's own `branched()` differs at *every* height, so it can't
//! express a shared prefix on its own; this test composes one with a local
//! `ForkChain` that switches branch at the fork point.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test reorg_detect_db -- --ignored --nocapture

use async_trait::async_trait;
use chainscope_core::{
    source::{ChainSource, SourceError},
    types::{Hash32, RawLog},
    BlockUnit,
};
use chainscope_indexer::{
    db,
    reorg::{check_continuity, ChainCheck},
    testkit::SyntheticChain,
};
use sqlx::{postgres::PgPoolOptions, PgPool};

const FLOOR: u64 = 100;
const OLD_TIP: u64 = 130;
const HEAD: u64 = 300;

/// A node whose canonical chain is branch 0 at and below `fork_point` and
/// branch 1 above it — a genuine shared prefix, unlike a single branched chain.
struct ForkChain {
    fork_point: u64,
}

impl ForkChain {
    fn canon(&self, n: u64) -> Hash32 {
        let branch = if n <= self.fork_point { 0 } else { 1 };
        SyntheticChain::branched(HEAD, branch).hash_at(n)
    }
}

#[async_trait]
impl ChainSource for ForkChain {
    async fn latest_block(&self) -> Result<u64, SourceError> {
        Ok(HEAD)
    }
    async fn finalized_block(&self) -> Result<u64, SourceError> {
        Ok(HEAD.saturating_sub(64))
    }
    async fn fetch_block(&self, number: u64) -> Result<BlockUnit, SourceError> {
        Ok(BlockUnit {
            number,
            hash: self.canon(number),
            parent_hash: self.canon(number.saturating_sub(1)),
            timestamp: 0,
            logs: vec![],
        })
    }
    async fn fetch_logs(&self, _: u64, _: u64) -> Result<Vec<RawLog>, SourceError> {
        Ok(vec![])
    }
    async fn block_hash(&self, number: u64) -> Result<Hash32, SourceError> {
        Ok(self.canon(number))
    }
    fn finality_depth(&self) -> u64 {
        64
    }
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_reorg_{}_{}", std::process::id(), tag);
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

/// Record the branch-0 chain we had indexed before the reorg, over the pending
/// window `[FLOOR+1, OLD_TIP]`, and set the finality line at `FLOOR`.
async fn seed_indexed_chain(pool: &PgPool) {
    let old = SyntheticChain::new(OLD_TIP); // branch 0
    for n in (FLOOR + 1)..=OLD_TIP {
        let h = old.hash_at(n);
        let p = old.hash_at(n - 1);
        sqlx::query(
            "INSERT INTO blocks (number, block_hash, parent_hash, block_time)
             VALUES ($1, $2, $3, now())",
        )
        .bind(n as i64)
        .bind(h.as_slice())
        .bind(p.as_slice())
        .execute(pool)
        .await
        .unwrap();
    }
    db::advance_finality(pool, HEAD, FLOOR).await.unwrap();
}

/// The next block after our tip, as the node now reports it.
async fn incoming(fork: &ForkChain, number: u64) -> BlockUnit {
    fork.fetch_block(number).await.unwrap()
}

/// The node still agrees with everything we recorded: a linear extension, no
/// backward walk needed.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_linear_extension_is_recognised() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "extend").await;
    seed_indexed_chain(&pool).await;

    // No divergence anywhere in the window (fork point far above the tip).
    let fork = ForkChain { fork_point: HEAD };
    let block = incoming(&fork, OLD_TIP + 1).await;

    let check = check_continuity(&fork, &pool, &block).await.unwrap();
    assert_eq!(check, ChainCheck::Extends);

    eprintln!("detect extend OK");
    drop_db(&admin, pool, &name).await;
}

/// One reorged block at the tip: fetching the next block reveals the parent no
/// longer matches, and the walk finds the fork one block down.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_shallow_reorg_finds_the_fork_point() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "shallow").await;
    seed_indexed_chain(&pool).await;

    // Block 130 was rewritten; 129 and below still agree.
    let fork = ForkChain { fork_point: OLD_TIP - 1 };
    let block = incoming(&fork, OLD_TIP + 1).await;

    let check = check_continuity(&fork, &pool, &block).await.unwrap();
    assert_eq!(check, ChainCheck::Forked { fork_point: OLD_TIP - 1 });

    eprintln!("detect shallow reorg OK: fork at {}", OLD_TIP - 1);
    drop_db(&admin, pool, &name).await;
}

/// A reorg reaching many blocks back, but still above the finalised line,
/// resolves to the exact fork point.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_deep_reorg_within_the_window_finds_the_fork_point() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "deep").await;
    seed_indexed_chain(&pool).await;

    // Fork at 110: blocks 111..=130 are all orphaned, 110 is the common ancestor.
    let fork = ForkChain { fork_point: 110 };
    let block = incoming(&fork, OLD_TIP + 1).await;

    let check = check_continuity(&fork, &pool, &block).await.unwrap();
    assert_eq!(check, ChainCheck::Forked { fork_point: 110 });

    eprintln!("detect deep reorg OK: fork at 110");
    drop_db(&admin, pool, &name).await;
}

/// A reorg whose fork point lies below the finalised line contradicts finality
/// itself — it must surface as an error, never be indexed over.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_reorg_past_the_finalised_line_is_surfaced() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "toodeep").await;
    seed_indexed_chain(&pool).await;

    // Fork at 99, below the finalised line 100: block 100 itself would have
    // changed, which finality promised could never happen.
    let fork = ForkChain { fork_point: FLOOR - 1 };
    let block = incoming(&fork, OLD_TIP + 1).await;

    let err = check_continuity(&fork, &pool, &block).await.unwrap_err();
    assert!(
        err.to_string().contains("finalised line"),
        "should name the finality violation: {err}"
    );

    eprintln!("detect too-deep reorg OK: surfaced `{err}`");
    drop_db(&admin, pool, &name).await;
}

/// A block whose parent has already been finalised and pruned has nothing in the
/// window to disagree with, so it extends cleanly.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_block_adjacent_to_the_frozen_region_extends() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "adjacent").await;
    seed_indexed_chain(&pool).await;

    // Block FLOOR+1's parent is FLOOR, which is finalised and not in the window.
    let fork = ForkChain { fork_point: HEAD };
    let block = incoming(&fork, FLOOR + 1).await;

    let check = check_continuity(&fork, &pool, &block).await.unwrap();
    assert_eq!(check, ChainCheck::Extends, "no recorded parent to fork against");

    eprintln!("detect frozen-adjacent OK");
    drop_db(&admin, pool, &name).await;
}
