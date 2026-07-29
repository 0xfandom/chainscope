//! Transactional rewind and live-path recovery (#46), against a real Postgres.
//!
//! Proves the rewind primitive and the producer wiring that drives it:
//!   * `rewind_to` deletes every header/swap/liq above the fork point and resets
//!     the live cursor to it;
//!   * a failure mid-rewind rolls the delete and the cursor reset back together;
//!   * `DbReorgHandler` detects a fork and rewinds the database;
//!   * a real `Producer` with the handler, fed a forked chain, winds `next` back
//!     to the fork point and re-publishes the canonical branch forward.
//!
//! Candle compensation is #47, so these assert on raw rows and the cursor only.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test reorg_rewind_db -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainscope_core::{
    source::{ChainSource, SourceError},
    types::{Address20, Hash32, RawLog},
    BlockUnit,
};
use chainscope_indexer::{
    db,
    producer::Producer,
    reorg::{Continuity, DbReorgHandler, ReorgHandler},
    testkit::{SyntheticChain, SYNTHETIC_POOL},
    transformer::decode_block,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::HashSet;
use tokio_util::sync::CancellationToken;

const FIRST: u64 = 90;
const OLD_TIP: u64 = 130;
const FINALIZED: u64 = 89;
const HEAD: u64 = 300;

/// A node whose canonical chain is branch 0 at and below `fork_point`, branch 1
/// above it — a real shared prefix.
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
    let name = format!("chainscope_rewind_{}_{}", std::process::id(), tag);
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
    db::ensure_partitions(&pool).await.unwrap();
    // Synthetic blocks land on 2026-07-24; that day's swap partition must exist.
    for parent in ["swaps", "liq_events"] {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {parent}_20260724 PARTITION OF {parent} \
             FOR VALUES FROM ('2026-07-24') TO ('2026-07-25')"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

/// Index the branch-0 chain `[FIRST, OLD_TIP]` through the real write path — one
/// decodable swap per block — and set the finality line at `FINALIZED`.
async fn seed_indexed_chain(pool: &PgPool) {
    let chain = SyntheticChain::new(OLD_TIP); // branch 0
    let watched: HashSet<Address20> = [SYNTHETIC_POOL].into_iter().collect();
    for n in FIRST..=OLD_TIP {
        let (batch, _) = decode_block(&chain.unit(n), &watched);
        db::write_row_batches(pool, &[batch], false).await.unwrap();
    }
    db::advance_finality(pool, HEAD, FINALIZED).await.unwrap();
}

async fn max_block(pool: &PgPool) -> Option<i64> {
    sqlx::query("SELECT max(number) AS m FROM blocks")
        .fetch_one(pool)
        .await
        .unwrap()
        .get("m")
}
async fn swap_count_above(pool: &PgPool, n: u64) -> i64 {
    sqlx::query("SELECT count(*) FROM swaps WHERE block_number > $1")
        .bind(n as i64)
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0)
}
async fn live_cursor(pool: &PgPool) -> Option<u64> {
    db::load_live_cursor(pool).await.unwrap()
}

/// A rewind deletes everything above the fork point and moves the cursor back.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn rewind_deletes_orphans_and_resets_the_cursor() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "primitive").await;
    seed_indexed_chain(&pool).await;

    assert_eq!(max_block(&pool).await, Some(OLD_TIP as i64));
    assert_eq!(live_cursor(&pool).await, Some(OLD_TIP));

    let fork = 125;
    let removed = db::rewind_to(&pool, fork, false).await.unwrap();

    assert_eq!(removed, OLD_TIP - fork, "blocks 126..=130 removed");
    assert_eq!(max_block(&pool).await, Some(fork as i64), "nothing above the fork survives");
    assert_eq!(swap_count_above(&pool, fork).await, 0, "orphaned swaps gone");
    assert_eq!(live_cursor(&pool).await, Some(fork), "cursor wound back to the fork");

    eprintln!("rewind primitive OK: removed {removed}, cursor={fork}");
    drop_db(&admin, pool, &name).await;
}

/// A failure mid-rewind leaves the pre-rewind state entirely intact.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn rewind_rolls_back_on_failure() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "rollback").await;
    seed_indexed_chain(&pool).await;

    let err = db::rewind_to(&pool, 125, true).await.unwrap_err();
    assert!(err.to_string().contains("injected failure"));

    // Nothing moved: the orphans and the cursor are exactly as they were.
    assert_eq!(max_block(&pool).await, Some(OLD_TIP as i64), "no header deleted");
    assert_eq!(swap_count_above(&pool, 125).await, (OLD_TIP - 125) as i64, "no swap deleted");
    assert_eq!(live_cursor(&pool).await, Some(OLD_TIP), "cursor unchanged");

    eprintln!("rewind rollback OK: delete and cursor reset roll back together");
    drop_db(&admin, pool, &name).await;
}

/// The production handler detects a fork and rewinds; a clean block is waved
/// through untouched.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn the_db_handler_rewinds_on_a_fork() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "handler").await;
    seed_indexed_chain(&pool).await;

    // A clean extension: no divergence, nothing rewound.
    let clean = Arc::new(ForkChain { fork_point: HEAD });
    let handler = DbReorgHandler::new(clean.clone(), pool.clone());
    let next = clean.fetch_block(OLD_TIP + 1).await.unwrap();
    assert_eq!(handler.on_block(&next).await.unwrap(), Continuity::Extends);
    assert_eq!(max_block(&pool).await, Some(OLD_TIP as i64), "a clean block rewinds nothing");

    // A fork at 127: fetching 131 reveals the mismatch and rewinds to 127.
    let forked = Arc::new(ForkChain { fork_point: 127 });
    let handler = DbReorgHandler::new(forked.clone(), pool.clone());
    let next = forked.fetch_block(OLD_TIP + 1).await.unwrap();
    assert_eq!(
        handler.on_block(&next).await.unwrap(),
        Continuity::RewoundTo { fork_point: 127 }
    );
    assert_eq!(max_block(&pool).await, Some(127), "rewound to the fork point");
    assert_eq!(live_cursor(&pool).await, Some(127));

    eprintln!("db handler OK: clean extends, fork rewinds to 127");
    drop_db(&admin, pool, &name).await;
}

/// A real producer with the handler, fed a forked chain, winds `next` back to
/// the fork point and re-publishes the canonical branch — the first block it
/// emits is the canonical one just above the fork, never an orphan.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn the_producer_recovers_the_canonical_branch_after_a_reorg() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "producer").await;
    seed_indexed_chain(&pool).await;

    let fork = 125u64;
    let source: Arc<dyn ChainSource> = Arc::new(ForkChain { fork_point: fork });
    let handler: Arc<dyn ReorgHandler> =
        Arc::new(DbReorgHandler::new(Arc::clone(&source), pool.clone()));

    let (sink, mut rx) = chainscope_core::build_transport::<BlockUnit>(chainscope_core::TransportSpec::Channel { capacity: 64 }).unwrap();
    let cancel = CancellationToken::new();
    let producer = Producer::new(
        Arc::clone(&source),
        sink,
        Some(OLD_TIP), // resume at OLD_TIP+1, which triggers the reorg
        0,
        Duration::from_millis(5),
        cancel.clone(),
    )
    .with_reorg_handler(handler);
    let handle = tokio::spawn(producer.run());

    // The canonical hash the branch settled on above the fork.
    let canonical = SyntheticChain::branched(HEAD, 1);

    // The very first block published must be the canonical block just above the
    // fork — proof the orphan (OLD_TIP+1 on our old branch) was never emitted and
    // the producer wound back.
    let first = rx.recv().await.unwrap().unwrap().payload;
    assert_eq!(first.number, fork + 1, "resumed at the fork point, not the old tip");
    assert_eq!(first.hash, canonical.hash_at(fork + 1), "re-published the canonical branch");

    // Drain forward to the canonical rewrite of the block that first exposed the
    // reorg.
    let mut last = first.number;
    while last < OLD_TIP + 1 {
        let u = rx.recv().await.unwrap().unwrap().payload;
        assert_eq!(u.hash, canonical.hash_at(u.number), "every re-published block is canonical");
        last = u.number;
    }
    cancel.cancel();
    handle.await.unwrap().unwrap();

    // The database was rewound to the fork; nothing above it survived the reorg
    // (no writer in this test re-adds them).
    assert_eq!(max_block(&pool).await, Some(fork as i64), "window rewound to the fork");
    assert_eq!(live_cursor(&pool).await, Some(fork));

    eprintln!("producer recovery OK: resumed at {}, canonical to {}", fork + 1, OLD_TIP + 1);
    drop_db(&admin, pool, &name).await;
}
