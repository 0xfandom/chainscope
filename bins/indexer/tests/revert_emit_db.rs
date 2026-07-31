//! #59: on a reorg the producer *emits a revert*, it does not delete.
//!
//! The M4 counterpart (`reorg_rewind_db`) proves the producer + `DbReorgHandler`
//! rewinds the database in place. This proves the phase-2 behaviour over the same
//! forked-chain scaffolding: `LogReorgHandler` reuses M4 detection verbatim, but
//! on a fork it returns `EmitRevert`, and the producer appends a `Revert` to the
//! log ahead of the canonical replay — touching nothing in storage. The
//! consumer-side undo that acts on that revert is #60; here we assert only what
//! the producer emits, and that it deleted nothing.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test revert_emit_db -- --ignored --nocapture

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainscope_core::{
    source::{ChainSource, SourceError},
    types::{Address20, Hash32, RawLog},
    BlockUnit, Envelope,
};
use chainscope_indexer::{
    db,
    producer::Producer,
    reorg::{Continuity, LogReorgHandler, ReorgHandler},
    testkit::{SyntheticChain, SYNTHETIC_POOL},
    transformer::decode_block,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio_util::sync::CancellationToken;

const FIRST: u64 = 90;
const OLD_TIP: u64 = 130;
const FINALIZED: u64 = 89;
const HEAD: u64 = 300;
const FORK: u64 = 125;

/// A node canonical on branch 0 at/below `fork_point`, branch 1 above it.
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
    let name = format!("chainscope_revert_{}_{}", std::process::id(), tag);
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

/// The log-mode handler detects the same fork the DB handler does, but returns
/// `EmitRevert` and leaves the database untouched.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn the_log_handler_emits_a_revert_and_deletes_nothing() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "handler").await;
    seed_indexed_chain(&pool).await;
    assert_eq!(max_block(&pool).await, Some(OLD_TIP as i64));

    // A clean extension waves through, exactly as in phase 1.
    let clean = Arc::new(ForkChain { fork_point: HEAD });
    let handler = LogReorgHandler::new(clean.clone(), pool.clone());
    let next = clean.fetch_block(OLD_TIP + 1).await.unwrap();
    assert_eq!(handler.on_block(&next).await.unwrap(), Continuity::Extends);

    // A fork: the same detection M4 uses, but the effect is a revert, not a
    // delete — the fork point matches, and the database is untouched.
    let forked = Arc::new(ForkChain { fork_point: FORK });
    let handler = LogReorgHandler::new(forked.clone(), pool.clone());
    let next = forked.fetch_block(OLD_TIP + 1).await.unwrap();
    assert_eq!(
        handler.on_block(&next).await.unwrap(),
        Continuity::EmitRevert { fork_point: FORK }
    );
    assert_eq!(
        max_block(&pool).await,
        Some(OLD_TIP as i64),
        "the log handler must not delete anything — the revert is an event, not a rewind"
    );

    eprintln!("log handler OK: fork -> EmitRevert {{ {FORK} }}, nothing deleted");
    drop_db(&admin, pool, &name).await;
}

/// A real producer with the log handler, fed a forked chain, publishes
/// `Revert { from_block: FORK }` first and then the canonical branch as `Data`,
/// in that order — and never touches storage.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn the_producer_emits_a_revert_then_replays_canonical() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "producer").await;
    seed_indexed_chain(&pool).await;

    let source: Arc<dyn ChainSource> = Arc::new(ForkChain { fork_point: FORK });
    let handler: Arc<dyn ReorgHandler> =
        Arc::new(LogReorgHandler::new(Arc::clone(&source), pool.clone()));
    let (sink, mut rx) = chainscope_core::build_transport::<Envelope<BlockUnit>>(
        chainscope_core::TransportSpec::Channel { capacity: 64 },
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let producer = Producer::new(
        Arc::clone(&source),
        sink,
        Some(OLD_TIP), // resume at OLD_TIP+1 → triggers the reorg
        0,
        Duration::from_millis(5),
        cancel.clone(),
    )
    .with_reorg_handler(handler);
    let handle = tokio::spawn(producer.run());

    // First out of the seam: the revert, carrying the fork point.
    let first = rx.recv().await.unwrap().unwrap().payload;
    assert_eq!(
        first,
        Envelope::Revert { from_block: FORK },
        "the producer appends the revert before replaying the canonical branch"
    );

    // Then the canonical block just above the fork, as data. (Because this test
    // has no writer applying the undo, the producer's own reorg guard still reads
    // the stale seeded chain and will re-detect the same fork on the *next*
    // block — expected, since the undo that would advance the recorded chain is
    // the consumer's job in #60. So we assert the revert-then-canonical emission
    // and stop; full convergence with a writer is #60/#62.)
    let canonical = SyntheticChain::branched(HEAD, 1);
    let above_fork = rx.recv().await.unwrap().unwrap().payload.into_data().unwrap();
    assert_eq!(above_fork.number, FORK + 1, "replays the canonical branch from the fork");
    assert_eq!(above_fork.hash, canonical.hash_at(FORK + 1), "and it is the canonical branch");
    cancel.cancel();
    handle.await.unwrap().unwrap();

    // The producer emitted a correction; it deleted nothing. The orphaned rows
    // are still in storage — undoing them is the consumer's job (#60).
    assert_eq!(
        max_block(&pool).await,
        Some(OLD_TIP as i64),
        "producer-side storage is untouched under the log transport"
    );

    eprintln!("producer OK: emitted Revert {{ {FORK} }}, replayed canonical, deleted nothing");
    drop_db(&admin, pool, &name).await;
}
