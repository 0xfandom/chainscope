//! The backfill driver's contract, against a real Postgres.
//!
//! Drives the *real* `BackfillDriver` over the synthetic chain (whose every
//! block carries one decodable swap) and asserts the three promises of #34:
//!
//!   1. a fixed range is written exactly once — right count, no duplicates;
//!   2. killed mid-run and restarted, it resumes from `backfill_cursor` with no
//!      gaps and no duplicates, and a plain replay is a no-op;
//!   3. it never touches a block above the finality floor.
//!
//! Ignored by default because it needs a Postgres it can create databases on:
//!
//!   docker compose up -d
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test backfill_db -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use chainscope_core::source::ChainSource;
use chainscope_indexer::{
    backfill::BackfillDriver,
    db,
    testkit::{SyntheticChain, SYNTHETIC_POOL},
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio_util::sync::CancellationToken;

// Synthetic height 200 → finality floor = 200 - 64 = 136. Backfilling [1, 136]
// writes 136 swaps (one per block); blocks 137..200 are above the floor and must
// never be written.
const HEIGHT: u64 = 200;
const START: u64 = 1;
const FLOOR: u64 = HEIGHT - 64;
const EXPECTED: u64 = FLOOR - START + 1;

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    // Tag per test so the three run in parallel against distinct databases.
    let name = format!("chainscope_backfill_{}_{}", std::process::id(), tag);
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#))
        .execute(admin)
        .await
        .unwrap();

    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(url.as_str())
        .await
        .unwrap();

    db::migrate(&pool).await.unwrap();
    db::ensure_partitions(&pool).await.unwrap();
    // The synthetic chain stamps blocks on 2026-07-24, which is outside the
    // rolling window ensure_partitions covers once the calendar moves past it,
    // so create that day's partitions explicitly. 200 one-second steps stay
    // within the single day.
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
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
}

async fn swap_count(pool: &PgPool) -> u64 {
    let n: i64 = sqlx::query("SELECT count(*) FROM swaps")
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0);
    n as u64
}

async fn distinct_swap_count(pool: &PgPool) -> u64 {
    let n: i64 = sqlx::query("SELECT count(*) FROM (SELECT DISTINCT tx_hash, log_index FROM swaps) t")
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0);
    n as u64
}

fn driver(pool: &PgPool, chunk: u64, cancel: CancellationToken) -> BackfillDriver {
    let source: Arc<dyn ChainSource> = Arc::new(SyntheticChain::new(HEIGHT));
    BackfillDriver::new(source, pool.clone(), vec![SYNTHETIC_POOL], START, chunk, cancel)
}

/// A clean backfill writes every block's rows exactly once, stops at the finality
/// floor, and a second run over the finished range changes nothing.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_clean_backfill_is_exactly_once_and_bounded_at_the_floor() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "clean").await;

    driver(&pool, 10, CancellationToken::new()).run().await.unwrap();

    // Exactly once.
    assert_eq!(swap_count(&pool).await, EXPECTED, "one swap per block in [1, floor]");
    assert_eq!(distinct_swap_count(&pool).await, EXPECTED, "no duplicate rows");

    // Bounded at the finality floor: nothing above it, and the top block is it.
    let max_block: i64 = sqlx::query("SELECT max(block_number) FROM swaps")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(max_block as u64, FLOOR, "highest indexed block is the floor");
    let above: i64 = sqlx::query("SELECT count(*) FROM swaps WHERE block_number > $1")
        .bind(FLOOR as i64)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(above, 0, "no reorg-eligible block above the floor was touched");

    // Cursor names the floor.
    assert_eq!(db::load_backfill_cursor(&pool).await.unwrap(), Some(FLOOR));

    // Re-running over the finished range is a no-op.
    driver(&pool, 10, CancellationToken::new()).run().await.unwrap();
    assert_eq!(swap_count(&pool).await, EXPECTED, "a completed backfill re-run adds nothing");

    drop_db(&admin, pool, &name).await;
    eprintln!("clean backfill OK: {EXPECTED} swaps, none above floor {FLOOR}, re-run a no-op");
}

/// Killed mid-run and resumed, the backfill converges to the same complete state
/// with no gap and no duplicate; rows and cursor stay consistent at the kill
/// boundary.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn an_interrupted_backfill_resumes_cleanly() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "interrupted").await;

    // Start a run and cancel it shortly after, so it stops at some chunk boundary.
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(driver(&pool, 1, cancel.clone()).run());
    tokio::time::sleep(Duration::from_millis(8)).await;
    cancel.cancel();
    handle.await.unwrap().unwrap();

    // At whatever boundary it stopped, rows and cursor agree: because every block
    // is active and a chunk is atomic, swaps == cursor - start + 1 exactly. No
    // partial chunk, no duplicate.
    let cursor = db::load_backfill_cursor(&pool).await.unwrap();
    let partial = swap_count(&pool).await;
    let expected_partial = cursor.map(|c| c - START + 1).unwrap_or(0);
    assert_eq!(partial, expected_partial, "rows and cursor consistent at the kill boundary");
    assert_eq!(distinct_swap_count(&pool).await, partial, "no duplicate rows mid-run");

    // Resume to completion: exactly the full set, no gap, no double-count.
    driver(&pool, 1, CancellationToken::new()).run().await.unwrap();
    assert_eq!(swap_count(&pool).await, EXPECTED, "resume fills the range completely");
    assert_eq!(distinct_swap_count(&pool).await, EXPECTED, "resume introduced no duplicate");
    assert_eq!(db::load_backfill_cursor(&pool).await.unwrap(), Some(FLOOR));

    eprintln!("interrupted backfill OK: stopped at {cursor:?} ({partial} rows), resumed to {EXPECTED}");
    drop_db(&admin, pool, &name).await;
}

/// A resume that re-runs already-written blocks (a cursor rewound below the
/// high-water mark) double-counts nothing — the ON CONFLICT / GREATEST replay
/// safety the write transaction guarantees.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn replaying_written_blocks_double_counts_nothing() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "replay").await;

    // Full run first.
    driver(&pool, 10, CancellationToken::new()).run().await.unwrap();
    assert_eq!(swap_count(&pool).await, EXPECTED);

    // Rewind the cursor into the middle, as a resume after a lost-progress restart
    // would, so the next run re-fetches and re-writes blocks that already exist.
    sqlx::query("UPDATE chain_state SET backfill_cursor = 70 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();

    // Re-run: [71, floor] is re-written over existing rows.
    driver(&pool, 10, CancellationToken::new()).run().await.unwrap();

    assert_eq!(swap_count(&pool).await, EXPECTED, "replayed blocks must not double-count");
    assert_eq!(distinct_swap_count(&pool).await, EXPECTED);
    assert_eq!(db::load_backfill_cursor(&pool).await.unwrap(), Some(FLOOR), "cursor back to the floor");

    eprintln!("replay OK: rewound cursor to 70, re-ran, still exactly {EXPECTED} swaps");
    drop_db(&admin, pool, &name).await;
}
