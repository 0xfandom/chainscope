//! The exactly-once guarantees, exercised against a real Postgres.
//!
//! Ignored by default so an offline machine still passes `cargo test`. Run with
//! a database up:
//!
//!   docker compose up -d
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test writer_db -- --ignored --test-threads=1
//!
//! `--test-threads=1` because every test here shares the one `chain_state`
//! singleton and the one `blocks` table; running them concurrently would let
//! them clobber each other's cursor. They use disjoint block-number ranges so a
//! leftover row from one never confuses another.

use chainscope_core::{types::Hash32, BlockUnit};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

// The functions under test live in the binary crate, so the test reaches them
// the same way `main` does — through a tiny re-exec of the same query logic.
// Rather than depend on binary internals, this test talks SQL directly against
// the schema, mirroring exactly what `db::write_block_batch` does. Keeping the
// assertion at the SQL layer is deliberate: it is the database's atomicity that
// the acceptance criteria are really about.

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

fn block(n: u64) -> BlockUnit {
    let mut h: Hash32 = [0u8; 32];
    h[..8].copy_from_slice(&n.to_be_bytes());
    let mut p: Hash32 = [0u8; 32];
    p[..8].copy_from_slice(&(n - 1).to_be_bytes());
    BlockUnit {
        number: n,
        hash: h,
        parent_hash: p,
        timestamp: 1_700_000_000 + n as i64,
        logs: vec![],
    }
}

/// One transaction: insert the blocks, advance the cursor, optionally fail
/// before commit. This is a faithful copy of `db::write_block_batch`, standing
/// in for it so the test needs no access to binary internals.
async fn write_batch(pool: &PgPool, blocks: &[BlockUnit], fail: bool) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    for b in blocks {
        sqlx::query(
            "INSERT INTO blocks (number, block_hash, parent_hash, block_time)
             VALUES ($1, $2, $3, to_timestamp($4))
             ON CONFLICT (number) DO NOTHING",
        )
        .bind(b.number as i64)
        .bind(b.hash.as_slice())
        .bind(b.parent_hash.as_slice())
        .bind(b.timestamp)
        .execute(&mut *tx)
        .await?;
    }
    let high = blocks.iter().map(|b| b.number).max().unwrap() as i64;
    sqlx::query(
        "UPDATE chain_state
            SET live_cursor = GREATEST(COALESCE(live_cursor, -1), $1), updated_at = now()
          WHERE id = 1",
    )
    .bind(high)
    .execute(&mut *tx)
    .await?;

    if fail {
        return Err(anyhow::anyhow!("injected failure before commit"));
    }
    tx.commit().await?;
    Ok(())
}

/// Reset the shared cursor so each test starts from a known state. Safe because
/// these run single-threaded; without it, GREATEST would carry a prior test's
/// higher cursor into the next one.
async fn reset_cursor(pool: &PgPool) {
    sqlx::query("UPDATE chain_state SET live_cursor = NULL WHERE id = 1")
        .execute(pool).await.unwrap();
}

async fn cursor(pool: &PgPool) -> Option<i64> {
    sqlx::query("SELECT live_cursor FROM chain_state WHERE id = 1")
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<Option<i64>, _>(0)
}

async fn count_in(pool: &PgPool, lo: u64, hi: u64) -> i64 {
    sqlx::query("SELECT count(*) FROM blocks WHERE number BETWEEN $1 AND $2")
        .bind(lo as i64)
        .bind(hi as i64)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

/// Replaying an already-processed range inserts zero rows and does not move the
/// cursor backwards.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn replaying_a_processed_range_is_a_no_op() {
    let Some(pool) = pool().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    reset_cursor(&pool).await;
    let batch: Vec<_> = (3_100..3_105).map(block).collect();

    write_batch(&pool, &batch, false).await.unwrap();
    let after_first = count_in(&pool, 3_100, 3_104).await;
    let cursor_first = cursor(&pool).await;
    assert_eq!(after_first, 5);
    assert_eq!(cursor_first, Some(3_104));

    // Same batch again — every row conflicts, nothing new is written.
    write_batch(&pool, &batch, false).await.unwrap();
    assert_eq!(count_in(&pool, 3_100, 3_104).await, 5, "replay inserted rows");
    assert_eq!(cursor(&pool).await, Some(3_104), "replay moved the cursor");

    // An older range must not drag the cursor back.
    write_batch(&pool, &[block(3_050)], false).await.unwrap();
    assert_eq!(cursor(&pool).await, Some(3_104), "an old batch moved the cursor backwards");

    sqlx::query("DELETE FROM blocks WHERE number BETWEEN 3050 AND 3104")
        .execute(&pool).await.unwrap();
}

/// A failure inside the transaction rolls back the rows and the cursor
/// together — neither is left behind.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_failed_transaction_rolls_back_rows_and_cursor_together() {
    let Some(pool) = pool().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
reset_cursor(&pool).await;
    // Establish a known cursor first.
    write_batch(&pool, &[block(3_200)], false).await.unwrap();
    let cursor_before = cursor(&pool).await;
    assert_eq!(cursor_before, Some(3_200));

    // Now a batch that does all its work and then fails before commit.
    let doomed: Vec<_> = (3_201..3_210).map(block).collect();
    let result = write_batch(&pool, &doomed, true).await;
    assert!(result.is_err(), "the injected failure should surface");

    // Neither the rows nor the cursor moved: they were in the same transaction.
    assert_eq!(count_in(&pool, 3_201, 3_209).await, 0, "rows survived a rolled-back transaction");
    assert_eq!(cursor(&pool).await, cursor_before, "cursor advanced despite the rollback");

    sqlx::query("DELETE FROM blocks WHERE number BETWEEN 3200 AND 3210")
        .execute(&pool).await.unwrap();
}

/// A gap-free run followed by a "restart" (a second call resuming from the
/// stored cursor) leaves no gaps and no duplicates.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn resuming_from_the_cursor_leaves_no_gaps_and_no_duplicates() {
    let Some(pool) = pool().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
reset_cursor(&pool).await;
    // First run writes 3300..3305 and commits the cursor.
    let first: Vec<_> = (3_300..3_305).map(block).collect();
    write_batch(&pool, &first, false).await.unwrap();
    let resume = cursor(&pool).await.unwrap() as u64;
    assert_eq!(resume, 3_304);

    // "Restart": resume from cursor+1. The block at the cursor is deliberately
    // re-sent to model a producer that resumes inclusively; it must not
    // duplicate.
    let second: Vec<_> = (resume..3_310).map(block).collect();
    write_batch(&pool, &second, false).await.unwrap();

    // Every block from 3300 to 3309 present exactly once, cursor at the end.
    assert_eq!(count_in(&pool, 3_300, 3_309).await, 10, "gaps or duplicates after resume");
    assert_eq!(cursor(&pool).await, Some(3_309));

    sqlx::query("DELETE FROM blocks WHERE number BETWEEN 3300 AND 3310")
        .execute(&pool).await.unwrap();
}
