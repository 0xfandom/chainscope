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
//! singleton, the one `blocks` table, and the raw event tables; running them
//! concurrently would let them clobber each other's cursor. They use disjoint
//! block-number ranges so a leftover row from one never confuses another.
//!
//! Unlike the M1 version, these drive the real `db::write_row_batches`, so the
//! test exercises the exact transaction the writer ships — block header, swap
//! rows, liq_event rows, and the cursor, all in one commit.

use bigdecimal::BigDecimal;
use chainscope_core::{
    types::{Hash32, SwapRow},
    RowBatch,
};
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPoolOptions::new().max_connections(2).connect(&url).await.ok()?;
    // Swaps land in day partitions; make sure the ones our timestamps fall in
    // exist before we insert.
    db::ensure_partitions(&pool).await.ok()?;
    Some(pool)
}

/// A block with no decoded rows. `block_time` is "now-ish" so its day partition
/// exists (created at migration and refreshed by `ensure_partitions`).
fn empty_block(n: u64) -> RowBatch {
    let mut h: Hash32 = [0u8; 32];
    h[..8].copy_from_slice(&n.to_be_bytes());
    let mut p: Hash32 = [0u8; 32];
    p[..8].copy_from_slice(&(n - 1).to_be_bytes());
    RowBatch {
        block_number: n,
        block_hash: h,
        parent_hash: p,
        // A fixed timestamp inside an existing day partition (2026-07-24).
        block_time: 1_784_894_400, // 2026-07-24T12:00:00Z
        swaps: vec![],
        liq_events: vec![],
    }
}

/// The same block, but carrying one swap keyed by `(tx_hash, log_index)`.
fn block_with_swap(n: u64, tx_byte: u8, log_index: u32) -> RowBatch {
    let mut b = empty_block(n);
    let mut tx: Hash32 = [0u8; 32];
    tx[0] = tx_byte;
    tx[24..].copy_from_slice(&n.to_be_bytes());
    b.swaps.push(SwapRow {
        tx_hash: tx,
        log_index,
        pool: [0xaa; 20],
        sender: [0xbb; 20],
        recipient: [0xcc; 20],
        amount0: BigDecimal::from(140586),
        amount1: BigDecimal::from(-74025266944810i64),
        sqrt_price_x96: BigDecimal::from(12345678u64),
        liquidity: BigDecimal::from(9_999u64),
        tick: 200858,
    });
    b
}

async fn reset_cursor(pool: &PgPool) {
    sqlx::query("UPDATE chain_state SET live_cursor = NULL WHERE id = 1")
        .execute(pool)
        .await
        .unwrap();
}

async fn cursor(pool: &PgPool) -> Option<i64> {
    sqlx::query("SELECT live_cursor FROM chain_state WHERE id = 1")
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<Option<i64>, _>(0)
}

async fn blocks_in(pool: &PgPool, lo: u64, hi: u64) -> i64 {
    sqlx::query("SELECT count(*) FROM blocks WHERE number BETWEEN $1 AND $2")
        .bind(lo as i64)
        .bind(hi as i64)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

async fn swaps_in(pool: &PgPool, lo: u64, hi: u64) -> i64 {
    sqlx::query("SELECT count(*) FROM swaps WHERE block_number BETWEEN $1 AND $2")
        .bind(lo as i64)
        .bind(hi as i64)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

async fn cleanup(pool: &PgPool, lo: u64, hi: u64) {
    sqlx::query("DELETE FROM swaps WHERE block_number BETWEEN $1 AND $2")
        .bind(lo as i64)
        .bind(hi as i64)
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM blocks WHERE number BETWEEN $1 AND $2")
        .bind(lo as i64)
        .bind(hi as i64)
        .execute(pool)
        .await
        .ok();
}

/// Replaying an already-processed range inserts zero rows and does not move the
/// cursor backwards — at both the block and the swap level.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn replaying_a_processed_range_is_a_no_op() {
    let Some(pool) = pool().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    reset_cursor(&pool).await;
    cleanup(&pool, 3_050, 3_110).await;

    let batch: Vec<_> = (3_100..3_105).map(|n| block_with_swap(n, 0x11, 0)).collect();

    db::write_row_batches(&pool, &batch, false).await.unwrap();
    assert_eq!(blocks_in(&pool, 3_100, 3_104).await, 5);
    assert_eq!(swaps_in(&pool, 3_100, 3_104).await, 5, "one swap per block");
    assert_eq!(cursor(&pool).await, Some(3_104));

    // Same batch again — every block and every swap conflicts, nothing new.
    db::write_row_batches(&pool, &batch, false).await.unwrap();
    assert_eq!(blocks_in(&pool, 3_100, 3_104).await, 5, "replay inserted blocks");
    assert_eq!(swaps_in(&pool, 3_100, 3_104).await, 5, "replay double-counted swaps");
    assert_eq!(cursor(&pool).await, Some(3_104), "replay moved the cursor");

    // An older range must not drag the cursor back.
    db::write_row_batches(&pool, &[empty_block(3_050)], false).await.unwrap();
    assert_eq!(cursor(&pool).await, Some(3_104), "an old batch moved the cursor backwards");

    cleanup(&pool, 3_050, 3_110).await;
}

/// A failure inside the transaction rolls back the block, its swaps, and the
/// cursor together — nothing is left behind.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_failed_transaction_rolls_back_everything_together() {
    let Some(pool) = pool().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    reset_cursor(&pool).await;
    cleanup(&pool, 3_200, 3_210).await;

    db::write_row_batches(&pool, &[empty_block(3_200)], false).await.unwrap();
    let cursor_before = cursor(&pool).await;
    assert_eq!(cursor_before, Some(3_200));

    // A batch that does all its work — blocks and swaps — then fails before commit.
    let doomed: Vec<_> = (3_201..3_210).map(|n| block_with_swap(n, 0x22, 0)).collect();
    let result = db::write_row_batches(&pool, &doomed, true).await;
    assert!(result.is_err(), "the injected failure should surface");

    // Nothing moved: block rows, swap rows, and cursor were one transaction.
    assert_eq!(blocks_in(&pool, 3_201, 3_209).await, 0, "blocks survived a rollback");
    assert_eq!(swaps_in(&pool, 3_201, 3_209).await, 0, "swaps survived a rollback");
    assert_eq!(cursor(&pool).await, cursor_before, "cursor advanced despite the rollback");

    cleanup(&pool, 3_200, 3_210).await;
}

/// A gap-free run followed by a "restart" (a second call resuming from the
/// stored cursor) leaves no gaps and no duplicates, blocks and swaps alike.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn resuming_from_the_cursor_leaves_no_gaps_and_no_duplicates() {
    let Some(pool) = pool().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    reset_cursor(&pool).await;
    cleanup(&pool, 3_300, 3_310).await;

    let first: Vec<_> = (3_300..3_305).map(|n| block_with_swap(n, 0x33, 0)).collect();
    db::write_row_batches(&pool, &first, false).await.unwrap();
    let resume = cursor(&pool).await.unwrap() as u64;
    assert_eq!(resume, 3_304);

    // "Restart": resume from the cursor inclusively; the block at the cursor is
    // re-sent and must not duplicate.
    let second: Vec<_> = (resume..3_310).map(|n| block_with_swap(n, 0x33, 0)).collect();
    db::write_row_batches(&pool, &second, false).await.unwrap();

    assert_eq!(blocks_in(&pool, 3_300, 3_309).await, 10, "gaps or duplicate blocks after resume");
    assert_eq!(swaps_in(&pool, 3_300, 3_309).await, 10, "gaps or duplicate swaps after resume");
    assert_eq!(cursor(&pool).await, Some(3_309));

    cleanup(&pool, 3_300, 3_310).await;
}
