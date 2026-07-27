//! The bulk COPY write path and stream-then-discard window gating (#35).
//!
//! Three things this proves against a real Postgres:
//!   1. the COPY-through-staging path stores values *exactly* — the text
//!      encoding and the `::numeric` / `decode(_, 'hex')` casts round-trip a real
//!      Etherscan swap byte-for-byte;
//!   2. the retention window gates raw persistence — a block older than the floor
//!      is counted (`discarded`) but leaves no raw row, while a block within the
//!      window is stored (`persisted`);
//!   3. a failure before commit rolls the rows and the cursor back together.
//!
//! Ignored by default (needs a Postgres it can create databases on):
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test bulk_copy_db -- --ignored --nocapture

use bigdecimal::BigDecimal;
use std::str::FromStr;

use chainscope_core::{
    types::{Address20, Hash32},
    RowBatch, SwapRow,
};
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}
fn addr(hex: &str) -> Address20 {
    let mut out = [0u8; 20];
    hex::decode_to_slice(hex, &mut out).unwrap();
    out
}
fn h(hex: &str) -> Hash32 {
    let mut out = [0u8; 32];
    hex::decode_to_slice(hex, &mut out).unwrap();
    out
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_bulk_{}_{}", std::process::id(), tag);
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
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    db::ensure_partitions(&pool).await.unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
}

/// The DB's own "now" in unix seconds, so block_time lands in a day the default
/// partitions cover.
async fn now_epoch(pool: &PgPool) -> i64 {
    sqlx::query("SELECT extract(epoch FROM now())::bigint")
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0)
}

async fn swap_count(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) FROM swaps").fetch_one(pool).await.unwrap().get(0)
}

// A real mainnet swap (block 25601357, USDC/WETH 0.05%) — the same values
// decode_parity pins against Etherscan, reused here to prove the COPY path is
// exact, not just the per-row path.
fn real_swap_row() -> SwapRow {
    SwapRow {
        tx_hash: h("e18a03325588278d1d9605c762339598b31f34a5f8b2fd62a7ff0bfed60eb5dc"),
        log_index: 39,
        pool: addr("8ad599c3a0ff1de082011efddc58f1908eb6e6d8"),
        sender: addr("06cff7088619c7178f5e14f0b119458d08d2f5ef"),
        recipient: addr("06cff7088619c7178f5e14f0b119458d08d2f5ef"),
        amount0: bd("140586"),
        amount1: bd("-74025266944810"),
        sqrt_price_x96: bd("1820754252512732283398282170500178"),
        liquidity: bd("1620197336976127727"),
        tick: 200858,
    }
}

fn batch(block_number: u64, block_time: i64, swaps: Vec<SwapRow>) -> RowBatch {
    let mut hash = [0u8; 32];
    hash[..8].copy_from_slice(&block_number.to_be_bytes());
    RowBatch {
        block_number,
        block_hash: hash,
        parent_hash: [0u8; 32],
        block_time,
        swaps,
        liq_events: vec![],
    }
}

/// The COPY path stores a real swap byte-for-byte: negative amounts, the huge
/// sqrtPriceX96, and the tick all survive the text encoding and the numeric cast.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn the_copy_path_stores_a_real_swap_exactly() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "exact").await;
    let now = now_epoch(&pool).await;

    let b = batch(25_601_357, now, vec![real_swap_row()]);
    let stats = db::bulk_write_backfill(&pool, std::slice::from_ref(&b), 25_601_357, None, false)
        .await
        .unwrap();
    assert_eq!(stats.persisted, 1);
    assert_eq!(stats.discarded, 0);

    let r = sqlx::query(
        "SELECT encode(pool,'hex') AS pool, encode(sender,'hex') AS sender, amount0, amount1,
                sqrt_price_x96, liquidity, tick
           FROM swaps WHERE log_index = 39",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(r.get::<String, _>("pool"), "8ad599c3a0ff1de082011efddc58f1908eb6e6d8");
    assert_eq!(r.get::<String, _>("sender"), "06cff7088619c7178f5e14f0b119458d08d2f5ef");
    assert_eq!(r.get::<BigDecimal, _>("amount0"), bd("140586"));
    assert_eq!(r.get::<BigDecimal, _>("amount1"), bd("-74025266944810"), "sign survives COPY");
    assert_eq!(r.get::<BigDecimal, _>("sqrt_price_x96"), bd("1820754252512732283398282170500178"));
    assert_eq!(r.get::<BigDecimal, _>("liquidity"), bd("1620197336976127727"));
    assert_eq!(r.get::<i32, _>("tick"), 200858);

    // Idempotent: the same COPY again inserts nothing (staging + ON CONFLICT).
    db::bulk_write_backfill(&pool, std::slice::from_ref(&b), 25_601_357, None, false)
        .await
        .unwrap();
    assert_eq!(swap_count(&pool).await, 1, "a replayed COPY chunk double-counts nothing");

    eprintln!("COPY exactness OK: real swap round-trips byte-for-byte, replay a no-op");
    drop_db(&admin, pool, &name).await;
}

/// Stream-then-discard: a block older than the window floor leaves no raw row but
/// is counted; a block within the window is persisted.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn below_the_window_floor_leaves_no_raw_row() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "window").await;
    let now = now_epoch(&pool).await;

    let day = 86_400;
    let floor = now - 30 * day; // keep the last 30 days of raw
    let within = batch(2000, now, vec![real_swap_row()]);
    // An old block, 100 days back — below the floor. Its own day has no partition,
    // which is fine precisely because its raw row must not be written.
    let mut old_swap = real_swap_row();
    old_swap.tx_hash = h("00000000000000000000000000000000000000000000000000000000deadbe01");
    old_swap.log_index = 0;
    let below = batch(1000, now - 100 * day, vec![old_swap]);

    let stats = db::bulk_write_backfill(&pool, &[below, within], 2000, Some(floor), false)
        .await
        .unwrap();

    assert_eq!(stats.persisted, 1, "only the in-window swap is stored");
    assert_eq!(stats.discarded, 1, "the below-floor swap is counted, not stored");
    assert_eq!(swap_count(&pool).await, 1, "exactly one raw row on disk");

    // The one row on disk is the in-window one.
    let li: i32 = sqlx::query("SELECT log_index FROM swaps")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(li, 39, "the stored row is the in-window swap");

    // The cursor still advances to the covered top, over the discarded block.
    assert_eq!(db::load_backfill_cursor(&pool).await.unwrap(), Some(2000));

    eprintln!("window gating OK: 1 persisted, 1 discarded, cursor advanced past both");
    drop_db(&admin, pool, &name).await;
}

/// A failure before commit rolls the COPYed rows and the cursor back together.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_failure_before_commit_rolls_rows_and_cursor_back() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "rollback").await;
    let now = now_epoch(&pool).await;

    let b = batch(3000, now, vec![real_swap_row()]);
    let err = db::bulk_write_backfill(&pool, std::slice::from_ref(&b), 3000, None, true).await;
    assert!(err.is_err(), "fail_before_commit must surface an error");

    assert_eq!(swap_count(&pool).await, 0, "no row survives a rolled-back COPY");
    assert_eq!(db::load_backfill_cursor(&pool).await.unwrap(), None, "cursor untouched");

    eprintln!("rollback OK: mid-COPY failure leaves no rows and no cursor advance");
    drop_db(&admin, pool, &name).await;
}
