//! OHLCV 1m candle aggregation (#36), against a real Postgres.
//!
//! Proves the candle fold that runs inside the bulk write:
//!   * a bucket's open/high/low/close/volume/count match a hand computation;
//!   * replaying a chunk leaves the candle unchanged (no double-counted volume) —
//!     the RETURNING discipline;
//!   * a bucket touched by two writes keeps its true open and accumulates volume;
//!   * a below-window swap updates the candle while leaving no raw row;
//!   * a minute with no swaps has no candle (a gap is absence, not a zero row).
//!
//! Prices are exact here on purpose: sqrtPriceX96 = k * 2^96 gives price
//! (sqrt/2^96)^2 = k^2, an exact integer, so the asserts need no float slack.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test candles_db -- --ignored --nocapture

use bigdecimal::BigDecimal;
use std::str::FromStr;

use chainscope_core::{
    types::{Address20, Hash32},
    RowBatch, SwapRow,
};
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

// 2^96, the Uniswap V3 Q96 fixed-point one.
const Q96: &str = "79228162514264337593543950336";
const POOL: Address20 = [0x11; 20];

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

/// sqrtPriceX96 = k * 2^96, so the derived price is exactly k^2.
fn sqrt_for_price_k2(k: u32) -> BigDecimal {
    bd(Q96) * BigDecimal::from(k)
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_candle_{}_{}", std::process::id(), tag);
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
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

async fn now_epoch(pool: &PgPool) -> i64 {
    sqlx::query("SELECT extract(epoch FROM now())::bigint").fetch_one(pool).await.unwrap().get(0)
}

/// One swap in a one-swap block. `k` sets the price to k^2; amounts drive volume.
fn swap_block(block_number: u64, block_time: i64, k: u32, amount0: &str, amount1: &str) -> RowBatch {
    let mut tx_hash: Hash32 = [0u8; 32];
    tx_hash[24..].copy_from_slice(&block_number.to_be_bytes());
    let mut block_hash = [0u8; 32];
    block_hash[..8].copy_from_slice(&block_number.to_be_bytes());
    RowBatch {
        block_number,
        block_hash,
        parent_hash: [0u8; 32],
        block_time,
        swaps: vec![SwapRow {
            tx_hash,
            log_index: 0,
            pool: POOL,
            sender: [0xAA; 20],
            recipient: [0xBB; 20],
            amount0: bd(amount0),
            amount1: bd(amount1),
            sqrt_price_x96: sqrt_for_price_k2(k),
            liquidity: bd("1000"),
            tick: 0,
        }],
        liq_events: vec![],
    }
}

struct Candle {
    open: BigDecimal,
    high: BigDecimal,
    low: BigDecimal,
    close: BigDecimal,
    volume0: BigDecimal,
    volume1: BigDecimal,
    trade_count: i32,
}

async fn only_candle(pool: &PgPool) -> Candle {
    let r = sqlx::query(
        "SELECT open, high, low, close, volume0, volume1, trade_count FROM ohlcv_1m",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    Candle {
        open: r.get("open"),
        high: r.get("high"),
        low: r.get("low"),
        close: r.get("close"),
        volume0: r.get("volume0"),
        volume1: r.get("volume1"),
        trade_count: r.get("trade_count"),
    }
}

async fn candle_row_count(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) FROM ohlcv_1m").fetch_one(pool).await.unwrap().get(0)
}
async fn swap_count(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) FROM swaps").fetch_one(pool).await.unwrap().get(0)
}

// Three swaps in one minute: prices 4, 25, 1 in block order; so open=4 (first),
// close=1 (last), high=25, low=1. Volume0 = 10+20+3 = 33, volume1 = 5+8+7 = 20.
fn one_minute_batches(t: i64) -> Vec<RowBatch> {
    vec![
        swap_block(100, t, 2, "10", "-5"),
        swap_block(101, t, 5, "20", "-8"),
        swap_block(102, t, 1, "-3", "7"),
    ]
}

fn assert_expected(c: &Candle) {
    assert_eq!(c.open, bd("4"), "open is the first swap's price");
    assert_eq!(c.high, bd("25"), "high is the max price");
    assert_eq!(c.low, bd("1"), "low is the min price");
    assert_eq!(c.close, bd("1"), "close is the last swap's price");
    assert_eq!(c.volume0, bd("33"), "volume0 sums |amount0|");
    assert_eq!(c.volume1, bd("20"), "volume1 sums |amount1|");
    assert_eq!(c.trade_count, 3);
}

/// A minute's candle matches a hand-computed OHLCV, and only that one bucket
/// exists (a quiet minute produces no candle).
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_candle_matches_a_hand_computed_ohlcv() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "hand").await;
    let t = now_epoch(&pool).await;

    db::bulk_write_backfill(&pool, &one_minute_batches(t), 102, None, false).await.unwrap();

    assert_expected(&only_candle(&pool).await);
    assert_eq!(candle_row_count(&pool).await, 1, "one bucket, none for empty minutes");

    eprintln!("candle OHLCV OK: open4 high25 low1 close1 vol0=33 vol1=20 n=3");
    drop_db(&admin, pool, &name).await;
}

/// Replaying the same chunk leaves the candle exactly as it was — volume is not
/// double-counted, because the fold is keyed off the rows that actually inserted.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn replaying_a_chunk_does_not_double_count_volume() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "replay").await;
    let t = now_epoch(&pool).await;

    db::bulk_write_backfill(&pool, &one_minute_batches(t), 102, None, false).await.unwrap();
    // Same chunk again — inserts nothing, so it must fold nothing.
    db::bulk_write_backfill(&pool, &one_minute_batches(t), 102, None, false).await.unwrap();

    assert_expected(&only_candle(&pool).await);
    assert_eq!(candle_row_count(&pool).await, 1);

    eprintln!("candle replay OK: volume unchanged after re-running the chunk");
    drop_db(&admin, pool, &name).await;
}

/// A bucket split across two writes keeps its true open (the earliest swap) and
/// accumulates volume — the same final candle as one combined write.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_bucket_spanning_two_writes_keeps_its_open_and_accumulates() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "split").await;
    let t = now_epoch(&pool).await;

    // First write: the first two swaps of the minute.
    db::bulk_write_backfill(&pool, &[swap_block(100, t, 2, "10", "-5"), swap_block(101, t, 5, "20", "-8")], 101, None, false)
        .await
        .unwrap();
    // Second write: the last swap, same minute, later block.
    db::bulk_write_backfill(&pool, &[swap_block(102, t, 1, "-3", "7")], 102, None, false)
        .await
        .unwrap();

    // Identical to writing all three at once.
    assert_expected(&only_candle(&pool).await);

    eprintln!("candle split-write OK: open held, close advanced, volume accumulated");
    drop_db(&admin, pool, &name).await;
}

/// A below-window swap updates the candle but leaves no raw row — the aggregate
/// covers history the raw table no longer holds.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_below_window_swap_still_feeds_the_candle() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "belowwin").await;
    let t = now_epoch(&pool).await;

    let day = 86_400;
    let floor = t - 30 * day;
    // One swap, 100 days old — below the floor. price = 3^2 = 9.
    let old = swap_block(500, t - 100 * day, 3, "42", "-17");
    let stats = db::bulk_write_backfill(&pool, &[old], 500, Some(floor), false).await.unwrap();

    assert_eq!(stats.persisted, 0, "below-window swap is not stored raw");
    assert_eq!(stats.discarded, 1);
    assert_eq!(swap_count(&pool).await, 0, "no raw row on disk");

    // ...but its candle exists.
    let c = only_candle(&pool).await;
    assert_eq!(c.open, bd("9"));
    assert_eq!(c.close, bd("9"));
    assert_eq!(c.volume0, bd("42"));
    assert_eq!(c.volume1, bd("17"));
    assert_eq!(c.trade_count, 1);

    eprintln!("below-window candle OK: aggregate present, raw absent");
    drop_db(&admin, pool, &name).await;
}
