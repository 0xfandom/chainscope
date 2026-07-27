//! Candle compensation on a reorg (#47), against a real Postgres.
//!
//! When a rewind (#46) deletes orphaned swaps, the candles those swaps fed must
//! stop reflecting them. This proves the compensation that runs inside the same
//! rewind transaction:
//!   * a bucket split across the fork is recomputed from its survivors —
//!     open/high/low/close, not just volume;
//!   * a bucket whose every swap was orphaned has its candle deleted (a true gap
//!     again), while a bucket entirely below the fork is left untouched;
//!   * a reorg-then-reindex lands the same candle as a clean single index of the
//!     canonical branch.
//!
//! Prices are exact by construction: sqrtPriceX96 = k * 2^96 gives price k^2.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test reorg_candles_db -- --ignored --nocapture

use bigdecimal::BigDecimal;
use std::str::FromStr;

use chainscope_core::{
    types::{Address20, Hash32},
    RowBatch, SwapRow,
};
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

const Q96: &str = "79228162514264337593543950336"; // 2^96
const POOL: Address20 = [0x11; 20];

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}
fn sqrt_for_price_k2(k: u32) -> BigDecimal {
    bd(Q96) * BigDecimal::from(k)
}

/// One swap in a one-swap block. `k` sets the price to k^2.
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

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_rc_{}_{}", std::process::id(), tag);
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
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

struct Candle {
    open: BigDecimal,
    high: BigDecimal,
    low: BigDecimal,
    close: BigDecimal,
    volume0: BigDecimal,
    trade_count: i32,
}

/// The candle for a given minute bucket, if any.
async fn candle_at(pool: &PgPool, bucket_epoch: i64) -> Option<Candle> {
    let r = sqlx::query(
        "SELECT open, high, low, close, volume0, trade_count
           FROM ohlcv_1m WHERE bucket = date_trunc('minute', to_timestamp($1))",
    )
    .bind(bucket_epoch)
    .fetch_optional(pool)
    .await
    .unwrap()?;
    Some(Candle {
        open: r.get("open"),
        high: r.get("high"),
        low: r.get("low"),
        close: r.get("close"),
        volume0: r.get("volume0"),
        trade_count: r.get("trade_count"),
    })
}

async fn candle_count(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) FROM ohlcv_1m").fetch_one(pool).await.unwrap().get(0)
}

/// A bucket split across the fork is rebuilt from its survivors — including a new
/// low and close the deleted trades used to own.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_split_bucket_is_recomputed_from_survivors() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "split").await;
    let t = now_epoch(&pool).await;

    // One minute, four swaps: 120 & 121 survive (fork=125), 126 & 127 orphaned.
    // Survivors' prices: 4 (block 120), 25 (block 121). Orphans: 9, 1.
    let batches = vec![
        swap_block(120, t, 2, "10", "-5"),
        swap_block(121, t, 5, "20", "-8"),
        swap_block(126, t, 3, "7", "-3"),
        swap_block(127, t, 1, "4", "-2"),
    ];
    db::bulk_write_backfill(&pool, &batches, 127, None, false).await.unwrap();

    // Before: open=4, high=25, low=1 (block 127), close=1, vol0=41, count=4.
    let before = candle_at(&pool, t).await.unwrap();
    assert_eq!(before.low, bd("1"));
    assert_eq!(before.close, bd("1"));
    assert_eq!(before.trade_count, 4);

    db::rewind_to(&pool, 125, false).await.unwrap();

    // After: only 120 & 121 remain. open=4, high=25, low=4, close=25, vol0=30.
    let c = candle_at(&pool, t).await.unwrap();
    assert_eq!(c.open, bd("4"), "open unchanged — earliest survivor");
    assert_eq!(c.high, bd("25"));
    assert_eq!(c.low, bd("4"), "the low the orphan owned is gone");
    assert_eq!(c.close, bd("25"), "close is the latest survivor, not the orphan");
    assert_eq!(c.volume0, bd("30"), "10 + 20, the orphaned 7 + 4 removed");
    assert_eq!(c.trade_count, 2);

    eprintln!("split recompute OK: low 1->4, close 1->25, vol 41->30, n 4->2");
    drop_db(&admin, pool, &name).await;
}

/// A fully-orphaned bucket's candle is deleted; a bucket entirely below the fork
/// is untouched.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn orphaned_candle_is_deleted_and_safe_candle_is_kept() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "gap").await;
    let t = now_epoch(&pool).await;
    let safe_minute = t - 600; // ten minutes earlier, a different bucket
    let orphan_minute = t;

    let batches = vec![
        // Safe bucket, blocks below the fork.
        swap_block(100, safe_minute, 2, "10", "-5"),
        swap_block(101, safe_minute, 3, "6", "-1"),
        // Orphan bucket, every swap above the fork.
        swap_block(130, orphan_minute, 4, "8", "-8"),
        swap_block(131, orphan_minute, 5, "9", "-9"),
    ];
    db::bulk_write_backfill(&pool, &batches, 131, None, false).await.unwrap();
    assert_eq!(candle_count(&pool).await, 2, "two buckets before the reorg");

    db::rewind_to(&pool, 125, false).await.unwrap();

    assert!(candle_at(&pool, orphan_minute).await.is_none(), "fully-orphaned candle gone");
    let safe = candle_at(&pool, safe_minute).await.expect("safe candle survives");
    assert_eq!(safe.open, bd("4"), "untouched: open k=2 -> 4");
    assert_eq!(safe.trade_count, 2);
    assert_eq!(candle_count(&pool).await, 1, "only the safe bucket remains");

    eprintln!("gap + untouched OK: orphan candle deleted, safe candle intact");
    drop_db(&admin, pool, &name).await;
}

/// Reorg-then-reindex lands the same candle as a clean single index of the
/// canonical branch — the acceptance for compensation.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_reorg_then_reindex_matches_a_clean_index() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };

    // Shared prefix (survives): block 120 price 4, 121 price 25. Canonical above
    // the fork: 126 price 36, 127 price 49. The old branch had different trades
    // at 126.
    let shared = |t: i64| vec![swap_block(120, t, 2, "10", "-5"), swap_block(121, t, 5, "20", "-8")];
    let canonical_tail =
        |t: i64| vec![swap_block(126, t, 6, "3", "-3"), swap_block(127, t, 7, "5", "-5")];

    // --- reorg path: index shared + an OLD tail, rewind, index the canonical tail
    let (reorg, rn) = fresh_db(&admin, "reorg").await;
    let t = now_epoch(&reorg).await;
    let mut old = shared(t);
    old.push(swap_block(126, t, 9, "99", "-99")); // old branch: different price and volume
    db::bulk_write_backfill(&reorg, &old, 126, None, false).await.unwrap();
    db::rewind_to(&reorg, 125, false).await.unwrap();
    db::bulk_write_backfill(&reorg, &canonical_tail(t), 127, None, false).await.unwrap();

    // --- clean path: index shared + canonical tail directly
    let (clean, cn) = fresh_db(&admin, "clean").await;
    let t2 = now_epoch(&clean).await;
    let mut all = shared(t2);
    all.extend(canonical_tail(t2));
    db::bulk_write_backfill(&clean, &all, 127, None, false).await.unwrap();

    // Same candle both ways (compare on the fields, buckets differ only by the
    // per-database `now()`).
    let r = candle_at(&reorg, t).await.unwrap();
    let c = candle_at(&clean, t2).await.unwrap();
    assert_eq!(r.open, c.open, "open");
    assert_eq!(r.high, c.high, "high");
    assert_eq!(r.low, c.low, "low");
    assert_eq!(r.close, c.close, "close");
    assert_eq!(r.volume0, c.volume0, "volume0");
    assert_eq!(r.trade_count, c.trade_count, "trade_count");
    assert_eq!(c.trade_count, 4, "shared + canonical tail");

    eprintln!("reorg-then-reindex == clean index OK: open={}, close={}, n={}", c.open, c.close, c.trade_count);
    drop_db(&admin, reorg, &rn).await;
    drop_db(&admin, clean, &cn).await;
}
