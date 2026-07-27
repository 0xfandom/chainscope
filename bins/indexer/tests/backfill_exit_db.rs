//! The M3 exit criterion (#37): backfill is restartable, its aggregates are
//! complete over all history, and raw disk stays bounded to the retention window.
//!
//! Backfill's three promises are exactly the ones that break silently — a resume
//! that double-counts volume, or a stream-then-discard that quietly keeps
//! everything, only shows up under a real interrupted run. So this drives the
//! *real* backfill driver over the synthetic chain, aborts it at many points,
//! resumes, and asserts:
//!
//!   * killed-and-resumed converges to the same complete candle totals as a clean
//!     single pass — no gap, no double-count;
//!   * with a finite window, raw rows are bounded to the in-window blocks while
//!     the candles still cover every block.
//!
//! The synthetic chain makes the totals exact: blocks [0, 136] (one swap each,
//! `amount0 = n`, `amount1 = -n`), so 137 trades and Σ|amount| = Σ(0..=136) =
//! 9316 on each side.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test backfill_exit_db -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use bigdecimal::BigDecimal;
use std::str::FromStr;

use chainscope_core::source::ChainSource;
use chainscope_indexer::{
    backfill::BackfillDriver,
    db,
    testkit::{SyntheticChain, SYNTHETIC_POOL},
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio_util::sync::CancellationToken;

const HEIGHT: u64 = 200;
const START: u64 = 0;
const FLOOR: u64 = HEIGHT - 64; // finality floor = 136
const EXPECTED_TRADES: i64 = (FLOOR - START + 1) as i64; // 137
const EXPECTED_VOLUME: &str = "9316"; // Σ n for n in 0..=136

// The synthetic chain's base timestamp (testkit BASE_TS): block n is stamped
// BASE_TS + n seconds. Used to place a window floor partway through the range.
const BASE_TS: i64 = 1_784_851_200;

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_exit_{}_{}", std::process::id(), tag);
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    db::ensure_partitions(&pool).await.unwrap();
    // Synthetic blocks land on 2026-07-24; create that day's partitions for the
    // in-window swaps that will be stored.
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

fn driver(pool: &PgPool, chunk: u64, floor: Option<i64>, cancel: CancellationToken) -> BackfillDriver {
    let source: Arc<dyn ChainSource> = Arc::new(SyntheticChain::new(HEIGHT));
    BackfillDriver::new(source, pool.clone(), vec![SYNTHETIC_POOL], START, chunk, cancel)
        .with_window_floor(floor)
}

/// Drive the backfill to completion through many interruptions: cancel at a
/// varying point, resume, repeat until the cursor reaches the floor, then a final
/// uninterrupted pass guarantees completion.
async fn run_through_interruptions(pool: &PgPool, floor: Option<i64>) {
    for i in 0..60u64 {
        if db::load_backfill_cursor(pool).await.unwrap() == Some(FLOOR) {
            return;
        }
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(driver(pool, 1, floor, cancel.clone()).run());
        // Vary the kill delay so the abort lands at different chunk boundaries.
        tokio::time::sleep(Duration::from_millis(2 + (i % 6))).await;
        cancel.cancel();
        handle.await.unwrap().unwrap();
    }
    // Guaranteed completion, uninterrupted.
    driver(pool, 8, floor, CancellationToken::new()).run().await.unwrap();
}

/// (trade_count sum, volume0 sum, volume1 sum) across all candles.
async fn candle_totals(pool: &PgPool) -> (i64, BigDecimal, BigDecimal) {
    let r = sqlx::query(
        "SELECT COALESCE(sum(trade_count),0) AS n,
                COALESCE(sum(volume0),0)     AS v0,
                COALESCE(sum(volume1),0)     AS v1
           FROM ohlcv_1m",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    (r.get::<i64, _>("n"), r.get::<BigDecimal, _>("v0"), r.get::<BigDecimal, _>("v1"))
}

async fn swap_count(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) FROM swaps").fetch_one(pool).await.unwrap().get(0)
}

/// A clean single pass and a heavily-interrupted run reach the identical complete
/// aggregate state: every block's swap counted once, volume never double-counted.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn interrupted_backfill_converges_to_the_clean_aggregate() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };

    // Clean baseline.
    let (clean, cname) = fresh_db(&admin, "clean").await;
    driver(&clean, 8, None, CancellationToken::new()).run().await.unwrap();
    let clean_totals = candle_totals(&clean).await;
    assert_eq!(clean_totals.0, EXPECTED_TRADES, "clean: every block's swap counted once");
    assert_eq!(clean_totals.1, bd(EXPECTED_VOLUME), "clean: volume0 total");
    assert_eq!(clean_totals.2, bd(EXPECTED_VOLUME), "clean: volume1 total");
    assert_eq!(swap_count(&clean).await, EXPECTED_TRADES, "clean: raw rows (window = keep all)");

    // Interrupted run, window = keep all.
    let (rough, rname) = fresh_db(&admin, "rough").await;
    run_through_interruptions(&rough, None).await;
    let rough_totals = candle_totals(&rough).await;

    assert_eq!(rough_totals, clean_totals, "interrupted run must match the clean aggregate exactly");
    assert_eq!(db::load_backfill_cursor(&rough).await.unwrap(), Some(FLOOR));
    assert_eq!(swap_count(&rough).await, EXPECTED_TRADES, "no gap, no duplicate raw row");

    eprintln!(
        "exit criterion OK: interrupted == clean == {} trades, {} volume each side",
        EXPECTED_TRADES, EXPECTED_VOLUME
    );
    drop_db(&admin, clean, &cname).await;
    drop_db(&admin, rough, &rname).await;
}

/// With a finite window partway through the range, an interrupted backfill keeps
/// raw rows bounded to the in-window blocks while the candles still cover every
/// block — stream-then-discard holds under interruption.
#[tokio::test]
#[ignore = "requires a running Postgres"]
async fn a_finite_window_bounds_raw_disk_while_aggregates_stay_complete() {
    let Some(admin) = admin().await else {
        eprintln!("skipped: set DATABASE_URL to a running Postgres");
        return;
    };
    let (pool, name) = fresh_db(&admin, "window").await;

    // Floor at BASE_TS + 30: blocks 0..29 are below it (aggregated, not stored),
    // blocks 30..=136 are within (stored raw). 107 in-window blocks.
    let floor = BASE_TS + 30;
    let in_window = (FLOOR - 30 + 1) as i64; // 107

    run_through_interruptions(&pool, Some(floor)).await;

    // Raw is bounded to the in-window blocks — the below-window ones left none.
    assert_eq!(swap_count(&pool).await, in_window, "raw rows bounded to the window");

    // ...yet the candles cover every block, in-window and below alike.
    let totals = candle_totals(&pool).await;
    assert_eq!(totals.0, EXPECTED_TRADES, "aggregates cover all history");
    assert_eq!(totals.1, bd(EXPECTED_VOLUME), "volume0 total over all blocks");
    assert_eq!(totals.2, bd(EXPECTED_VOLUME), "volume1 total over all blocks");
    assert_eq!(db::load_backfill_cursor(&pool).await.unwrap(), Some(FLOOR));

    eprintln!(
        "windowed exit criterion OK: {in_window} raw rows kept, candles cover all {EXPECTED_TRADES} trades"
    );
    drop_db(&admin, pool, &name).await;
}
