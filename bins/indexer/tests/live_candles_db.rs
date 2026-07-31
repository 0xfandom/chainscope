//! Live-path 1m candle fold (#111), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test live_candles_db -- --ignored --nocapture

use std::str::FromStr;

use bigdecimal::BigDecimal;
use chainscope_core::{
    types::{Address20, Hash32, SwapRow},
    RowBatch,
};
use chainscope_indexer::db;
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];
const BLOCK_TIME: i64 = 1_784_894_400; // 2026-07-24T12:00:00Z
// sqrtPriceX96 values chosen so price = sqrt^2 / 2^192 is exact:
//   2^96 -> price 1 ; 2^97 -> price 4.
const SQRT_P1: &str = "79228162514264337593543950336"; // 2^96
const SQRT_P4: &str = "158456325028528675187087900672"; // 2^97

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_livecdl_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    db::ensure_partitions(&pool).await.unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS swaps_20260724 PARTITION OF swaps \
         FOR VALUES FROM ('2026-07-24') TO ('2026-07-25')",
    )
    .execute(&pool)
    .await
    .unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

/// One block with two swaps in the same minute bucket: price 1 then price 4.
fn block_two_swaps() -> RowBatch {
    let mk = |log: u32, sqrt: &str, a0: &str, a1: &str| SwapRow {
        tx_hash: {
            let mut h: Hash32 = [0; 32];
            h[0] = 0xAA;
            h[31] = log as u8;
            h
        },
        log_index: log,
        pool: POOL,
        sender: [0xff; 20],
        recipient: [0x11; 20],
        amount0: bd(a0),
        amount1: bd(a1),
        sqrt_price_x96: bd(sqrt),
        liquidity: bd("1"),
        tick: 0,
    };
    RowBatch {
        block_number: 100,
        block_hash: [0x64; 32],
        parent_hash: [0x63; 32],
        block_time: BLOCK_TIME,
        swaps: vec![
            mk(0, SQRT_P1, "10", "-20"),
            mk(1, SQRT_P4, "5", "-8"),
        ],
        liq_events: vec![],
    }
}

async fn candle(pool: &PgPool) -> (String, String, String, String, String, String, i32) {
    let row: (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, i32) =
        sqlx::query_as(
            "SELECT open, high, low, close, volume0, volume1, trade_count \
             FROM ohlcv_1m WHERE pool = $1",
        )
        .bind(POOL.as_slice())
        .fetch_one(pool)
        .await
        .unwrap();
    let n = |v: BigDecimal| v.normalized().to_string();
    (n(row.0), n(row.1), n(row.2), n(row.3), n(row.4), n(row.5), row.6)
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn live_swaps_fold_candles_and_replay_is_a_noop() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;

    db::write_row_batches(&pool, &[block_two_swaps()], false).await.unwrap();

    // open = first price (1), close = last (4), high 4, low 1, volumes summed.
    let c = candle(&pool).await;
    assert_eq!(c, ("1".into(), "4".into(), "1".into(), "4".into(), "15".into(), "28".into(), 2));

    // Replay the same block: no swaps insert, so the candle does not move.
    db::write_row_batches(&pool, &[block_two_swaps()], false).await.unwrap();
    assert_eq!(candle(&pool).await, c, "replay leaves the candle unchanged");

    drop_db(&admin, pool, &name).await;
}
