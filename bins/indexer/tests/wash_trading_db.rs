//! Wash-trade flagging (#74), against a real Postgres.
//!
//! Drives the real fold to build wallet stats, then `flag_wash_trading`, and
//! asserts a self-trader and a churner are excluded while a normal trader in the
//! same run is not. The flag is a pure function of the swaps, so re-running
//! converges.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test wash_trading_db -- --ignored --nocapture

use std::str::FromStr;

use bigdecimal::BigDecimal;
use chainscope_core::{
    types::{Address20, Hash32, SwapRow},
    RowBatch,
};
use chainscope_indexer::{
    db::{self, WashParams},
    pnl::Numeraire,
};
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];
const TOKEN: Address20 = [0x70; 20];
const USDC: Address20 = [0xdc; 20];
const CLEAN: Address20 = [0x01; 20];
const SELFY: Address20 = [0x02; 20]; // self-trader
const CHURN: Address20 = [0x03; 20]; // churner
const BLOCK_TIME: i64 = 1_784_894_400;

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

fn numeraire() -> Numeraire {
    Numeraire {
        stables: [USDC].into_iter().collect(),
        weth: None,
        weth_price_pool: None,
    }
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_wash_{}", std::process::id());
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
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS swaps_20260724 PARTITION OF swaps \
         FOR VALUES FROM ('2026-07-24') TO ('2026-07-25')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO pools (address, token0, token1, fee, tick_spacing, \
             token0_decimals, token1_decimals, is_indexed) \
         VALUES ($1,$2,$3,3000,60,18,6,true)",
    )
    .bind(POOL.as_slice())
    .bind(TOKEN.as_slice())
    .bind(USDC.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(admin)
        .await
        .ok();
}

#[allow(clippy::too_many_arguments)]
fn swap(
    block: u64,
    tx_byte: u8,
    sender: Address20,
    recipient: Address20,
    amount0: &str,
    amount1: &str,
) -> RowBatch {
    let mut h: Hash32 = [0; 32];
    h[..8].copy_from_slice(&block.to_be_bytes());
    let mut p: Hash32 = [0; 32];
    p[..8].copy_from_slice(&(block - 1).to_be_bytes());
    let mut tx: Hash32 = [0; 32];
    tx[0] = tx_byte;
    tx[24..].copy_from_slice(&block.to_be_bytes());
    RowBatch {
        block_number: block,
        block_hash: h,
        parent_hash: p,
        block_time: BLOCK_TIME,
        swaps: vec![SwapRow {
            tx_hash: tx,
            log_index: 0,
            pool: POOL,
            sender,
            recipient,
            amount0: bd(amount0),
            amount1: bd(amount1),
            sqrt_price_x96: bd("1"),
            liquidity: bd("1"),
            tick: 0,
        }],
        liq_events: vec![],
    }
}

const BUY: (&str, &str) = ("-1000000000000000000", "100000000"); // buy 1 TOKEN for 100 USDC
const SELL: (&str, &str) = ("1000000000000000000", "-100000000"); // sell 1 TOKEN for 100 USDC

async fn excluded(pool: &PgPool, w: Address20) -> bool {
    sqlx::query_scalar("SELECT excluded FROM wallet_stats WHERE wallet = $1")
        .bind(w.as_slice())
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn flags_self_trades_and_churn_not_a_normal_trader() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let num = numeraire();

    let ext = [0xff; 20];
    let mut block = 100;

    // CLEAN: one buy, one sell (2 trades, not self, not churn).
    db::write_row_batches_with_pnl(&pool, &[swap(block, 0xC0, ext, CLEAN, BUY.0, BUY.1)], false, &num)
        .await
        .unwrap();
    block += 1;
    db::write_row_batches_with_pnl(
        &pool,
        &[swap(block, 0xC1, ext, CLEAN, "1000000000000000000", "-150000000")],
        false,
        &num,
    )
    .await
    .unwrap();

    // SELFY: three self-trades (sender == recipient).
    for i in 0..3u8 {
        block += 1;
        let (a0, a1) = if i % 2 == 0 { BUY } else { SELL };
        db::write_row_batches_with_pnl(&pool, &[swap(block, 0xD0 + i, SELFY, SELFY, a0, a1)], false, &num)
            .await
            .unwrap();
    }

    // CHURN: six trades netting ~0 (3 buys, 3 sells of 1 TOKEN), sender != recipient.
    for i in 0..6u8 {
        block += 1;
        let (a0, a1) = if i < 3 { BUY } else { SELL };
        db::write_row_batches_with_pnl(&pool, &[swap(block, 0xE0 + i, ext, CHURN, a0, a1)], false, &num)
            .await
            .unwrap();
    }

    let flagged = db::flag_wash_trading(&pool, &WashParams::default()).await.unwrap();
    assert_eq!(flagged, 2, "exactly the self-trader and the churner");
    assert!(!excluded(&pool, CLEAN).await, "normal trader not flagged");
    assert!(excluded(&pool, SELFY).await, "self-trader flagged");
    assert!(excluded(&pool, CHURN).await, "churner flagged");

    // Idempotent: re-running the pure recompute changes nothing.
    let again = db::flag_wash_trading(&pool, &WashParams::default()).await.unwrap();
    assert_eq!(again, 2);
    assert!(!excluded(&pool, CLEAN).await);

    drop_db(&admin, pool, &name).await;
}
