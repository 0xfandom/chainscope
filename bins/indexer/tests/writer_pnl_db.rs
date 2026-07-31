//! FIFO cost-basis PnL folded into the writer transaction (#72), real Postgres.
//!
//! Drives the exact live write path (`write_row_batches_with_pnl`) over a
//! hand-worked buy then sell, checks the lots, the realised PnL, the wallet
//! rollup and the consumption ledger against arithmetic done by hand, then
//! replays the same batches and asserts nothing moved — exactly-once for the
//! derived state, inherited from the swap insert's RETURNING gate.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test writer_pnl_db -- --ignored --nocapture

use std::str::FromStr;

use bigdecimal::BigDecimal;
use chainscope_core::{
    types::{Address20, Hash32, SwapRow},
    RowBatch,
};
use chainscope_indexer::{db, pnl::Numeraire};
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];
const TOKEN: Address20 = [0x70; 20]; // token0, 18 decimals — the asset
const USDC: Address20 = [0xdc; 20]; // token1, 6 decimals — the numeraire
const WALLET: Address20 = [0x11; 20];
const BLOCK_TIME: i64 = 1_784_894_400; // 2026-07-24T12:00:00Z

fn bd(s: &str) -> BigDecimal {
    BigDecimal::from_str(s).unwrap()
}

/// bigdecimal equality is scale-sensitive; compare the values, not the scales.
fn same(a: &BigDecimal, b: &BigDecimal) {
    assert_eq!(a.normalized(), b.normalized(), "{a} != {b}");
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

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_pnl_{}_{}", std::process::id(), tag);
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
    // The asset pool: TOKEN (18) / USDC (6).
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

/// A block carrying one swap for our pool and wallet, amounts pool-perspective.
fn swap_block(block: u64, tx_byte: u8, amount0: &str, amount1: &str) -> RowBatch {
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
            sender: [0xff; 20],
            recipient: WALLET,
            amount0: bd(amount0),
            amount1: bd(amount1),
            sqrt_price_x96: bd("1"),
            liquidity: bd("1"),
            tick: 0,
        }],
        liq_events: vec![],
    }
}

/// Buy 2 TOKEN for 200 USDC: token0 leaves the pool (-2e18), token1 enters (+200e6).
fn buy_block() -> RowBatch {
    swap_block(100, 0xB1, "-2000000000000000000", "200000000")
}

/// Sell 2 TOKEN for 300 USDC: token0 enters the pool (+2e18), token1 leaves (-300e6).
fn sell_block() -> RowBatch {
    swap_block(101, 0x5E, "2000000000000000000", "-300000000")
}

/// NUMERIC stat columns.
async fn stat(pool: &PgPool, col: &str) -> BigDecimal {
    sqlx::query_scalar(&format!("SELECT {col} FROM wallet_stats WHERE wallet = $1"))
        .bind(WALLET.as_slice())
        .fetch_one(pool)
        .await
        .unwrap()
}

/// INTEGER stat columns (trades, wins).
async fn stat_int(pool: &PgPool, col: &str) -> i32 {
    sqlx::query_scalar(&format!("SELECT {col} FROM wallet_stats WHERE wallet = $1"))
        .bind(WALLET.as_slice())
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn position_qty(pool: &PgPool) -> BigDecimal {
    sqlx::query_scalar("SELECT qty_held FROM wallet_positions WHERE wallet=$1 AND token=$2")
        .bind(WALLET.as_slice())
        .bind(TOKEN.as_slice())
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn consumptions(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM lot_consumptions")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn buy_opens_a_lot_sell_realises_against_it() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin, "buysell").await;
    let num = numeraire();

    // --- buy: opens one lot, realises nothing ---
    db::write_row_batches_with_pnl(&pool, &[buy_block()], false, &num).await.unwrap();
    same(&position_qty(&pool).await, &bd("2"));
    let cost: BigDecimal =
        sqlx::query_scalar("SELECT cost_basis_usd FROM wallet_positions WHERE wallet=$1 AND token=$2")
            .bind(WALLET.as_slice())
            .bind(TOKEN.as_slice())
            .fetch_one(&pool)
            .await
            .unwrap();
    same(&cost, &bd("200")); // 2 @ $100
    same(&stat(&pool, "realized_pnl_usd").await, &bd("0"));
    assert_eq!(stat_int(&pool, "trades").await, 1);
    same(&stat(&pool, "volume_usd").await, &bd("200"));
    assert_eq!(consumptions(&pool).await, 0);

    // --- sell: consumes the lot, realises 300 - 200 = 100 ---
    db::write_row_batches_with_pnl(&pool, &[sell_block()], false, &num).await.unwrap();
    same(&position_qty(&pool).await, &bd("0")); // lot fully consumed
    same(&stat(&pool, "realized_pnl_usd").await, &bd("100"));
    assert_eq!(stat_int(&pool, "trades").await, 2);
    assert_eq!(stat_int(&pool, "wins").await, 1);
    same(&stat(&pool, "volume_usd").await, &bd("500"));
    same(&stat(&pool, "avg_size_usd").await, &bd("250"));
    assert_eq!(consumptions(&pool).await, 1);

    let (qty, cost_c, proceeds, realized): (BigDecimal, BigDecimal, BigDecimal, BigDecimal) =
        sqlx::query_as(
            "SELECT qty_consumed, lot_unit_cost_usd, proceeds_usd, realized_pnl_usd \
             FROM lot_consumptions",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
    same(&qty, &bd("2"));
    same(&cost_c, &bd("100"));
    same(&proceeds, &bd("300"));
    same(&realized, &bd("100"));

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn replay_folds_nothing() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin, "replay").await;
    let num = numeraire();

    let batches = [buy_block(), sell_block()];
    db::write_row_batches_with_pnl(&pool, &batches, false, &num).await.unwrap();

    let realized_1 = stat(&pool, "realized_pnl_usd").await;
    let trades_1 = stat_int(&pool, "trades").await;
    same(&realized_1, &bd("100"));
    assert_eq!(trades_1, 2);
    assert_eq!(consumptions(&pool).await, 1);

    // Replay the exact same batches: no swap inserts, so no PnL folds.
    db::write_row_batches_with_pnl(&pool, &batches, false, &num).await.unwrap();
    same(&stat(&pool, "realized_pnl_usd").await, &realized_1);
    assert_eq!(stat_int(&pool, "trades").await, trades_1);
    assert_eq!(consumptions(&pool).await, 1, "replay adds no ledger rows");

    drop_db(&admin, pool, &name).await;
}
