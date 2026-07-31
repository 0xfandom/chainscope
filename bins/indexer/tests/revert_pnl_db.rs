//! Reorg PnL reversal (#73), against a real Postgres.
//!
//! A reorg must leave PnL exactly as a clean ingest of the canonical chain would:
//! the orphaned swaps' effect on lots, realised PnL and the ledger is undone from
//! the consumption ledger, not recomputed. These drive the real `rewind_to` and
//! compare, table for table, against a fresh clean ingest as the oracle.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test revert_pnl_db -- --ignored --nocapture

use std::str::FromStr;

use bigdecimal::BigDecimal;
use chainscope_core::{
    types::{Address20, Hash32, SwapRow},
    RowBatch,
};
use chainscope_indexer::{db, pnl::Numeraire};
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];
const TOKEN: Address20 = [0x70; 20]; // asset, 18 decimals
const USDC: Address20 = [0xdc; 20]; // numeraire, 6 decimals
const WALLET: Address20 = [0x11; 20];
const BLOCK_TIME: i64 = 1_784_894_400; // 2026-07-24T12:00:00Z
const FORK: u64 = 100;

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

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_rpnl_{}_{}", std::process::id(), tag);
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

/// Block 100 (survives the fork): buy 2 TOKEN for 200 USDC.
fn buy() -> RowBatch {
    swap_block(100, 0xB1, "-2000000000000000000", "200000000")
}
/// Block 101 orphan: sell 2 TOKEN for 400 USDC — the branch that gets reverted.
fn orphan_sell() -> RowBatch {
    swap_block(101, 0x0A, "2000000000000000000", "-400000000")
}
/// Block 101 canonical: sell 1 TOKEN for 150 USDC.
fn canonical_sell() -> RowBatch {
    swap_block(101, 0x5E, "1000000000000000000", "-150000000")
}

/// A comparable snapshot of the wallet's PnL state.
#[derive(Debug, PartialEq)]
struct Snap {
    realized: String,
    qty_held: String,
    trades: i32,
    wins: i32,
    consumptions: i64,
    consumed_realized: String,
}

async fn snapshot(pool: &PgPool) -> Snap {
    let realized: Option<BigDecimal> =
        sqlx::query_scalar("SELECT realized_pnl_usd FROM wallet_stats WHERE wallet=$1")
            .bind(WALLET.as_slice())
            .fetch_optional(pool)
            .await
            .unwrap();
    let (trades, wins): (i32, i32) = sqlx::query_as(
        "SELECT COALESCE(trades,0), COALESCE(wins,0) FROM wallet_stats WHERE wallet=$1",
    )
    .bind(WALLET.as_slice())
    .fetch_optional(pool)
    .await
    .unwrap()
    .unwrap_or((0, 0));
    let qty: Option<BigDecimal> =
        sqlx::query_scalar("SELECT qty_held FROM wallet_positions WHERE wallet=$1 AND token=$2")
            .bind(WALLET.as_slice())
            .bind(TOKEN.as_slice())
            .fetch_optional(pool)
            .await
            .unwrap();
    let cons: i64 = sqlx::query_scalar("SELECT count(*) FROM lot_consumptions")
        .fetch_one(pool)
        .await
        .unwrap();
    let cons_realized: Option<BigDecimal> =
        sqlx::query_scalar("SELECT COALESCE(sum(realized_pnl_usd),0) FROM lot_consumptions")
            .fetch_one(pool)
            .await
            .unwrap();
    let norm = |v: Option<BigDecimal>| v.unwrap_or_else(|| bd("0")).normalized().to_string();
    Snap {
        realized: norm(realized),
        qty_held: norm(qty),
        trades,
        wins,
        consumptions: cons,
        consumed_realized: norm(cons_realized),
    }
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn revert_then_reindex_matches_clean_ingest() {
    let Some(admin) = admin().await else { return };
    let num = numeraire();

    // --- the reverted path: index the orphan branch, rewind, reindex canonical ---
    let (reverted, rn) = fresh_db(&admin, "reverted").await;
    db::write_row_batches_with_pnl(&reverted, &[buy()], false, &num).await.unwrap();
    db::write_row_batches_with_pnl(&reverted, &[orphan_sell()], false, &num).await.unwrap();
    // Orphan state: sold all 2 for 400, realised 200.
    assert_eq!(snapshot(&reverted).await.realized, "200");

    db::rewind_to(&reverted, FORK, false).await.unwrap();
    // Reversal alone lands back on the post-buy state: lot restored, nothing realised.
    let after_rewind = snapshot(&reverted).await;
    assert_eq!(after_rewind.realized, "0");
    assert_eq!(after_rewind.qty_held, "2");
    assert_eq!(after_rewind.trades, 1);
    assert_eq!(after_rewind.consumptions, 0);

    db::write_row_batches_with_pnl(&reverted, &[canonical_sell()], false, &num).await.unwrap();

    // --- the oracle: a clean ingest of only the canonical chain ---
    let (clean, cn) = fresh_db(&admin, "clean").await;
    db::write_row_batches_with_pnl(&clean, &[buy()], false, &num).await.unwrap();
    db::write_row_batches_with_pnl(&clean, &[canonical_sell()], false, &num).await.unwrap();

    let reverted_snap = snapshot(&reverted).await;
    let clean_snap = snapshot(&clean).await;
    assert_eq!(reverted_snap, clean_snap, "reverted+reindexed must equal clean ingest");
    // And concretely: sold 1 of 2 @ $100 for $150 -> realised 50, 1 left.
    assert_eq!(clean_snap.realized, "50");
    assert_eq!(clean_snap.qty_held, "1");
    assert_eq!(clean_snap.trades, 2);
    assert_eq!(clean_snap.wins, 1);
    assert_eq!(clean_snap.consumptions, 1);

    drop_db(&admin, reverted, &rn).await;
    drop_db(&admin, clean, &cn).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn repeated_rewind_is_a_noop() {
    let Some(admin) = admin().await else { return };
    let num = numeraire();
    let (pool, name) = fresh_db(&admin, "idem").await;

    db::write_row_batches_with_pnl(&pool, &[buy()], false, &num).await.unwrap();
    db::write_row_batches_with_pnl(&pool, &[orphan_sell()], false, &num).await.unwrap();
    db::rewind_to(&pool, FORK, false).await.unwrap();
    let once = snapshot(&pool).await;

    // A redelivered revert finds nothing above the fork.
    db::rewind_to(&pool, FORK, false).await.unwrap();
    assert_eq!(snapshot(&pool).await, once, "second rewind changes nothing");

    drop_db(&admin, pool, &name).await;
}
