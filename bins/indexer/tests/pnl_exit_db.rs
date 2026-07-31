//! M6 exit criterion (#76), against a real Postgres.
//!
//! Three properties, the bar M3 #37 and M4 #49 set for their layers:
//!   * explorer parity — a hand-worked trade sequence yields the PnL computed by
//!     hand from the swaps;
//!   * replay identity — folding the same batch twice leaves PnL byte-identical;
//!   * reorg identity — a storm of reorgs converges to a clean ingest of the
//!     canonical chain.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test pnl_exit_db -- --ignored --nocapture

use std::str::FromStr;

use bigdecimal::BigDecimal;
use chainscope_core::{
    types::{Address20, Hash32, SwapRow},
    RowBatch,
};
use chainscope_indexer::{db, pnl::Numeraire};
use sqlx::{postgres::PgPoolOptions, PgPool};

const POOL: Address20 = [0x9a; 20];
const TOKEN: Address20 = [0x70; 20];
const USDC: Address20 = [0xdc; 20];
const W: Address20 = [0x11; 20];
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

async fn fresh_db(admin: &PgPool, tag: &str) -> (PgPool, String) {
    let name = format!("chainscope_exit_{}_{}", std::process::id(), tag);
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

fn swap(block: u64, tx_byte: u8, a0: &str, a1: &str) -> RowBatch {
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
            recipient: W,
            amount0: bd(a0),
            amount1: bd(a1),
            sqrt_price_x96: bd("1"),
            liquidity: bd("1"),
            tick: 0,
        }],
        liq_events: vec![],
    }
}

/// The canonical chain: buy 3@100, buy 2@200, sell 1@150, sell 3@300, buy 1@250.
fn canonical() -> Vec<RowBatch> {
    vec![
        swap(100, 0x10, "-3000000000000000000", "300000000"), // buy 3 for 300
        swap(101, 0x11, "-2000000000000000000", "400000000"), // buy 2 for 400
        swap(102, 0x12, "1000000000000000000", "-150000000"), // sell 1 for 150
        swap(103, 0x13, "3000000000000000000", "-900000000"), // sell 3 for 900
        swap(104, 0x14, "-1000000000000000000", "250000000"), // buy 1 for 250
    ]
}

/// A doomed block at height `h`: a big sell that will be reverted.
fn orphan_at(h: u64) -> RowBatch {
    swap(h, 0xEE, "1000000000000000000", "-999000000")
}

#[derive(Debug, PartialEq)]
struct Snap {
    realized: String,
    qty_held: String,
    cost_basis: String,
    trades: i32,
    wins: i32,
    consumptions: i64,
    consumed_realized: String,
}

async fn snapshot(pool: &PgPool) -> Snap {
    let n = |v: Option<BigDecimal>| v.unwrap_or_else(|| bd("0")).normalized().to_string();
    let realized: Option<BigDecimal> =
        sqlx::query_scalar("SELECT realized_pnl_usd FROM wallet_stats WHERE wallet=$1")
            .bind(W.as_slice())
            .fetch_optional(pool)
            .await
            .unwrap();
    let (trades, wins): (i32, i32) = sqlx::query_as(
        "SELECT COALESCE(trades,0), COALESCE(wins,0) FROM wallet_stats WHERE wallet=$1",
    )
    .bind(W.as_slice())
    .fetch_optional(pool)
    .await
    .unwrap()
    .unwrap_or((0, 0));
    let qty: Option<BigDecimal> =
        sqlx::query_scalar("SELECT qty_held FROM wallet_positions WHERE wallet=$1 AND token=$2")
            .bind(W.as_slice())
            .bind(TOKEN.as_slice())
            .fetch_optional(pool)
            .await
            .unwrap();
    let cost: Option<BigDecimal> =
        sqlx::query_scalar("SELECT cost_basis_usd FROM wallet_positions WHERE wallet=$1 AND token=$2")
            .bind(W.as_slice())
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
    Snap {
        realized: n(realized),
        qty_held: n(qty),
        cost_basis: n(cost),
        trades,
        wins,
        consumptions: cons,
        consumed_realized: n(cons_realized),
    }
}

async fn clean_ingest(admin: &PgPool, tag: &str) -> (PgPool, String, Snap) {
    let (pool, name) = fresh_db(admin, tag).await;
    let num = numeraire();
    db::write_row_batches_with_pnl(&pool, &canonical(), false, &num).await.unwrap();
    let snap = snapshot(&pool).await;
    (pool, name, snap)
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn explorer_parity() {
    let Some(admin) = admin().await else { return };
    let (pool, name, snap) = clean_ingest(&admin, "parity").await;
    // Hand arithmetic from the swaps:
    //   lots after buys: [3@100, 2@200]
    //   sell 1@150  -> consume 1@100, realised 50            (lots [2@100, 2@200])
    //   sell 3@300  -> consume 2@100 (+400) + 1@200 (+100)   (lots [1@200])
    //   buy 1@250   -> lots [1@200, 1@250]
    assert_eq!(snap.realized, "550");
    assert_eq!(snap.qty_held, "2");
    assert_eq!(snap.cost_basis, "450"); // 200 + 250
    assert_eq!(snap.trades, 5);
    assert_eq!(snap.wins, 2);
    assert_eq!(snap.consumptions, 3); // one for the 1-token sell, two for the 3-token sell
    assert_eq!(snap.consumed_realized, "550");
    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn replay_is_identical() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin, "replay").await;
    let num = numeraire();
    let chain = canonical();

    db::write_row_batches_with_pnl(&pool, &chain, false, &num).await.unwrap();
    let once = snapshot(&pool).await;
    // Fold the exact same batch again — no swaps insert, so no PnL folds.
    db::write_row_batches_with_pnl(&pool, &chain, false, &num).await.unwrap();
    assert_eq!(snapshot(&pool).await, once, "replay leaves PnL identical");

    drop_db(&admin, pool, &name).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn reorg_storm_converges_to_clean_ingest() {
    let Some(admin) = admin().await else { return };
    let num = numeraire();

    // The oracle: a clean ingest of the canonical chain.
    let (clean, cn, oracle) = clean_ingest(&admin, "oracle").await;

    // The storm: at every canonical height, index a doomed orphan first, rewind
    // it away, then index the canonical block — a reorg at each step.
    let (storm, sn) = fresh_db(&admin, "storm").await;
    for c in canonical() {
        let h = c.block_number;
        db::write_row_batches_with_pnl(&storm, &[orphan_at(h)], false, &num).await.unwrap();
        db::rewind_to(&storm, h - 1, false).await.unwrap();
        db::write_row_batches_with_pnl(&storm, &[c], false, &num).await.unwrap();
    }

    let storm_snap = snapshot(&storm).await;
    assert_eq!(storm_snap, oracle, "storm converges to the clean-ingest PnL");
    assert_eq!(storm_snap.realized, "550"); // and it is the right answer

    drop_db(&admin, clean, &cn).await;
    drop_db(&admin, storm, &sn).await;
}
