//! Leaderboard and scorecard (#75), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test leaderboard_db -- --ignored --nocapture

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
const W_A: Address20 = [0x0a; 20]; // realises 100
const W_OPEN: Address20 = [0x0b; 20]; // realises 50, still holds 1
const W_WASH: Address20 = [0x0c; 20]; // realises 200 but self-trades → excluded
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
    let name = format!("chainscope_lb_{}", std::process::id());
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

fn swap(block: u64, tx_byte: u8, w: Address20, self_trade: bool, a0: &str, a1: &str) -> RowBatch {
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
            sender: if self_trade { w } else { [0xff; 20] },
            recipient: w,
            amount0: bd(a0),
            amount1: bd(a1),
            sqrt_price_x96: bd("1"),
            liquidity: bd("1"),
            tick: 0,
        }],
        liq_events: vec![],
    }
}

async fn fold(pool: &PgPool, num: &Numeraire, b: RowBatch) {
    db::write_row_batches_with_pnl(pool, &[b], false, num).await.unwrap();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn leaderboard_ranks_and_excludes_wash() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let num = numeraire();

    // W_A: buy 1 @ $100, sell 1 @ $200 -> realised 100.
    fold(&pool, &num, swap(100, 0x01, W_A, false, "-1000000000000000000", "100000000")).await;
    fold(&pool, &num, swap(101, 0x02, W_A, false, "1000000000000000000", "-200000000")).await;

    // W_OPEN: buy 2 @ $100, sell 1 @ $150 -> realised 50, still holds 1.
    fold(&pool, &num, swap(102, 0x03, W_OPEN, false, "-2000000000000000000", "200000000")).await;
    fold(&pool, &num, swap(103, 0x04, W_OPEN, false, "1000000000000000000", "-150000000")).await;

    // W_WASH: three self-trades, one realising 200 -> highest PnL, but wash.
    fold(&pool, &num, swap(104, 0x05, W_WASH, true, "-1000000000000000000", "100000000")).await;
    fold(&pool, &num, swap(105, 0x06, W_WASH, true, "1000000000000000000", "-300000000")).await;
    fold(&pool, &num, swap(106, 0x07, W_WASH, true, "-1000000000000000000", "100000000")).await;

    db::flag_wash_trading(&pool, &WashParams::default()).await.unwrap();
    db::refresh_leaderboard(&pool).await.unwrap();

    let board = db::leaderboard(&pool, 100).await.unwrap();
    let wallets: Vec<Address20> = board.iter().map(|r| r.wallet).collect();
    assert_eq!(wallets, vec![W_A, W_OPEN], "ranked by realised PnL, wash wallet absent");
    assert_eq!(board[0].realized_pnl_usd.normalized(), bd("100"));
    assert_eq!(board[1].realized_pnl_usd.normalized(), bd("50"));
    assert!(!wallets.contains(&W_WASH), "highest PnL but excluded as wash");

    // Scorecard for the wallet still holding a position.
    let sc = db::scorecard(&pool, &W_OPEN, 10).await.unwrap().unwrap();
    assert_eq!(sc.realized_pnl_usd.normalized(), bd("50"));
    assert_eq!(sc.trades, 2);
    assert_eq!(sc.wins, 1);
    assert!(!sc.excluded);
    assert_eq!(sc.open_positions.len(), 1);
    assert_eq!(sc.open_positions[0].token, TOKEN);
    assert_eq!(sc.open_positions[0].qty_held.normalized(), bd("1"));
    assert_eq!(sc.open_positions[0].cost_basis_usd.normalized(), bd("100"));
    assert_eq!(sc.recent_realized.len(), 1);
    assert_eq!(sc.recent_realized[0].realized_pnl_usd.normalized(), bd("50"));
    assert_eq!(sc.recent_realized[0].proceeds_usd.normalized(), bd("150"));

    // An unknown wallet has no scorecard.
    assert!(db::scorecard(&pool, &[0x99; 20], 10).await.unwrap().is_none());

    drop_db(&admin, pool, &name).await;
}
