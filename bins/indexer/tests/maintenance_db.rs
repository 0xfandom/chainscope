//! Watchlist maintenance (#100), against a real Postgres.
//!
//! Proves the periodic task keeps the leaderboard live: it recomputes the wash
//! flags and refreshes the matview, so a newly wash-flagged wallet drops off on
//! the next cycle.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-indexer --test maintenance_db -- --ignored --nocapture

use std::time::Duration;

use chainscope_core::types::Address20;
use chainscope_indexer::{db, maintenance::MaintenanceTask};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio_util::sync::CancellationToken;

const CLEAN: Address20 = [0x0a; 20];
const SELFY: Address20 = [0x0c; 20]; // becomes a self-trader

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_maint_{}", std::process::id());
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

async fn on_leaderboard(pool: &PgPool, w: Address20) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM leaderboard WHERE wallet = $1")
        .bind(w.as_slice())
        .fetch_one(pool)
        .await
        .unwrap();
    n == 1
}

/// Insert a wallet_stats row.
async fn stats(pool: &PgPool, w: Address20, pnl: i64) {
    sqlx::query(
        "INSERT INTO wallet_stats (wallet, realized_pnl_usd, trades, wins, volume_usd, excluded) \
         VALUES ($1, $2, 1, 1, $2, false)",
    )
    .bind(w.as_slice())
    .bind(pnl)
    .execute(pool)
    .await
    .unwrap();
}

/// Give a wallet enough self-trades that the wash filter will flag it.
async fn make_self_trader(pool: &PgPool, w: Address20) {
    for i in 0..3u8 {
        let mut tx = [0u8; 32];
        tx[0] = 0xd0 + i;
        sqlx::query(
            "INSERT INTO swaps (block_time, tx_hash, log_index, block_number, pool, sender, \
                 recipient, amount0, amount1, sqrt_price_x96, liquidity, tick) \
             VALUES (to_timestamp(1784894400), $1, 0, $2, $3, $4, $4, 1, -1, 1, 1, 0)",
        )
        .bind(tx.as_slice())
        .bind(200i64 + i as i64)
        .bind([0x9au8; 20].as_slice())
        .bind(w.as_slice())
        .execute(pool)
        .await
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn maintenance_keeps_the_leaderboard_live() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let task = MaintenanceTask::new(pool.clone(), Duration::from_secs(60), CancellationToken::new());

    // Two clean wallets. First cycle populates the leaderboard with both.
    stats(&pool, CLEAN, 100).await;
    stats(&pool, SELFY, 200).await;
    task.tick().await.unwrap();
    assert!(on_leaderboard(&pool, CLEAN).await);
    assert!(on_leaderboard(&pool, SELFY).await, "highest PnL, present while clean");

    // SELFY becomes a self-trader; the next cycle flags it and drops it.
    make_self_trader(&pool, SELFY).await;
    let excluded = task.tick().await.unwrap();
    assert_eq!(excluded, 1);
    assert!(on_leaderboard(&pool, CLEAN).await, "clean wallet stays");
    assert!(!on_leaderboard(&pool, SELFY).await, "wash wallet gone next cycle");

    drop_db(&admin, pool, &name).await;
}
