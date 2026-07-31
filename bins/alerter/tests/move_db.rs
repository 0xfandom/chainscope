//! Watchlist-move detector (#101), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-alerter --test move_db -- --ignored --nocapture

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainscope_alerter::config::Config;
use chainscope_alerter::detect::watchlist_moves;
use chainscope_alerter::{Alerter, Notifier};
use chainscope_core::types::Address20;
use chainscope_indexer::pnl::Numeraire;
use sqlx::postgres::{PgPool, PgPoolOptions};

const POOL: Address20 = [0x9a; 20];
const TOKEN: Address20 = [0x70; 20];
const USDC: Address20 = [0xdc; 20];
const WATCHED: Address20 = [0x11; 20];
const STRANGER: Address20 = [0x22; 20];

struct Counting(Arc<AtomicUsize>);
#[async_trait]
impl Notifier for Counting {
    async fn send(&self, _t: &str) -> anyhow::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn numeraire() -> Numeraire {
    Numeraire {
        stables: [USDC].into_iter().collect(),
        weth: None,
        weth_price_pool: None,
    }
}

fn config() -> Config {
    Config {
        database_url: String::new(),
        telegram_bot_token: "t".into(),
        telegram_chat_id: "t".into(),
        poll_interval: Duration::from_secs(15),
        move_threshold_usd: 100.0,
        move_lookback_blocks: 300,
        cluster_size: 3,
        cluster_window_secs: 7_200,
        watchlist_size: 100,
        numeraire: numeraire(),
    }
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_move_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    chainscope_indexer::db::migrate(&pool).await.unwrap();
    chainscope_indexer::db::ensure_partitions(&pool).await.unwrap();
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
    // WATCHED is on the leaderboard.
    sqlx::query(
        "INSERT INTO wallet_stats (wallet, realized_pnl_usd, trades, wins, volume_usd, excluded) \
         VALUES ($1, 1000, 1, 1, 1000, false)",
    )
    .bind(WATCHED.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("REFRESH MATERIALIZED VIEW leaderboard").execute(&pool).await.unwrap();
    sqlx::query("UPDATE chain_state SET live_cursor = 110 WHERE id = 1").execute(&pool).await.unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

/// A buy: `recipient` buys TOKEN paying `usdc` USDC (token1 into pool).
async fn buy(pool: &PgPool, block: i64, tx_byte: u8, recipient: Address20, usdc_millionths: i64) {
    let mut tx = [0u8; 32];
    tx[0] = tx_byte;
    tx[24..].copy_from_slice(&(block as u64).to_be_bytes());
    sqlx::query(
        "INSERT INTO swaps (block_time, tx_hash, log_index, block_number, pool, sender, \
             recipient, amount0, amount1, sqrt_price_x96, liquidity, tick) \
         VALUES (to_timestamp(1784894400), $1, 0, $2, $3, $4, $5, -2000000000000000000, $6, 1, 1, 0)",
    )
    .bind(tx.as_slice())
    .bind(block)
    .bind(POOL.as_slice())
    .bind([0xffu8; 20].as_slice())
    .bind(recipient.as_slice())
    .bind(usdc_millionths)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn fires_on_a_big_watchlist_buy_only() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let sends = Arc::new(AtomicUsize::new(0));
    let alerter = Alerter {
        pool: pool.clone(),
        notifier: Arc::new(Counting(sends.clone())),
        config: config(),
    };

    // WATCHED: one $300 buy (above the $100 threshold), one $50 buy (below).
    buy(&pool, 100, 0xA1, WATCHED, 300_000_000).await;
    buy(&pool, 101, 0xA2, WATCHED, 50_000_000).await;
    // A stranger's big buy must not fire — not on the watchlist.
    buy(&pool, 102, 0xA3, STRANGER, 900_000_000).await;

    let sent = watchlist_moves(&alerter).await.unwrap();
    assert_eq!(sent, 1, "only the watched, above-threshold buy");
    assert_eq!(sends.load(Ordering::SeqCst), 1);

    // A second pass rescans the same window but dedupe sends nothing new.
    let again = watchlist_moves(&alerter).await.unwrap();
    assert_eq!(again, 0);
    assert_eq!(sends.load(Ordering::SeqCst), 1);

    drop_db(&admin, pool, &name).await;
}
