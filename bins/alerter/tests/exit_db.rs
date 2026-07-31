//! M8 exit criterion (#104): no double-sends across replay and reorg.
//!
//! Drives all three detectors over a fixture with a counting stub notifier,
//! asserts one delivery per unique alert, then replays the same data and
//! re-indexes it (as a reorg would) and asserts the delivery count does not
//! move — the alerts_sent ledger absorbs it.
//!
//! The other half of the exit — a real message on a phone — is a manual smoke
//! test (needs a live TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID) recorded in the
//! milestone notes; it cannot live in CI without secrets.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-alerter --test exit_db -- --ignored --nocapture

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainscope_alerter::config::Config;
use chainscope_alerter::detect::{cluster_buys, new_pools, watchlist_moves};
use chainscope_alerter::{Alerter, Notifier};
use chainscope_core::types::Address20;
use chainscope_indexer::pnl::Numeraire;
use sqlx::postgres::{PgPool, PgPoolOptions};

const POOL: Address20 = [0x9a; 20];
const TOKEN: Address20 = [0x70; 20];
const USDC: Address20 = [0xdc; 20];
const W1: Address20 = [0x11; 20];
const W2: Address20 = [0x22; 20];
const W3: Address20 = [0x33; 20];
const NEW_POOL: Address20 = [0xa1; 20];
const BASE_TS: i64 = 1_784_894_400;

struct Counting(Arc<AtomicUsize>);
#[async_trait]
impl Notifier for Counting {
    async fn send(&self, _t: &str) -> anyhow::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
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
        numeraire: Numeraire {
            stables: [USDC].into_iter().collect(),
            weth: None,
            weth_price_pool: None,
        },
    }
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn buy(pool: &PgPool, block: i64, tx_byte: u8, wallet: Address20, usdc: i64, secs: i64) {
    let mut tx = [0u8; 32];
    tx[0] = tx_byte;
    tx[24..].copy_from_slice(&(block as u64).to_be_bytes());
    sqlx::query(
        "INSERT INTO swaps (block_time, tx_hash, log_index, block_number, pool, sender, \
             recipient, amount0, amount1, sqrt_price_x96, liquidity, tick) \
         VALUES (to_timestamp($1), $2, 0, $3, $4, $5, $6, -2000000000000000000, $7, 1, 1, 0)",
    )
    .bind(BASE_TS + secs)
    .bind(tx.as_slice())
    .bind(block)
    .bind(POOL.as_slice())
    .bind([0xffu8; 20].as_slice())
    .bind(wallet.as_slice())
    .bind(usdc)
    .execute(pool)
    .await
    .unwrap();
}

/// Seed the fixture: watchlist, a big move + a 3-wallet cluster, and a fresh pool.
async fn seed_swaps(pool: &PgPool) {
    // W1's $300 buy is both a move and part of the cluster; W2/W3 are cluster-only.
    buy(pool, 900, 0xB1, W1, 300_000_000, 0).await;
    buy(pool, 901, 0xB2, W2, 50_000_000, 60).await;
    buy(pool, 902, 0xB3, W3, 50_000_000, 120).await;
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_alexit_{}", std::process::id());
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
    // A freshly-discovered pool for the sniffer alert.
    sqlx::query(
        "INSERT INTO pools (address, token0, token1, fee, tick_spacing, is_indexed, risk_flags) \
         VALUES ($1,$2,$3,3000,60,false,'{\"flags\":[\"fresh\"]}'::jsonb)",
    )
    .bind(NEW_POOL.as_slice())
    .bind(TOKEN.as_slice())
    .bind(USDC.as_slice())
    .execute(&pool)
    .await
    .unwrap();
    for w in [W1, W2, W3] {
        sqlx::query(
            "INSERT INTO wallet_stats (wallet, realized_pnl_usd, trades, wins, volume_usd, excluded) \
             VALUES ($1, 1000, 1, 1, 1000, false)",
        )
        .bind(w.as_slice())
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("REFRESH MATERIALIZED VIEW leaderboard").execute(&pool).await.unwrap();
    sqlx::query("UPDATE chain_state SET live_cursor = 1000 WHERE id = 1").execute(&pool).await.unwrap();
    seed_swaps(&pool).await;
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

async fn run_all(a: &Alerter) -> usize {
    watchlist_moves(a).await.unwrap() + cluster_buys(a).await.unwrap() + new_pools(a).await.unwrap()
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn no_double_sends_across_replay_and_reorg() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let sends = Arc::new(AtomicUsize::new(0));
    let alerter = Alerter {
        pool: pool.clone(),
        notifier: Arc::new(Counting(sends.clone())),
        config: config(),
    };

    // First pass: one move (W1's $300 buy), one cluster (W1/W2/W3), one new pool.
    assert_eq!(run_all(&alerter).await, 3, "three distinct alerts");
    assert_eq!(sends.load(Ordering::SeqCst), 3);

    // Replay: the same poll again claims nothing new.
    assert_eq!(run_all(&alerter).await, 0);
    assert_eq!(sends.load(Ordering::SeqCst), 3);

    // Reorg re-index: the orphaned swaps are deleted and the canonical ones
    // re-written with the same keys. The delivery ledger persists, so nothing
    // resends.
    sqlx::query("DELETE FROM swaps WHERE block_number > 800").execute(&pool).await.unwrap();
    seed_swaps(&pool).await;
    assert_eq!(run_all(&alerter).await, 0, "reorg re-index sends nothing new");
    assert_eq!(sends.load(Ordering::SeqCst), 3, "delivered exactly three, once each");

    drop_db(&admin, pool, &name).await;
}
