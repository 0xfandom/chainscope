//! New-pool scorecard detector (#103), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-alerter --test newpool_db -- --ignored --nocapture

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainscope_alerter::config::Config;
use chainscope_alerter::detect::new_pools;
use chainscope_alerter::{Alerter, Notifier};
use chainscope_core::types::Address20;
use chainscope_indexer::pnl::Numeraire;
use sqlx::postgres::{PgPool, PgPoolOptions};

const P1: Address20 = [0xa1; 20];
const P2: Address20 = [0xa2; 20];
const INDEXED: Address20 = [0xb0; 20];

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
        numeraire: Numeraire::disabled(),
    }
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_newpool_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    chainscope_indexer::db::migrate(&pool).await.unwrap();
    // Two discovered pools + one already-indexed pool (must not alert).
    for (a, indexed) in [(P1, false), (P2, false), (INDEXED, true)] {
        sqlx::query(
            "INSERT INTO pools (address, token0, token1, fee, tick_spacing, is_indexed, risk_flags) \
             VALUES ($1, $2, $3, 3000, 60, $4, '{\"flags\":[\"fresh\"]}'::jsonb)",
        )
        .bind(a.as_slice())
        .bind([0x70u8; 20].as_slice())
        .bind([0xdcu8; 20].as_slice())
        .bind(indexed)
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

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn scorecards_fire_once_per_discovered_pool() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let sends = Arc::new(AtomicUsize::new(0));
    let alerter = Alerter {
        pool: pool.clone(),
        notifier: Arc::new(Counting(sends.clone())),
        config: config(),
    };

    // Two discovered pools alert; the indexed one does not.
    assert_eq!(new_pools(&alerter).await.unwrap(), 2);
    assert_eq!(sends.load(Ordering::SeqCst), 2);
    // A re-scan alerts nothing new.
    assert_eq!(new_pools(&alerter).await.unwrap(), 0);
    assert_eq!(sends.load(Ordering::SeqCst), 2);

    drop_db(&admin, pool, &name).await;
}
