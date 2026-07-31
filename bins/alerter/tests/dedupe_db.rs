//! Alert dedupe (#99), against a real Postgres.
//!
//!   DATABASE_URL=postgres://chainscope:chainscope@localhost:5432/chainscope \
//!     cargo test -p chainscope-alerter --test dedupe_db -- --ignored --nocapture

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainscope_alerter::config::Config;
use chainscope_alerter::{Alerter, Notifier};
use sqlx::postgres::{PgPool, PgPoolOptions};

/// A notifier that counts sends instead of hitting the network.
struct Counting(Arc<AtomicUsize>);

#[async_trait]
impl Notifier for Counting {
    async fn send(&self, _text: &str) -> anyhow::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn config(database_url: String) -> Config {
    Config {
        database_url,
        telegram_bot_token: "test".into(),
        telegram_chat_id: "test".into(),
        poll_interval: Duration::from_secs(15),
        move_threshold_usd: 25_000.0,
        move_lookback_blocks: 300,
        cluster_size: 3,
        cluster_window_secs: 7_200,
        watchlist_size: 100,
        numeraire: chainscope_indexer::pnl::Numeraire::disabled(),
    }
}

async fn admin() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new().max_connections(2).connect(&url).await.ok()
}

async fn fresh_db(admin: &PgPool) -> (PgPool, String) {
    let name = format!("chainscope_alerter99_{}", std::process::id());
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(admin).await.unwrap();
    let base = std::env::var("DATABASE_URL").unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&format!("/{name}"));
    let pool = PgPoolOptions::new().max_connections(4).connect(url.as_str()).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    (pool, name)
}

async fn drop_db(admin: &PgPool, pool: PgPool, name: &str) {
    pool.close().await;
    sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)).execute(admin).await.ok();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL"]
async fn an_alert_is_delivered_once() {
    let Some(admin) = admin().await else { return };
    let (pool, name) = fresh_db(&admin).await;
    let sends = Arc::new(AtomicUsize::new(0));
    let alerter = Alerter {
        pool: pool.clone(),
        notifier: Arc::new(Counting(sends.clone())),
        config: config(String::new()),
    };

    let key = "move:0xaa:0";
    assert!(alerter.dispatch(key, "first").await.unwrap(), "first claim sends");
    assert!(!alerter.dispatch(key, "again").await.unwrap(), "second claim is a no-op");
    // A third attempt (a later poll re-scanning the same window) also sends nothing.
    assert!(!alerter.dispatch(key, "again").await.unwrap());

    assert_eq!(sends.load(Ordering::SeqCst), 1, "delivered exactly once");

    // A different event delivers on its own.
    assert!(alerter.dispatch("move:0xbb:0", "other").await.unwrap());
    assert_eq!(sends.load(Ordering::SeqCst), 2);

    drop_db(&admin, pool, &name).await;
}
