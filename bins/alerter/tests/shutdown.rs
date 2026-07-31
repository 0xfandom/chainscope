//! Graceful shutdown (#124): the alerter's poll loop returns promptly on cancel.
//!
//! Offline — a lazily-connected pool never reaches a database (each tick's query
//! fails fast and is logged), so this exercises only the loop's response to the
//! cancellation token.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainscope_alerter::config::Config;
use chainscope_alerter::{Alerter, Notifier};
use chainscope_indexer::pnl::Numeraire;
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

struct Silent;
#[async_trait]
impl Notifier for Silent {
    async fn send(&self, _t: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

fn config() -> Config {
    Config {
        database_url: String::new(),
        telegram_bot_token: "t".into(),
        telegram_chat_id: "t".into(),
        poll_interval: Duration::from_millis(50),
        move_threshold_usd: 100.0,
        move_lookback_blocks: 300,
        cluster_size: 3,
        cluster_window_secs: 7_200,
        watchlist_size: 100,
        numeraire: Numeraire::disabled(),
    }
}

#[tokio::test]
async fn the_loop_returns_when_cancelled() {
    let pool = PgPool::connect_lazy("postgres://unused/unused").unwrap();
    let alerter = Alerter {
        pool,
        notifier: Arc::new(Silent),
        config: config(),
    };
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(alerter.run(cancel.clone()));

    // Let it spin a few ticks, then ask it to stop.
    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel.cancel();

    // It must return well within a bounded time, not hang.
    let stopped = tokio::time::timeout(Duration::from_secs(2), handle).await;
    assert!(stopped.is_ok(), "alerter did not stop within the bound");
    assert!(stopped.unwrap().unwrap().is_ok(), "alerter returned an error");
}
