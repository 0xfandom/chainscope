//! chainscope alerter binary.

use std::sync::Arc;

use chainscope_alerter::config::Config;
use chainscope_alerter::notify::Telegram;
use chainscope_alerter::{db, Alerter};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = Config::from_env()?;
    tracing::info!(poll_secs = cfg.poll_interval.as_secs(), "configuration loaded");

    let pool = db::connect(&cfg.database_url).await?;
    let notifier = Arc::new(Telegram::new(
        cfg.telegram_bot_token.clone(),
        cfg.telegram_chat_id.clone(),
    ));
    let alerter = Alerter {
        pool,
        notifier,
        config: cfg,
    };

    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal_cancel.cancel();
    });

    alerter.run(cancel).await
}

/// Resolve on SIGINT or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received");
}
