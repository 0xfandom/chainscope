//! chainscope read API binary.

use std::sync::Arc;
use std::time::Duration;

use chainscope_api::{app, config::Config, db, AppState};
use tokio::sync::Notify;

/// Hard cap on how long graceful drain may take *after* the signal, so a stuck
/// connection cannot hang the process past `docker compose down`'s patience.
const DRAIN_CAP: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Configuration first, before a socket or a connection is opened.
    let cfg = Config::from_env()?;
    tracing::info!(bind = %cfg.bind, max_conns = cfg.max_connections, "configuration loaded");

    let pool = db::connect(&cfg.database_url, cfg.max_connections).await?;
    let state = AppState::new(pool, cfg.cache_ttl);

    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    tracing::info!(addr = %cfg.bind, "chainscope-api listening");

    // The signal both begins axum's graceful drain and starts the hard cap: if
    // the drain outlives DRAIN_CAP, force the shutdown rather than hang.
    let signalled = Arc::new(Notify::new());
    let s2 = signalled.clone();
    let server = axum::serve(listener, app(state)).with_graceful_shutdown(async move {
        shutdown_signal().await;
        s2.notify_one();
    });

    tokio::select! {
        r = server => r?,
        _ = async { signalled.notified().await; tokio::time::sleep(DRAIN_CAP).await; } => {
            tracing::warn!("graceful drain exceeded {DRAIN_CAP:?}; forcing shutdown");
        }
    }
    Ok(())
}

/// Resolve on SIGINT or SIGTERM so the server drains in-flight requests.
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
