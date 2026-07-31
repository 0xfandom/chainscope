//! The poll loop and the dedupe-guarded dispatch every detector shares.

use std::sync::Arc;

use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::notify::Notifier;

pub struct Alerter {
    pub pool: PgPool,
    pub notifier: Arc<dyn Notifier>,
    pub config: Config,
}

impl Alerter {
    /// Deliver an alert once. Claims the key in `alerts_sent`; sends only if the
    /// claim was new. Returns whether it sent. A re-scan, replay or reorg
    /// re-index of the same event finds the row already claimed and sends
    /// nothing.
    pub async fn dispatch(&self, key: &str, text: &str) -> anyhow::Result<bool> {
        if crate::db::claim(&self.pool, key).await? {
            self.notifier.send(text).await?;
            tracing::info!(key, "alert sent");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// One detection pass. The detectors plug in here in #101/#102/#103; for now
    /// it is a no-op so the loop and delivery spine can land on their own.
    async fn tick(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Poll until cancelled.
    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        tracing::info!(
            poll_ms = self.config.poll_interval.as_millis() as u64,
            "alerter started"
        );
        let mut interval = tokio::time::interval(self.config.poll_interval);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(e) = self.tick().await {
                        // A failed pass must not kill the loop — log and try the
                        // next tick. Nothing was claimed for a send that failed.
                        tracing::error!(error = %e, "alert pass failed");
                    }
                }
            }
        }
        tracing::info!("alerter stopped");
        Ok(())
    }
}
