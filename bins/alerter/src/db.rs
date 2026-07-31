//! The alerter's data access — read-only over the shared schema, except the one
//! write it owns: claiming a row in the `alerts_sent` idempotency ledger.

use sqlx::postgres::{PgPool, PgPoolOptions};

pub async fn connect(url: &str) -> anyhow::Result<PgPool> {
    Ok(PgPoolOptions::new().max_connections(4).connect(url).await?)
}

/// Claim an alert key. Returns `true` only if this call inserted the row — i.e.
/// this is the first time the alert has been seen. A replay or a re-scan finds
/// the row already there and returns `false`, so the caller sends nothing.
///
/// Claim-then-send (0009): the ledger row goes in first, and the message is sent
/// only on a `true`. The rare cost is a crash between claim and send losing one
/// alert; the alternative (send then claim) double-sends on every replay, which
/// for a phone notification is the worse failure.
pub async fn claim(pool: &PgPool, key: &str) -> anyhow::Result<bool> {
    let inserted = sqlx::query(
        "INSERT INTO alerts_sent (alert_key) VALUES ($1)
         ON CONFLICT (alert_key) DO NOTHING
         RETURNING 1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(inserted.is_some())
}
