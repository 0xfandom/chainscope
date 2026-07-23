//! Database connection and schema migration.
//!
//! Migrations are embedded into the binary at compile time by `migrate!`, so a
//! deployed indexer carries its own schema and there is no separate migration
//! step to forget. On startup it compares the embedded set against the
//! `_sqlx_migrations` table and applies only what is missing — running twice
//! against the same database is a no-op.

use anyhow::Context;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Migrations live at the workspace root, not inside this crate, because the
/// API binary and any operator running `sqlx migrate` by hand need the same set.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Connect to Postgres using `DATABASE_URL`.
///
/// Reading the environment directly is deliberate scaffolding: issue #3 replaces
/// this with real configuration loading and startup validation.
pub async fn connect() -> anyhow::Result<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL is not set — copy .env.example to .env")?;

    PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .context("could not connect to Postgres — is `docker compose up -d` running?")
}

/// Apply any migrations the database has not seen yet.
pub async fn migrate(pool: &PgPool) -> anyhow::Result<()> {
    MIGRATOR
        .run(pool)
        .await
        .context("migration failed; the database is unchanged")?;
    Ok(())
}

/// Create the day partitions the raw event tables will need shortly.
///
/// Called on every startup rather than only at migration time: a process that
/// has been running for a week has long since passed the partitions its initial
/// migration created, and an insert into a day with no partition is an error by
/// design (see migrations/0004_swaps.sql).
pub async fn ensure_partitions(pool: &PgPool) -> anyhow::Result<i32> {
    let created: i32 = sqlx::query_scalar("SELECT ensure_day_partitions()")
        .fetch_one(pool)
        .await
        .context("could not create day partitions")?;
    Ok(created)
}
