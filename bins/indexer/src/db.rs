//! Database connection and schema migration.
//!
//! Migrations are embedded into the binary at compile time by `migrate!`, so a
//! deployed indexer carries its own schema and there is no separate migration
//! step to forget. On startup it compares the embedded set against the
//! `_sqlx_migrations` table and applies only what is missing — running twice
//! against the same database is a no-op.

use anyhow::Context;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::config::Database;

/// Migrations live at the workspace root, not inside this crate, because the
/// API binary and any operator running `sqlx migrate` by hand need the same set.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Connect to Postgres.
///
/// Takes an already-validated `Database` config rather than reading the
/// environment, so this function cannot be the place a configuration mistake
/// surfaces — by the time it runs, the URL is known to parse.
pub async fn connect(cfg: &Database) -> anyhow::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .connect(&cfg.url)
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

/// Read the live pipeline's resume point.
///
/// `None` means nothing has been processed yet — distinct from `Some(0)`, which
/// would claim the genesis block was already handled. Advancing this value is
/// the writer's job (#7) and happens in the same transaction as the rows, which
/// is what makes a crash resume rather than lose or repeat work.
pub async fn load_live_cursor(pool: &PgPool) -> anyhow::Result<Option<u64>> {
    let cursor: Option<i64> = sqlx::query_scalar("SELECT live_cursor FROM chain_state WHERE id = 1")
        .fetch_one(pool)
        .await
        .context("could not read the live cursor")?;

    // Postgres has no unsigned integers, so the column is BIGINT. A negative
    // value would mean the row was written by something other than this
    // program, which is worth refusing rather than silently treating as huge.
    cursor
        .map(|c| u64::try_from(c).map_err(|_| anyhow::anyhow!("live_cursor is negative: {c}")))
        .transpose()
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
