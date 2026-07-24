//! Database connection and schema migration.
//!
//! Migrations are embedded into the binary at compile time by `migrate!`, so a
//! deployed indexer carries its own schema and there is no separate migration
//! step to forget. On startup it compares the embedded set against the
//! `_sqlx_migrations` table and applies only what is missing — running twice
//! against the same database is a no-op.

use anyhow::Context;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Postgres;

use crate::config::Database;
use chainscope_core::BlockUnit;

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

/// Write one batch of blocks and advance the cursor, atomically.
///
/// This is the transaction where the project's exactly-once guarantee is
/// manufactured. The block rows and the cursor move together inside a single
/// `BEGIN`/`COMMIT`, so a crash can only ever leave the database in one of two
/// states: the whole batch is present and the cursor names its last block, or
/// none of it is and the cursor is unchanged. There is no third state where the
/// cursor claims progress the rows do not back up.
///
/// Idempotency comes from two places working together. `ON CONFLICT (number) DO
/// NOTHING` means replaying a block inserts nothing, and the cursor moves with
/// `GREATEST`, so replaying an old range can never drag it backwards. Together
/// they make replaying any range a no-op, which is what lets crash recovery be
/// "rewind the cursor and rerun" without special cases.
///
/// `fail_before_commit` exists only for tests: it forces the transaction to be
/// dropped after all the work but before `COMMIT`, so a test can assert that the
/// rows and the cursor roll back together. In normal use it is always false.
///
/// M2 extends this same function to also insert decoded swaps and liq_events,
/// and M6 to update wallet PnL — all inside this transaction, because that is
/// the only place they can be made idempotent alongside the cursor. The rows
/// they derive from must be the ones that actually inserted here (via
/// `RETURNING`), never the incoming batch, or a replay would double-count.
pub async fn write_block_batch(
    pool: &PgPool,
    blocks: &[BlockUnit],
    fail_before_commit: bool,
) -> anyhow::Result<u64> {
    if blocks.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await.context("could not open write transaction")?;

    for b in blocks {
        insert_block(&mut tx, b).await?;
    }

    // The producer is sequential and in order, so the last block is the highest.
    // GREATEST is defensive: even a mis-ordered batch can only move the cursor
    // forward, never back.
    let high = blocks.iter().map(|b| b.number).max().unwrap() as i64;
    sqlx::query(
        "UPDATE chain_state
            SET live_cursor = GREATEST(COALESCE(live_cursor, -1), $1),
                head_height = GREATEST(COALESCE(head_height, -1), $1),
                updated_at  = now()
          WHERE id = 1",
    )
    .bind(high)
    .execute(&mut *tx)
    .await
    .context("could not advance the live cursor")?;

    if fail_before_commit {
        // Drop the transaction without committing. Postgres rolls it back, and
        // the assertion the test cares about — rows and cursor gone together —
        // holds without any cleanup.
        return Err(anyhow::anyhow!("injected failure before commit"));
    }

    tx.commit().await.context("could not commit the write transaction")?;
    Ok(blocks.len() as u64)
}

async fn insert_block(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    b: &BlockUnit,
) -> anyhow::Result<()> {
    // Per-row inserts for now. Bulk COPY is M3; at M1 batch sizes the difference
    // is noise, and correctness is the only thing this milestone is proving.
    sqlx::query(
        "INSERT INTO blocks (number, block_hash, parent_hash, block_time)
         VALUES ($1, $2, $3, to_timestamp($4))
         ON CONFLICT (number) DO NOTHING",
    )
    .bind(b.number as i64)
    .bind(b.hash.as_slice())
    .bind(b.parent_hash.as_slice())
    .bind(b.timestamp)
    .execute(&mut **tx)
    .await
    .with_context(|| format!("could not insert block {}", b.number))?;
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
