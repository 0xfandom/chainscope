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
use chainscope_core::{LiqRow, RowBatch, SwapRow};

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

/// Write a batch of decoded blocks and advance the cursor, atomically.
///
/// This is the transaction where the project's exactly-once guarantee is
/// manufactured. Each block's header, its swap rows, its liquidity-event rows,
/// and the cursor advance move together inside a single `BEGIN`/`COMMIT`, so a
/// crash can only ever leave the database in one of two states: the whole batch
/// is present and the cursor names its last block, or none of it is and the
/// cursor is unchanged. There is no third state — no swap without its block, no
/// cursor ahead of the rows it claims.
///
/// Idempotency comes from `ON CONFLICT DO NOTHING` on every table's natural key
/// plus a cursor that only moves forward (`GREATEST`). Replaying a block inserts
/// nothing and cannot drag the cursor back, which is what lets crash recovery be
/// "rewind the cursor and rerun" without special cases.
///
/// A `RowBatch` with no swaps and no liq_events is still written: its block
/// header lands and the cursor advances, so a quiet block is not re-scanned
/// forever.
///
/// `fail_before_commit` exists only for tests: it drops the transaction after
/// all the work but before `COMMIT`, so a test can assert rows and cursor roll
/// back together. In normal use it is always false.
///
/// M6 extends this same transaction to update wallet PnL, for the same reason
/// the rows are here: it is the only place derived state can be made idempotent
/// alongside the cursor. Anything derived must come from the rows that *actually
/// inserted* (via `RETURNING`), never the incoming batch, or a replay would
/// double-count.
pub async fn write_row_batches(
    pool: &PgPool,
    batches: &[RowBatch],
    fail_before_commit: bool,
) -> anyhow::Result<u64> {
    if batches.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await.context("could not open write transaction")?;

    for b in batches {
        insert_block(&mut tx, b).await?;
        for s in &b.swaps {
            insert_swap(&mut tx, b, s).await?;
        }
        for l in &b.liq_events {
            insert_liq(&mut tx, b, l).await?;
        }
    }

    // The producer is sequential and in order, so the last block is the highest.
    // GREATEST is defensive: even a mis-ordered batch can only move the cursor
    // forward, never back.
    let high = batches.iter().map(|b| b.block_number).max().unwrap() as i64;
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
    Ok(batches.len() as u64)
}

async fn insert_block(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    b: &RowBatch,
) -> anyhow::Result<()> {
    // Per-row inserts for now. Bulk COPY is M3; at these batch sizes the
    // difference is noise, and correctness is the only thing M1/M2 are proving.
    sqlx::query(
        "INSERT INTO blocks (number, block_hash, parent_hash, block_time)
         VALUES ($1, $2, $3, to_timestamp($4))
         ON CONFLICT (number) DO NOTHING",
    )
    .bind(b.block_number as i64)
    .bind(b.block_hash.as_slice())
    .bind(b.parent_hash.as_slice())
    .bind(b.block_time)
    .execute(&mut **tx)
    .await
    .with_context(|| format!("could not insert block {}", b.block_number))?;
    Ok(())
}

async fn insert_swap(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    b: &RowBatch,
    s: &SwapRow,
) -> anyhow::Result<()> {
    // block_time and block_number come from the block, not the row: block_time
    // is the partition key, and it must match the value the block header used so
    // the conflict on `(block_time, tx_hash, log_index)` fires on replay.
    sqlx::query(
        "INSERT INTO swaps
            (block_time, tx_hash, log_index, block_number, pool, sender, recipient,
             amount0, amount1, sqrt_price_x96, liquidity, tick)
         VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (block_time, tx_hash, log_index) DO NOTHING",
    )
    .bind(b.block_time)
    .bind(s.tx_hash.as_slice())
    .bind(s.log_index as i32)
    .bind(b.block_number as i64)
    .bind(s.pool.as_slice())
    .bind(s.sender.as_slice())
    .bind(s.recipient.as_slice())
    .bind(s.amount0.clone())
    .bind(s.amount1.clone())
    .bind(s.sqrt_price_x96.clone())
    .bind(s.liquidity.clone())
    .bind(s.tick)
    .execute(&mut **tx)
    .await
    .with_context(|| format!("could not insert swap at block {}", b.block_number))?;
    Ok(())
}

async fn insert_liq(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    b: &RowBatch,
    l: &LiqRow,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO liq_events
            (block_time, tx_hash, log_index, block_number, pool, kind, owner,
             tick_lower, tick_upper, amount, amount0, amount1)
         VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (block_time, tx_hash, log_index) DO NOTHING",
    )
    .bind(b.block_time)
    .bind(l.tx_hash.as_slice())
    .bind(l.log_index as i32)
    .bind(b.block_number as i64)
    .bind(l.pool.as_slice())
    .bind(l.kind.as_str())
    .bind(l.owner.as_slice())
    .bind(l.tick_lower)
    .bind(l.tick_upper)
    .bind(l.amount.clone())
    .bind(l.amount0.clone())
    .bind(l.amount1.clone())
    .execute(&mut **tx)
    .await
    .with_context(|| format!("could not insert liq_event at block {}", b.block_number))?;
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
