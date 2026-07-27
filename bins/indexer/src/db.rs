//! Database connection and schema migration.
//!
//! Migrations are embedded into the binary at compile time by `migrate!`, so a
//! deployed indexer carries its own schema and there is no separate migration
//! step to forget. On startup it compares the embedded set against the
//! `_sqlx_migrations` table and applies only what is missing — running twice
//! against the same database is a no-op.

use std::fmt::Write as _;

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

/// Read the backfill's resume point — the contiguous done-prefix of history.
///
/// `None` means no historical range has been completed yet, so backfill starts
/// at the configured `start_block`. Everything at or below `Some(n)` is known
/// complete; the driver resumes at `n + 1`.
pub async fn load_backfill_cursor(pool: &PgPool) -> anyhow::Result<Option<u64>> {
    let cursor: Option<i64> =
        sqlx::query_scalar("SELECT backfill_cursor FROM chain_state WHERE id = 1")
            .fetch_one(pool)
            .await
            .context("could not read the backfill cursor")?;

    cursor
        .map(|c| u64::try_from(c).map_err(|_| anyhow::anyhow!("backfill_cursor is negative: {c}")))
        .transpose()
}

/// Read the finality line — the highest block treated as irreversible.
///
/// `None` means finality has not been established yet: a fresh database, before
/// the finality tracker's first successful poll. Everything at or below
/// `Some(n)` is frozen — reorg detection (#39) never walks past it, and the
/// `blocks` header window holds nothing at or below it.
pub async fn load_finalized_height(pool: &PgPool) -> anyhow::Result<Option<u64>> {
    let h: Option<i64> =
        sqlx::query_scalar("SELECT finalized_height FROM chain_state WHERE id = 1")
            .fetch_one(pool)
            .await
            .context("could not read the finalized height")?;

    h.map(|v| u64::try_from(v).map_err(|_| anyhow::anyhow!("finalized_height is negative: {v}")))
        .transpose()
}

/// Read a recorded block hash from the reorg window.
///
/// `None` means we hold no header at that height — either it was never written
/// (a fresh window) or it has been pruned as finalised. The reorg walk (#45)
/// uses this to compare our recorded chain against the node's.
pub async fn stored_block_hash(
    pool: &PgPool,
    number: u64,
) -> anyhow::Result<Option<chainscope_core::types::Hash32>> {
    let raw: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT block_hash FROM blocks WHERE number = $1")
            .bind(number as i64)
            .fetch_optional(pool)
            .await
            .with_context(|| format!("could not read the stored hash for block {number}"))?;

    raw.map(|bytes| {
        <[u8; 32]>::try_from(bytes.as_slice())
            .map_err(|_| anyhow::anyhow!("block {number} has a {}-byte hash, not 32", bytes.len()))
    })
    .transpose()
}

/// The outcome of advancing the finality tier.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FinalityUpdate {
    /// The stored finalised height after the update (after the monotonic max).
    pub finalized_height: u64,
    /// How many now-finalised headers were pruned from the reorg window.
    pub headers_pruned: u64,
}

/// Advance the finality tier and prune the reorg window, in one transaction.
///
/// `head` and `finalized` are the chain tip and its finality line as just
/// observed. Both fold in with `GREATEST`, so finality is strictly monotonic: a
/// provider that momentarily reports a lower head or an earlier finalised block
/// can neither un-finalise a frozen block nor drag `head_height` backwards.
///
/// After the update, every `blocks` header at or below the *stored* finalised
/// height is deleted. A finalised block is irreversible, so its header can never
/// be the answer to "does the chain I recorded still match the node's?" —
/// keeping it would grow the window without it ever being read again. The prune
/// reads the post-`GREATEST` height back through `RETURNING`, so a stale, lower
/// `finalized` argument prunes nothing rather than the wrong set.
pub async fn advance_finality(
    pool: &PgPool,
    head: u64,
    finalized: u64,
) -> anyhow::Result<FinalityUpdate> {
    let mut tx = pool
        .begin()
        .await
        .context("could not open the finality transaction")?;

    let stored: i64 = sqlx::query_scalar(
        "UPDATE chain_state
            SET head_height      = GREATEST(COALESCE(head_height, -1), $1),
                finalized_height = GREATEST(COALESCE(finalized_height, -1), $2),
                updated_at       = now()
          WHERE id = 1
        RETURNING finalized_height",
    )
    .bind(head as i64)
    .bind(finalized as i64)
    .fetch_one(&mut *tx)
    .await
    .context("could not advance the finality tier")?;

    // Prune against the stored height, not the argument, so monotonicity governs
    // the prune as well: a regressive call leaves both the line and the window
    // untouched.
    let headers_pruned = sqlx::query("DELETE FROM blocks WHERE number <= $1")
        .bind(stored)
        .execute(&mut *tx)
        .await
        .context("could not prune finalized headers")?
        .rows_affected();

    tx.commit()
        .await
        .context("could not commit the finality update")?;

    Ok(FinalityUpdate {
        finalized_height: u64::try_from(stored).unwrap_or(0),
        headers_pruned,
    })
}

/// Unwind every row above `fork_point` and reset the live cursor to it,
/// atomically.
///
/// A reorg replaced the blocks above `fork_point`. This deletes the orphaned
/// headers, swaps and liquidity events, and moves `live_cursor` back to
/// `fork_point` so the producer re-indexes the canonical branch forward through
/// the ordinary write path. The delete and the cursor reset are one
/// transaction: a crash can leave only the pre-rewind state or the fully-rewound
/// state, never a hole the cursor does not record.
///
/// Re-indexing is deliberately *not* held inside this transaction. Fetching
/// canonical blocks is network round trips, and pinning a Postgres transaction
/// open across them would hold locks for the length of an RPC storm. Because the
/// cursor is reset atomically with the delete, the forward re-index is an
/// ordinary resumable write — each canonical block commits with its own
/// exactly-once guarantee.
///
/// The cursor is *set*, not `GREATEST`-ed: a rewind must move it back.
///
/// Returns the number of block headers removed. `fail_before_commit` exists only
/// for tests, to assert the delete and the reset roll back together.
pub async fn rewind_to(
    pool: &PgPool,
    fork_point: u64,
    fail_before_commit: bool,
) -> anyhow::Result<u64> {
    let f = fork_point as i64;
    let mut tx = pool.begin().await.context("could not open the rewind transaction")?;

    // swaps and liq_events are partitioned on block_time but carry block_number,
    // so a reorg — which is expressed in block numbers, not times — deletes by
    // block_number across whatever partitions the orphaned blocks fell in.
    let removed = sqlx::query("DELETE FROM blocks WHERE number > $1")
        .bind(f)
        .execute(&mut *tx)
        .await
        .context("could not delete orphaned block headers")?
        .rows_affected();

    sqlx::query("DELETE FROM swaps WHERE block_number > $1")
        .bind(f)
        .execute(&mut *tx)
        .await
        .context("could not delete orphaned swaps")?;

    sqlx::query("DELETE FROM liq_events WHERE block_number > $1")
        .bind(f)
        .execute(&mut *tx)
        .await
        .context("could not delete orphaned liquidity events")?;

    sqlx::query(
        "UPDATE chain_state SET live_cursor = $1, updated_at = now() WHERE id = 1",
    )
    .bind(f)
    .execute(&mut *tx)
    .await
    .context("could not roll the live cursor back to the fork point")?;

    if fail_before_commit {
        return Err(anyhow::anyhow!("injected failure before commit"));
    }

    tx.commit().await.context("could not commit the rewind")?;
    Ok(removed)
}

/// How a bulk backfill write split between kept and discarded raw rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BulkStats {
    /// Raw event rows COPYed into the partitions — those within the window.
    pub persisted: u64,
    /// Raw event rows dropped because their block is older than the window floor.
    /// They still feed the aggregator (#36); only the raw row is not stored.
    pub discarded: u64,
}

// Staging tables, dropped at commit. Deliberately plain column types — epoch
// seconds, hex strings, decimal strings — so the COPY payload is all digits,
// hex and decimals with no tabs, newlines or backslashes and therefore needs no
// escaping. The exact casts (`to_timestamp`, `decode(_, 'hex')`, `::numeric`)
// happen in the move below, where a bad value would fail loudly rather than
// corrupt silently.
//
// Run one statement per query rather than a single multi-statement `raw_sql`:
// `raw_sql().execute()` imposes a higher-ranked `Executor` bound that makes the
// spawned driver future fail the "Send is not general enough" check, whereas the
// plain `query().execute(&mut *tx)` path is the one the per-row writer already
// proved Send-safe.
const STAGE_STMTS: [&str; 3] = [
    "CREATE TEMP TABLE stage_blocks (number BIGINT, block_hash TEXT, parent_hash TEXT, block_time BIGINT) ON COMMIT DROP",
    "CREATE TEMP TABLE stage_swaps (block_time BIGINT, tx_hash TEXT, log_index INT, block_number BIGINT, pool TEXT, sender TEXT, recipient TEXT, amount0 TEXT, amount1 TEXT, sqrt_price_x96 TEXT, liquidity TEXT, tick INT) ON COMMIT DROP",
    "CREATE TEMP TABLE stage_liq (block_time BIGINT, tx_hash TEXT, log_index INT, block_number BIGINT, pool TEXT, kind TEXT, owner TEXT, tick_lower INT, tick_upper INT, amount TEXT, amount0 TEXT, amount1 TEXT) ON COMMIT DROP",
];

// Move staged rows into the partitioned raw tables, idempotently. ON CONFLICT DO
// NOTHING on the same natural keys the per-row path uses keeps a replayed chunk a
// no-op — the dedupe COPY itself cannot express, which is why the rows are staged
// first rather than COPYed straight in.
const BLOCKS_MOVE: &str = "
INSERT INTO blocks (number, block_hash, parent_hash, block_time)
SELECT number, decode(block_hash,'hex'), decode(parent_hash,'hex'), to_timestamp(block_time)
FROM stage_blocks ON CONFLICT (number) DO NOTHING";

const LIQ_MOVE: &str = "
INSERT INTO liq_events (block_time, tx_hash, log_index, block_number, pool, kind, owner, tick_lower, tick_upper, amount, amount0, amount1)
SELECT to_timestamp(block_time), decode(tx_hash,'hex'), log_index, block_number, decode(pool,'hex'), kind, decode(owner,'hex'), tick_lower, tick_upper, amount::numeric, amount0::numeric, amount1::numeric
FROM stage_liq ON CONFLICT (block_time, tx_hash, log_index) DO NOTHING";

// Move in-window swaps into the partitions and fold every new swap into the 1m
// candles (#36), in one statement so the two can never disagree.
//
// The candle set is `moved` — the swaps just inserted, taken from `RETURNING`
// so a replayed chunk (which inserts nothing) contributes nothing and volume is
// never double-counted — UNION the below-window staged swaps, which are folded
// into aggregates but not stored raw. Price is derived from sqrtPriceX96 the
// Uniswap V3 way: (sqrt/2^96)^2 = sqrt^2 / 2^192, a token1/token0 ratio.
//
// On conflict `open` is left untouched: because history is written in block
// order, the earliest swap of a bucket is always seen first, so the stored open
// is already the true open. `close` becomes the latest write's close, high/low
// widen, and volume and trade_count accumulate.
//
// `$1` is the window floor (unix seconds); `i64::MIN` disables it.
const CANDLE_MOVE: &str = "
WITH moved AS (
    INSERT INTO swaps (block_time, tx_hash, log_index, block_number, pool, sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick)
    SELECT to_timestamp(block_time), decode(tx_hash,'hex'), log_index, block_number, decode(pool,'hex'), decode(sender,'hex'), decode(recipient,'hex'), amount0::numeric, amount1::numeric, sqrt_price_x96::numeric, liquidity::numeric, tick
    FROM stage_swaps WHERE block_time >= $1
    ON CONFLICT (block_time, tx_hash, log_index) DO NOTHING
    RETURNING pool, block_time, block_number, log_index, sqrt_price_x96, amount0, amount1
),
new_swaps AS (
    SELECT pool, block_time, block_number, log_index, sqrt_price_x96, amount0, amount1 FROM moved
    UNION ALL
    SELECT decode(pool,'hex'), to_timestamp(block_time), block_number, log_index, sqrt_price_x96::numeric, amount0::numeric, amount1::numeric
    FROM stage_swaps WHERE block_time < $1
),
priced AS (
    SELECT pool,
           date_trunc('minute', block_time) AS bucket,
           block_number, log_index,
           (sqrt_price_x96 * sqrt_price_x96) / power(2::numeric, 192) AS price,
           abs(amount0) AS v0,
           abs(amount1) AS v1
    FROM new_swaps
),
agg AS (
    SELECT pool, bucket,
           (array_agg(price ORDER BY block_number, log_index))[1]           AS open,
           (array_agg(price ORDER BY block_number DESC, log_index DESC))[1] AS close,
           max(price) AS high,
           min(price) AS low,
           sum(v0)    AS volume0,
           sum(v1)    AS volume1,
           count(*)   AS trade_count
    FROM priced GROUP BY pool, bucket
)
INSERT INTO ohlcv_1m (pool, bucket, open, high, low, close, volume0, volume1, trade_count)
SELECT pool, bucket, open, high, low, close, volume0, volume1, trade_count FROM agg
ON CONFLICT (pool, bucket) DO UPDATE SET
    high        = GREATEST(ohlcv_1m.high, EXCLUDED.high),
    low         = LEAST(ohlcv_1m.low, EXCLUDED.low),
    close       = EXCLUDED.close,
    volume0     = ohlcv_1m.volume0 + EXCLUDED.volume0,
    volume1     = ohlcv_1m.volume1 + EXCLUDED.volume1,
    trade_count = ohlcv_1m.trade_count + EXCLUDED.trade_count";

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Bulk-write a chunk's decoded rows via `COPY`, gated by the retention window,
/// advancing the *backfill* cursor in the same transaction.
///
/// The fast, disk-aware historical write path (#35). It does three things at
/// once:
///
///  * **Bulk speed.** Rows are streamed with Postgres `COPY` into per-chunk
///    staging tables, then moved into the partitioned raw tables with a single
///    `INSERT ... SELECT ... ON CONFLICT DO NOTHING`. `COPY` is several times
///    faster than a round trip per row, and staging is what keeps the move
///    idempotent — `COPY` cannot express `ON CONFLICT`, and dropping the
///    natural-key dedupe is not a trade this project makes.
///
///  * **Exactness.** Amounts travel as their decimal string and are cast
///    `::numeric` on the way in — the same decimal round trip the per-row writer
///    uses, so nothing is rounded (see [`write_row_batches`]).
///
///  * **Stream-then-discard.** A block older than `window_floor` (unix seconds)
///    has its raw rows counted but *not* stored — to be folded into the
///    aggregates by #36 — so raw disk stays bounded to the window while the
///    aggregates cover all history. `None` keeps everything, the default until
///    the candle aggregator lands and a finite window becomes safe.
///
/// One transaction: a crash mid-`COPY` rolls the rows and the cursor back
/// together. The cursor advances to `covered_up_to` — the chunk's range top —
/// regardless of how many rows were kept, so empty and below-window blocks are
/// still recorded as done and never re-scanned.
pub async fn bulk_write_backfill(
    pool: &PgPool,
    batches: &[RowBatch],
    covered_up_to: u64,
    window_floor: Option<i64>,
    fail_before_commit: bool,
) -> anyhow::Result<BulkStats> {
    let mut tx = pool
        .begin()
        .await
        .context("could not open bulk backfill transaction")?;

    for stmt in STAGE_STMTS {
        sqlx::query(stmt)
            .execute(&mut *tx)
            .await
            .context("could not create staging tables")?;
    }

    // Each COPY runs in its own helper so the `PgCopyIn` borrow of the connection
    // lives within a single concrete lifetime. Held inline across the send loop
    // instead, it trips the compiler's "Send is not general enough" limitation
    // once the driver future is spawned (which the supervisor does).
    let mut stats = BulkStats::default();
    copy_blocks(&mut tx, batches, window_floor).await?;
    let (sp, sd) = copy_swaps(&mut tx, batches, window_floor).await?;
    let (lp, ld) = copy_liq(&mut tx, batches, window_floor).await?;
    stats.persisted = sp + lp;
    stats.discarded = sd + ld;

    // Move block headers and liquidity events into the partitions (idempotent).
    for stmt in [BLOCKS_MOVE, LIQ_MOVE] {
        sqlx::query(stmt)
            .execute(&mut *tx)
            .await
            .context("could not move staged rows into the raw tables")?;
    }

    // Move in-window swaps into the partitions AND fold every new swap into the 1m
    // candles, in one statement (#36). The candle set is the swaps just inserted
    // (via RETURNING, so a replay contributes nothing) plus the below-window swaps
    // we deliberately do not store raw — so aggregates cover all history while raw
    // disk stays bounded to the window. `$1` is the window floor; `i64::MIN` means
    // no floor, so everything is in-window and nothing is below it.
    let floor = window_floor.unwrap_or(i64::MIN);
    sqlx::query(CANDLE_MOVE)
        .bind(floor)
        .execute(&mut *tx)
        .await
        .context("could not move swaps and fold candles")?;

    sqlx::query(
        "UPDATE chain_state
            SET backfill_cursor = GREATEST(COALESCE(backfill_cursor, -1), $1),
                updated_at       = now()
          WHERE id = 1",
    )
    .bind(covered_up_to as i64)
    .execute(&mut *tx)
    .await
    .context("could not advance the backfill cursor")?;

    if fail_before_commit {
        return Err(anyhow::anyhow!("injected failure before commit"));
    }

    tx.commit()
        .await
        .context("could not commit the bulk backfill transaction")?;
    Ok(stats)
}

/// True when a block at `t` (unix seconds) is inside the retention window.
fn within_window(window_floor: Option<i64>, t: i64) -> bool {
    window_floor.is_none_or(|floor| t >= floor)
}

/// COPY the block headers within the window into `stage_blocks`.
async fn copy_blocks(
    conn: &mut sqlx::postgres::PgConnection,
    batches: &[RowBatch],
    window_floor: Option<i64>,
) -> anyhow::Result<()> {
    let mut copy = conn
        .copy_in_raw("COPY stage_blocks (number, block_hash, parent_hash, block_time) FROM STDIN")
        .await?;
    let mut line = String::new();
    for b in batches
        .iter()
        .filter(|b| within_window(window_floor, b.block_time))
    {
        line.clear();
        let _ = writeln!(
            line,
            "{}\t{}\t{}\t{}",
            b.block_number,
            hex(&b.block_hash),
            hex(&b.parent_hash),
            b.block_time
        );
        copy.send(line.as_bytes()).await?;
    }
    copy.finish().await.context("COPY into stage_blocks failed")?;
    Ok(())
}

/// COPY *all* swaps into `stage_swaps`, returning (persisted, discarded).
///
/// Every swap is staged — in-window and below-window alike — because the candle
/// fold aggregates both. Which become raw rows is decided by the move
/// (`block_time >= floor`), not here. `persisted` counts the in-window swaps that
/// will be stored, `discarded` the below-window ones that feed aggregates only.
async fn copy_swaps(
    conn: &mut sqlx::postgres::PgConnection,
    batches: &[RowBatch],
    window_floor: Option<i64>,
) -> anyhow::Result<(u64, u64)> {
    let (mut persisted, mut discarded) = (0u64, 0u64);
    let mut copy = conn
        .copy_in_raw(
            "COPY stage_swaps (block_time, tx_hash, log_index, block_number, pool, sender, \
             recipient, amount0, amount1, sqrt_price_x96, liquidity, tick) FROM STDIN",
        )
        .await?;
    let mut line = String::new();
    for b in batches {
        let keep = within_window(window_floor, b.block_time);
        for s in &b.swaps {
            line.clear();
            let _ = writeln!(
                line,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                b.block_time,
                hex(&s.tx_hash),
                s.log_index,
                b.block_number,
                hex(&s.pool),
                hex(&s.sender),
                hex(&s.recipient),
                s.amount0,
                s.amount1,
                s.sqrt_price_x96,
                s.liquidity,
                s.tick
            );
            copy.send(line.as_bytes()).await?;
            if keep {
                persisted += 1;
            } else {
                discarded += 1;
            }
        }
    }
    copy.finish().await.context("COPY into stage_swaps failed")?;
    Ok((persisted, discarded))
}

/// COPY the in-window liquidity events into `stage_liq`, returning
/// (persisted, discarded).
async fn copy_liq(
    conn: &mut sqlx::postgres::PgConnection,
    batches: &[RowBatch],
    window_floor: Option<i64>,
) -> anyhow::Result<(u64, u64)> {
    let (mut persisted, mut discarded) = (0u64, 0u64);
    let mut copy = conn
        .copy_in_raw(
            "COPY stage_liq (block_time, tx_hash, log_index, block_number, pool, kind, owner, \
             tick_lower, tick_upper, amount, amount0, amount1) FROM STDIN",
        )
        .await?;
    let mut line = String::new();
    for b in batches {
        let keep = within_window(window_floor, b.block_time);
        for l in &b.liq_events {
            if !keep {
                discarded += 1;
                continue;
            }
            line.clear();
            let _ = writeln!(
                line,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                b.block_time,
                hex(&l.tx_hash),
                l.log_index,
                b.block_number,
                hex(&l.pool),
                l.kind.as_str(),
                hex(&l.owner),
                l.tick_lower,
                l.tick_upper,
                l.amount,
                l.amount0,
                l.amount1
            );
            copy.send(line.as_bytes()).await?;
            persisted += 1;
        }
    }
    copy.finish().await.context("COPY into stage_liq failed")?;
    Ok((persisted, discarded))
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
