//! Database connection and schema migration.
//!
//! Migrations are embedded into the binary at compile time by `migrate!`, so a
//! deployed indexer carries its own schema and there is no separate migration
//! step to forget. On startup it compares the embedded set against the
//! `_sqlx_migrations` table and applies only what is missing — running twice
//! against the same database is a no-op.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::str::FromStr;

use anyhow::Context;
use bigdecimal::num_bigint::Sign;
use bigdecimal::BigDecimal;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::Json;
use sqlx::{Postgres, Row};

use crate::config::Database;
use crate::pnl::fifo::{Lot, Position, SellOutcome};
use crate::pnl::{classify, Classified, Numeraire, PoolMeta};
use chainscope_core::types::Address20;
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

// Candle compensation for a reorg (#47), all inside the rewind transaction.
//
// Deleting orphaned swaps is only half the correction: the candles those swaps
// fed still carry their volume — and, worse, a high or low that the deleted
// trade set. Volume and count could be subtracted, but open/high/low/close
// cannot: removing the trade that *was* the high means the new high can only be
// found from what remains. So every affected bucket is recomputed from the
// swaps that survive, not patched.
//
// `reorg_buckets` captures the affected `(pool, minute)` buckets *before* the
// swaps are deleted; the recompute reads them back and rebuilds each candle
// from the surviving raw rows. A reorg only ever touches pending (recent)
// blocks, which sit inside the raw retention window, so every surviving
// swap of an affected bucket is still on disk — a recent bucket cannot also
// hold an old, below-window swap that was folded then discarded, because those
// swaps are not in the same minute. So the surviving raw rows are the whole
// truth for the bucket, and a full recompute is exact.
const REORG_CAPTURE_BUCKETS: &str = "
CREATE TEMP TABLE reorg_buckets ON COMMIT DROP AS
SELECT DISTINCT pool, date_trunc('minute', block_time) AS bucket
FROM swaps WHERE block_number > $1";

// Drop every affected bucket's candle, then reinsert only those that still have
// surviving swaps. A bucket left with no survivors produces no group row, so it
// stays deleted — the minute becomes a true gap again.
const REORG_DROP_AFFECTED_CANDLES: &str = "
DELETE FROM ohlcv_1m o USING reorg_buckets b
WHERE o.pool = b.pool AND o.bucket = b.bucket";

const REORG_RECOMPUTE_CANDLES: &str = "
INSERT INTO ohlcv_1m (pool, bucket, open, high, low, close, volume0, volume1, trade_count)
SELECT pool, bucket,
       (array_agg(price ORDER BY block_number, log_index))[1]           AS open,
       max(price) AS high,
       min(price) AS low,
       (array_agg(price ORDER BY block_number DESC, log_index DESC))[1] AS close,
       sum(v0) AS volume0, sum(v1) AS volume1, count(*) AS trade_count
FROM (
    SELECT s.pool,
           date_trunc('minute', s.block_time) AS bucket,
           s.block_number, s.log_index,
           (s.sqrt_price_x96 * s.sqrt_price_x96) / power(2::numeric, 192) AS price,
           abs(s.amount0) AS v0,
           abs(s.amount1) AS v1
    FROM swaps s
    JOIN reorg_buckets b
      ON s.pool = b.pool AND date_trunc('minute', s.block_time) = b.bucket
) priced
GROUP BY pool, bucket";

/// Unwind every row above `fork_point`, compensate the candles it fed, and reset
/// the live cursor to it, atomically.
///
/// A reorg replaced the blocks above `fork_point`. This deletes the orphaned
/// headers, swaps and liquidity events, recomputes every candle those swaps
/// touched from the swaps that survive (#47), and moves `live_cursor` back to
/// `fork_point` so the producer re-indexes the canonical branch forward through
/// the ordinary write path. All of it is one transaction: a crash can leave only
/// the pre-rewind state or the fully-rewound state, and a reader never sees
/// candles that disagree with the raw rows beneath them.
///
/// Re-indexing the canonical branch is deliberately *not* held inside this
/// transaction. Fetching canonical blocks is network round trips, and pinning a
/// Postgres transaction open across them would hold locks for the length of an
/// RPC storm. Because the cursor is reset atomically with the delete, the
/// forward re-index is an ordinary resumable write — each canonical block
/// commits with its own exactly-once guarantee.
///
/// The cursor is *set*, not `GREATEST`-ed: a rewind must move it back.
///
/// Returns the number of block headers removed. `fail_before_commit` exists only
/// for tests, to assert the whole rewind rolls back together.
pub async fn rewind_to(
    pool: &PgPool,
    fork_point: u64,
    fail_before_commit: bool,
) -> anyhow::Result<u64> {
    let f = fork_point as i64;
    let mut tx = pool.begin().await.context("could not open the rewind transaction")?;

    // Capture the candle buckets the orphaned swaps fed, before the swaps go.
    sqlx::query(REORG_CAPTURE_BUCKETS)
        .bind(f)
        .execute(&mut *tx)
        .await
        .context("could not capture the affected candle buckets")?;

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

    // Recompute the affected candles from the survivors: drop them all, then
    // reinsert the buckets that still have swaps.
    for stmt in [REORG_DROP_AFFECTED_CANDLES, REORG_RECOMPUTE_CANDLES] {
        sqlx::query(stmt)
            .execute(&mut *tx)
            .await
            .context("could not recompute the affected candles")?;
    }

    // Undo the PnL the orphaned swaps produced, exactly, from the ledger. A no-op
    // when nothing was folded (PnL off), so the channel/kafka reorg paths that
    // run without a numeraire pay nothing here.
    reverse_pnl(&mut tx, f).await?;

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

// The live path's 1m candle fold (#111). Same price math and conflict rules as
// CANDLE_MOVE, but fed by arrays of the swaps just inserted (unnest) rather than
// a staging table — the live writer inserts per row, not by COPY. `open` is left
// untouched on conflict: live blocks arrive in order, so a bucket's earliest
// swap is seen first and the stored open is already true.
const CANDLE_LIVE: &str = "
WITH priced AS (
    SELECT pool,
           date_trunc('minute', to_timestamp(bt)) AS bucket,
           bn, li,
           (sp * sp) / power(2::numeric, 192) AS price,
           abs(a0) AS v0, abs(a1) AS v1
    FROM unnest($1::bytea[], $2::bigint[], $3::bigint[], $4::int[],
                $5::numeric[], $6::numeric[], $7::numeric[])
         AS t(pool, bt, bn, li, sp, a0, a1)
),
agg AS (
    SELECT pool, bucket,
           (array_agg(price ORDER BY bn, li))[1]           AS open,
           (array_agg(price ORDER BY bn DESC, li DESC))[1] AS close,
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
    write_row_batches_with_pnl(pool, batches, fail_before_commit, &Numeraire::disabled()).await
}

/// Like [`write_row_batches`], but also folds FIFO cost-basis PnL for every swap
/// that is *newly inserted this transaction* — into `wallet_positions`,
/// `wallet_stats` and the `lot_consumptions` ledger — inside the one commit that
/// carries the rows and the cursor.
///
/// A replayed batch inserts no swaps (the `ON CONFLICT` gate), so it folds no
/// PnL: exactly-once for derived state is inherited from the same RETURNING
/// discipline that already protects the candles, never re-earned. When the
/// numeraire prices nothing the fold is skipped entirely, so the plain
/// `write_row_batches` path pays nothing.
pub async fn write_row_batches_with_pnl(
    pool: &PgPool,
    batches: &[RowBatch],
    fail_before_commit: bool,
    numeraire: &Numeraire,
) -> anyhow::Result<u64> {
    if batches.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await.context("could not open write transaction")?;

    // Resolve the WETH/USD reference once for the batch — blocks are small, and
    // a single candle read keeps the fold from querying per swap.
    let active = numeraire.is_active();
    let weth_usd = if active {
        resolve_weth_usd(&mut tx, numeraire).await?
    } else {
        None
    };
    let price = numeraire.pricer(weth_usd);
    let mut pool_meta: HashMap<Address20, Option<PoolMeta>> = HashMap::new();

    // Candle inputs for the swaps newly inserted this tx. Folding from the
    // inserted set (not the incoming batch) keeps replay a zero delta, exactly
    // as the bulk path does.
    let mut c_pool: Vec<Vec<u8>> = Vec::new();
    let mut c_bt: Vec<i64> = Vec::new();
    let mut c_bn: Vec<i64> = Vec::new();
    let mut c_li: Vec<i32> = Vec::new();
    let mut c_sp: Vec<BigDecimal> = Vec::new();
    let mut c_a0: Vec<BigDecimal> = Vec::new();
    let mut c_a1: Vec<BigDecimal> = Vec::new();

    for b in batches {
        insert_block(&mut tx, b).await?;
        for s in &b.swaps {
            let inserted = insert_swap(&mut tx, b, s).await?;
            if inserted {
                c_pool.push(s.pool.to_vec());
                c_bt.push(b.block_time);
                c_bn.push(b.block_number as i64);
                c_li.push(s.log_index as i32);
                c_sp.push(s.sqrt_price_x96.clone());
                c_a0.push(s.amount0.clone());
                c_a1.push(s.amount1.clone());
                if active {
                    fold_swap_pnl(&mut tx, b.block_number, s, &price, &mut pool_meta).await?;
                }
            }
        }
        for l in &b.liq_events {
            insert_liq(&mut tx, b, l).await?;
        }
    }

    // Fold the 1m candles for the newly-inserted swaps — the live path's
    // equivalent of the bulk candle fold, so live prices survive raw pruning.
    if !c_pool.is_empty() {
        sqlx::query(CANDLE_LIVE)
            .bind(&c_pool)
            .bind(&c_bt)
            .bind(&c_bn)
            .bind(&c_li)
            .bind(&c_sp)
            .bind(&c_a0)
            .bind(&c_a1)
            .execute(&mut *tx)
            .await
            .context("could not fold live candles")?;
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

/// Insert one swap, returning whether it was *newly* inserted. A conflict (the
/// row already exists, i.e. a replay) returns `None` from `RETURNING`, so the
/// caller knows not to fold its PnL a second time — the gate that makes the
/// derived state exactly-once.
async fn insert_swap(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    b: &RowBatch,
    s: &SwapRow,
) -> anyhow::Result<bool> {
    // block_time and block_number come from the block, not the row: block_time
    // is the partition key, and it must match the value the block header used so
    // the conflict on `(block_time, tx_hash, log_index)` fires on replay.
    let inserted = sqlx::query(
        "INSERT INTO swaps
            (block_time, tx_hash, log_index, block_number, pool, sender, recipient,
             amount0, amount1, sqrt_price_x96, liquidity, tick)
         VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         ON CONFLICT (block_time, tx_hash, log_index) DO NOTHING
         RETURNING 1 AS inserted",
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
    .fetch_optional(&mut **tx)
    .await
    .with_context(|| format!("could not insert swap at block {}", b.block_number))?;
    Ok(inserted.is_some())
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

// ---------------------------------------------------------------------------
// FIFO cost-basis PnL fold (#72)
//
// Everything below runs inside the writer transaction, gated on a swap being
// newly inserted, so it commits atomically with the rows and the cursor and a
// replay contributes nothing.
// ---------------------------------------------------------------------------

/// The JSONB shape of one `wallet_positions.lots` entry. Amounts are decimal
/// strings — NUMERIC precision through JSON without going via float.
#[derive(serde::Serialize, serde::Deserialize)]
struct LotJson {
    qty: String,
    price_usd: String,
    block: i64,
}

impl LotJson {
    fn from_lot(l: &Lot) -> Self {
        Self {
            qty: l.qty.to_string(),
            price_usd: l.price_usd.to_string(),
            block: l.block as i64,
        }
    }
    fn into_lot(self) -> Lot {
        Lot {
            qty: BigDecimal::from_str(&self.qty).unwrap_or_else(|_| BigDecimal::from(0)),
            price_usd: BigDecimal::from_str(&self.price_usd).unwrap_or_else(|_| BigDecimal::from(0)),
            block: self.block as u64,
        }
    }
}

/// The current WETH/USD reference from our own candles, or `None` when the
/// numeraire has no WETH pool or that pool has no candle yet. `ohlcv_1m.close`
/// for a WETH/stable pool is USD-per-WETH by construction.
async fn resolve_weth_usd(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    numeraire: &Numeraire,
) -> anyhow::Result<Option<BigDecimal>> {
    let Some(pool) = numeraire.weth_price_pool else {
        return Ok(None);
    };
    let close: Option<BigDecimal> =
        sqlx::query_scalar("SELECT close FROM ohlcv_1m WHERE pool = $1 ORDER BY bucket DESC LIMIT 1")
            .bind(pool.as_slice())
            .fetch_optional(&mut **tx)
            .await
            .context("could not read the WETH reference price")?;
    Ok(close)
}

/// Pool token metadata, cached per transaction. `None` means the pool is unknown
/// or has no decimals recorded, so its swaps cannot be classified and are left
/// out of PnL (counted as raw rows only).
async fn pool_meta_cached(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    pool: Address20,
    cache: &mut HashMap<Address20, Option<PoolMeta>>,
) -> anyhow::Result<Option<PoolMeta>> {
    if let Some(hit) = cache.get(&pool) {
        return Ok(hit.clone());
    }
    let row = sqlx::query(
        "SELECT token0, token1, token0_decimals, token1_decimals FROM pools WHERE address = $1",
    )
    .bind(pool.as_slice())
    .fetch_optional(&mut **tx)
    .await
    .context("could not read pool metadata")?;

    let meta = row.and_then(|r| {
        let token0: Vec<u8> = r.get("token0");
        let token1: Vec<u8> = r.get("token1");
        let d0: Option<i16> = r.get("token0_decimals");
        let d1: Option<i16> = r.get("token1_decimals");
        match (
            token0.try_into().ok(),
            token1.try_into().ok(),
            d0,
            d1,
        ) {
            (Some(t0), Some(t1), Some(d0), Some(d1)) if d0 >= 0 && d1 >= 0 => Some(PoolMeta {
                token0: t0,
                token1: t1,
                token0_decimals: d0 as u8,
                token1_decimals: d1 as u8,
            }),
            _ => None,
        }
    });
    cache.insert(pool, meta.clone());
    Ok(meta)
}

/// Fold one newly-inserted swap into PnL: open a lot in the bought token, consume
/// lots of the sold token, record the drawdowns, and bump the wallet's stats.
async fn fold_swap_pnl<F>(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    block_number: u64,
    s: &SwapRow,
    price: &F,
    cache: &mut HashMap<Address20, Option<PoolMeta>>,
) -> anyhow::Result<()>
where
    F: Fn(&Address20) -> Option<BigDecimal>,
{
    let Some(meta) = pool_meta_cached(tx, s.pool, cache).await? else {
        return Ok(());
    };
    let trade = match classify(s, &meta, price) {
        Classified::Priced(t) => t,
        Classified::Unpriceable { .. } => return Ok(()),
    };

    // Only the non-numeraire (asset) leg is a tracked position — the numeraire
    // leg is money, the yardstick, not something a wallet holds a cost basis in.
    // Buying the asset opens a lot; selling it realises against the lots. A
    // money-for-money swap (both legs priced) has no asset to track, and a swap
    // with a priced leg always has exactly one unpriced one.
    let bought_is_money = price(&trade.bought).is_some();
    let sold_is_money = price(&trade.sold).is_some();

    match (bought_is_money, sold_is_money) {
        // Bought the asset, paid money: open a lot at the trade's per-unit cost.
        (false, true) => {
            if trade.bought_qty.sign() != Sign::Plus {
                return Ok(());
            }
            let unit_cost = trade.value_usd.clone() / trade.bought_qty.clone();
            let mut pos = load_position(tx, &trade.wallet, &trade.bought).await?;
            pos.buy(trade.bought_qty.clone(), unit_cost, block_number);
            save_position(tx, &trade.wallet, &trade.bought, &pos, block_number).await?;
            // A buy realises nothing, but it is still a trade the wallet made.
            bump_wallet_stats(
                tx,
                &trade.wallet,
                &BigDecimal::from(0),
                &trade.value_usd,
                block_number,
            )
            .await?;
        }
        // Sold the asset, received money: consume lots FIFO and realise.
        (true, false) => {
            if trade.sold_qty.sign() != Sign::Plus {
                return Ok(());
            }
            let mut pos = load_position(tx, &trade.wallet, &trade.sold).await?;
            let outcome = pos.sell(&trade.sold_qty, &trade.value_usd, block_number);
            save_position(tx, &trade.wallet, &trade.sold, &pos, block_number).await?;
            insert_consumptions(tx, s, &trade.sold, block_number, &outcome).await?;
            bump_wallet_stats(
                tx,
                &trade.wallet,
                &outcome.realized_pnl_usd,
                &trade.value_usd,
                block_number,
            )
            .await?;
        }
        // Money-for-money, or (unreachable) two unpriced legs: no asset position.
        _ => {}
    }
    Ok(())
}

/// Read a wallet's open lots for one token; an absent row is an empty position.
async fn load_position(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    wallet: &Address20,
    token: &Address20,
) -> anyhow::Result<Position> {
    let row = sqlx::query("SELECT lots FROM wallet_positions WHERE wallet = $1 AND token = $2")
        .bind(wallet.as_slice())
        .bind(token.as_slice())
        .fetch_optional(&mut **tx)
        .await
        .context("could not load wallet position")?;
    let lots = match row {
        Some(r) => {
            let Json(js): Json<Vec<LotJson>> = r.get("lots");
            js.into_iter().map(LotJson::into_lot).collect()
        }
        None => Vec::new(),
    };
    Ok(Position { lots })
}

/// Write a position back. `qty_held`/`cost_basis_usd` are recomputed from the
/// lots so the summary columns can never drift from the queue they summarise.
async fn save_position(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    wallet: &Address20,
    token: &Address20,
    pos: &Position,
    block: u64,
) -> anyhow::Result<()> {
    let lots: Vec<LotJson> = pos.lots.iter().map(LotJson::from_lot).collect();
    sqlx::query(
        "INSERT INTO wallet_positions
            (wallet, token, qty_held, cost_basis_usd, lots, updated_block)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (wallet, token) DO UPDATE SET
            qty_held       = EXCLUDED.qty_held,
            cost_basis_usd = EXCLUDED.cost_basis_usd,
            lots           = EXCLUDED.lots,
            updated_block  = EXCLUDED.updated_block",
    )
    .bind(wallet.as_slice())
    .bind(token.as_slice())
    .bind(pos.qty_held())
    .bind(pos.cost_basis_usd())
    .bind(Json(lots))
    .bind(block as i64)
    .execute(&mut **tx)
    .await
    .context("could not save wallet position")?;
    Ok(())
}

/// Append the sell's drawdowns to the ledger. Keyed on `(sell_tx, sell_log,
/// consume_seq)`, so a replay of the sell writes nothing.
async fn insert_consumptions(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    s: &SwapRow,
    token: &Address20,
    block: u64,
    outcome: &SellOutcome,
) -> anyhow::Result<()> {
    for (seq, c) in outcome.consumptions.iter().enumerate() {
        sqlx::query(
            "INSERT INTO lot_consumptions
                (sell_tx, sell_log, consume_seq, wallet, token, qty_consumed,
                 lot_unit_cost_usd, lot_acquired_block, proceeds_usd,
                 realized_pnl_usd, sell_block)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (sell_tx, sell_log, consume_seq) DO NOTHING",
        )
        .bind(s.tx_hash.as_slice())
        .bind(s.log_index as i32)
        .bind(seq as i32)
        .bind(s.recipient.as_slice())
        .bind(token.as_slice())
        .bind(c.qty_consumed.clone())
        .bind(c.lot_unit_cost_usd.clone())
        .bind(c.lot_block as i64)
        .bind(c.proceeds_usd.clone())
        .bind(c.realized_pnl_usd.clone())
        .bind(block as i64)
        .execute(&mut **tx)
        .await
        .context("could not record lot consumption")?;
    }
    Ok(())
}

/// Accumulate one swap into the wallet's rollup. Additive on conflict, which is
/// safe precisely because the fold only runs for a newly-inserted swap — a
/// replay never reaches here.
async fn bump_wallet_stats(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    wallet: &Address20,
    realized: &BigDecimal,
    value_usd: &BigDecimal,
    block: u64,
) -> anyhow::Result<()> {
    let win: i32 = if realized.sign() == Sign::Plus { 1 } else { 0 };
    sqlx::query(
        "INSERT INTO wallet_stats
            (wallet, realized_pnl_usd, trades, wins, volume_usd, avg_size_usd, last_active_block)
         VALUES ($1, $2, 1, $3, $4, $4, $5)
         ON CONFLICT (wallet) DO UPDATE SET
            realized_pnl_usd  = wallet_stats.realized_pnl_usd + EXCLUDED.realized_pnl_usd,
            trades            = wallet_stats.trades + 1,
            wins              = wallet_stats.wins + EXCLUDED.wins,
            volume_usd        = wallet_stats.volume_usd + EXCLUDED.volume_usd,
            avg_size_usd      = (wallet_stats.volume_usd + EXCLUDED.volume_usd)
                                / (wallet_stats.trades + 1),
            last_active_block = GREATEST(COALESCE(wallet_stats.last_active_block, -1),
                                         EXCLUDED.last_active_block)",
    )
    .bind(wallet.as_slice())
    .bind(realized.clone())
    .bind(win)
    .bind(value_usd.clone())
    .bind(block as i64)
    .execute(&mut **tx)
    .await
    .context("could not update wallet stats")?;
    Ok(())
}

/// Merge lot fragments that belong to the same original lot — same acquisition
/// block and unit cost — back into one, then order the queue FIFO (block asc).
///
/// A reorg restores a lot piecemeal (one fragment per drawdown that touched it),
/// so a lot originally acquired as a single buy comes back as several fragments;
/// coalescing rebuilds the single lot the canonical chain would have.
fn coalesce_lots(lots: Vec<Lot>) -> Vec<Lot> {
    let mut acc: Vec<(u64, String, BigDecimal, BigDecimal)> = Vec::new();
    for l in lots {
        let key = l.price_usd.normalized().to_string();
        if let Some(e) = acc.iter_mut().find(|e| e.0 == l.block && e.1 == key) {
            e.2 += &l.qty;
        } else {
            acc.push((l.block, key, l.qty, l.price_usd));
        }
    }
    acc.sort_by_key(|e| e.0);
    acc.into_iter()
        .map(|(block, _, qty, price_usd)| Lot {
            qty,
            price_usd,
            block,
        })
        .collect()
}

/// Undo, exactly, the PnL every swap above `fork` produced (#73).
///
/// Realised PnL is a sum of recorded contributions, so a reorg *reverses* rather
/// than recomputes (which FIFO could not do from the pruned survivors): restore
/// what each above-fork sell consumed, drop what each above-fork buy opened, and
/// back the totals out of `wallet_stats`. Runs inside the rewind transaction, so
/// it commits atomically with the swap/candle deletes and the cursor reset.
///
/// Idempotent: a redelivered revert (Kafka is at-least-once) finds nothing above
/// the fork and changes nothing. A no-op when PnL was never folded.
async fn reverse_pnl(tx: &mut sqlx::Transaction<'_, Postgres>, fork: i64) -> anyhow::Result<()> {
    #[derive(Default)]
    struct Delta {
        realized: BigDecimal,
        volume: BigDecimal,
        trades: i64,
        wins: i64,
    }
    let mut deltas: HashMap<Address20, Delta> = HashMap::new();
    let mut restore: HashMap<(Address20, Address20), Vec<Lot>> = HashMap::new();
    // Aggregate the ledger per sell so each sell counts as one trade, one win.
    let mut sells: HashMap<(Vec<u8>, i32), (Address20, BigDecimal, BigDecimal)> = HashMap::new();

    // --- A) sells above the fork: gather restorations and per-sell totals ---
    let rows = sqlx::query(
        "SELECT wallet, token, sell_tx, sell_log, qty_consumed, lot_unit_cost_usd,
                lot_acquired_block, proceeds_usd, realized_pnl_usd
           FROM lot_consumptions WHERE sell_block > $1",
    )
    .bind(fork)
    .fetch_all(&mut **tx)
    .await
    .context("could not read the consumption ledger for reversal")?;

    for r in &rows {
        let wallet: Vec<u8> = r.get("wallet");
        let token: Vec<u8> = r.get("token");
        let wa: Address20 = wallet.try_into().unwrap_or([0; 20]);
        let ta: Address20 = token.try_into().unwrap_or([0; 20]);
        let sell_tx: Vec<u8> = r.get("sell_tx");
        let sell_log: i32 = r.get("sell_log");
        let qty: BigDecimal = r.get("qty_consumed");
        let unit: BigDecimal = r.get("lot_unit_cost_usd");
        let lot_block: i64 = r.get("lot_acquired_block");
        let proceeds: BigDecimal = r.get("proceeds_usd");
        let realized: BigDecimal = r.get("realized_pnl_usd");

        restore.entry((wa, ta)).or_default().push(Lot {
            qty,
            price_usd: unit,
            block: lot_block as u64,
        });
        let e = sells
            .entry((sell_tx, sell_log))
            .or_insert_with(|| (wa, BigDecimal::from(0), BigDecimal::from(0)));
        e.1 += realized;
        e.2 += proceeds;
    }
    for (_, (wa, realized, proceeds)) in sells {
        let d = deltas.entry(wa).or_default();
        d.realized += &realized;
        d.volume += &proceeds;
        d.trades += 1;
        if realized.sign() == Sign::Plus {
            d.wins += 1;
        }
    }
    sqlx::query("DELETE FROM lot_consumptions WHERE sell_block > $1")
        .bind(fork)
        .execute(&mut **tx)
        .await
        .context("could not delete reversed consumptions")?;

    // --- B) touched positions: restored ones, plus any still holding a buy > fork ---
    let bpos = sqlx::query(
        "SELECT wallet, token FROM wallet_positions
          WHERE jsonb_path_exists(lots, '$[*] ? (@.block > $f)',
                                  jsonb_build_object('f', $1::bigint))",
    )
    .bind(fork)
    .fetch_all(&mut **tx)
    .await
    .context("could not find positions with orphaned lots")?;

    let mut touched: HashSet<(Address20, Address20)> = restore.keys().copied().collect();
    for r in &bpos {
        let wallet: Vec<u8> = r.get("wallet");
        let token: Vec<u8> = r.get("token");
        touched.insert((
            wallet.try_into().unwrap_or([0; 20]),
            token.try_into().unwrap_or([0; 20]),
        ));
    }

    for (wa, ta) in touched {
        let mut pos = load_position(tx, &wa, &ta).await?;
        if let Some(mut frags) = restore.remove(&(wa, ta)) {
            pos.lots.append(&mut frags);
        }
        pos.lots = coalesce_lots(pos.lots);

        // Every lot left above the fork is a buy to back out.
        let mut kept = Vec::new();
        for l in pos.lots.drain(..) {
            if l.block as i64 > fork {
                let d = deltas.entry(wa).or_default();
                d.volume += &l.qty * &l.price_usd;
                d.trades += 1;
            } else {
                kept.push(l);
            }
        }
        pos.lots = kept;

        if pos.lots.is_empty() {
            sqlx::query("DELETE FROM wallet_positions WHERE wallet = $1 AND token = $2")
                .bind(wa.as_slice())
                .bind(ta.as_slice())
                .execute(&mut **tx)
                .await
                .context("could not drop an emptied position")?;
        } else {
            save_position(tx, &wa, &ta, &pos, fork as u64).await?;
        }
    }

    // --- C) subtract the deltas; drop a wallet whose trades fall to zero ---
    for (wa, d) in deltas {
        let Some(row) = sqlx::query("SELECT trades FROM wallet_stats WHERE wallet = $1")
            .bind(wa.as_slice())
            .fetch_optional(&mut **tx)
            .await
            .context("could not read wallet stats for reversal")?
        else {
            continue;
        };
        let trades: i32 = row.get("trades");
        if trades - d.trades as i32 <= 0 {
            sqlx::query("DELETE FROM wallet_stats WHERE wallet = $1")
                .bind(wa.as_slice())
                .execute(&mut **tx)
                .await
                .context("could not drop an emptied wallet")?;
            continue;
        }
        // Last activity is a max over the survivors — recomputable, unlike the sums.
        let last_active: Option<i64> = sqlx::query_scalar(
            "SELECT GREATEST(
                (SELECT max((l->>'block')::bigint)
                   FROM wallet_positions wp, jsonb_array_elements(wp.lots) l
                  WHERE wp.wallet = $1),
                (SELECT max(sell_block) FROM lot_consumptions WHERE wallet = $1))",
        )
        .bind(wa.as_slice())
        .fetch_one(&mut **tx)
        .await
        .context("could not recompute last-active block")?;

        sqlx::query(
            "UPDATE wallet_stats SET
                realized_pnl_usd  = realized_pnl_usd - $2,
                volume_usd        = volume_usd - $3,
                trades            = trades - $4,
                wins              = wins - $5,
                avg_size_usd      = (volume_usd - $3) / (trades - $4),
                last_active_block = $6
             WHERE wallet = $1",
        )
        .bind(wa.as_slice())
        .bind(d.realized)
        .bind(d.volume)
        .bind(d.trades as i32)
        .bind(d.wins as i32)
        .bind(last_active)
        .execute(&mut **tx)
        .await
        .context("could not back stats out")?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Wash-trade filter (#74)
// ---------------------------------------------------------------------------

/// Thresholds for the wash-trade heuristics. All documented; conservative by
/// default so a normal active trader is never flagged.
#[derive(Debug, Clone)]
pub struct WashParams {
    /// Flag a wallet with at least this many self-trades (sender == recipient).
    pub self_trade_min: i64,
    /// A (wallet, pool) needs at least this many swaps to be considered churn.
    pub churn_min_trades: i64,
    /// …and a net position under this fraction of its gross volume — lots of
    /// trading that nets to almost nothing, i.e. volume without exposure.
    pub churn_net_ratio: f64,
}

impl Default for WashParams {
    fn default() -> Self {
        Self {
            self_trade_min: 3,
            churn_min_trades: 6,
            churn_net_ratio: 0.05,
        }
    }
}

/// Recompute `wallet_stats.excluded` from the recent raw swaps, flagging wallets
/// whose volume looks manufactured. Returns how many wallets are now excluded.
///
/// The flag is **set**, not toggled — a pure function of the surviving swaps —
/// so re-running after a replay or reorg converges to the same answer. The flag
/// lives on `wallet_stats` and so survives the raw swaps being pruned; the
/// leaderboard's partial index (`WHERE excluded = FALSE`) does the exclusion.
///
/// Heuristics, deliberately simple, documented on [`WashParams`]:
/// self-trading (`sender == recipient`) and churn (many trades in a pool that
/// net to almost no position).
pub async fn flag_wash_trading(pool: &PgPool, p: &WashParams) -> anyhow::Result<u64> {
    sqlx::query(
        "WITH self_trades AS (
             SELECT recipient AS wallet FROM swaps
              WHERE sender = recipient
              GROUP BY recipient
             HAVING count(*) >= $1
         ),
         churn AS (
             SELECT recipient AS wallet FROM swaps
              GROUP BY recipient, pool
             HAVING count(*) >= $2
                AND sum(abs(amount0)) > 0
                AND abs(sum(amount0))::float8 < $3 * sum(abs(amount0))::float8
         ),
         wash AS (SELECT wallet FROM self_trades UNION SELECT wallet FROM churn)
         UPDATE wallet_stats
            SET excluded = (wallet IN (SELECT wallet FROM wash))",
    )
    .bind(p.self_trade_min)
    .bind(p.churn_min_trades)
    .bind(p.churn_net_ratio)
    .execute(pool)
    .await
    .context("could not recompute wash-trade flags")?;

    let excluded: i64 = sqlx::query_scalar("SELECT count(*) FROM wallet_stats WHERE excluded")
        .fetch_one(pool)
        .await
        .context("could not count excluded wallets")?;
    Ok(excluded as u64)
}

// ---------------------------------------------------------------------------
// Leaderboard and wallet scorecard (#75)
//
// Read-side aggregates. M7 turns these into HTTP; here they are plain typed `db`
// functions so the API stays a thin layer and the exit test can assert them.
// ---------------------------------------------------------------------------

/// One leaderboard entry.
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderRow {
    pub wallet: Address20,
    pub realized_pnl_usd: BigDecimal,
    pub trades: i32,
    pub wins: i32,
    pub volume_usd: BigDecimal,
}

/// A wallet's still-open position in one token.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenPosition {
    pub token: Address20,
    pub qty_held: BigDecimal,
    pub cost_basis_usd: BigDecimal,
}

/// One realised drawdown in a wallet's recent history.
#[derive(Debug, Clone, PartialEq)]
pub struct RealizedTrade {
    pub sell_block: i64,
    pub token: Address20,
    pub qty: BigDecimal,
    pub proceeds_usd: BigDecimal,
    pub realized_pnl_usd: BigDecimal,
}

/// The full picture for one wallet: its rollup, open positions (unrealised, kept
/// separate from realised), and a recent realised trail.
#[derive(Debug, Clone, PartialEq)]
pub struct Scorecard {
    pub wallet: Address20,
    pub realized_pnl_usd: BigDecimal,
    pub trades: i32,
    pub wins: i32,
    pub volume_usd: BigDecimal,
    pub excluded: bool,
    pub open_positions: Vec<OpenPosition>,
    pub recent_realized: Vec<RealizedTrade>,
}

fn addr(bytes: Vec<u8>) -> Address20 {
    bytes.try_into().unwrap_or([0; 20])
}

/// Recompute the watchlist snapshot. Cheap — the view is at most 100 rows.
pub async fn refresh_leaderboard(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query("REFRESH MATERIALIZED VIEW leaderboard")
        .execute(pool)
        .await
        .context("could not refresh the leaderboard")?;
    Ok(())
}

/// The top `limit` wallets by realised PnL, wash-excluded, from the snapshot.
pub async fn leaderboard(pool: &PgPool, limit: i64) -> anyhow::Result<Vec<LeaderRow>> {
    let rows = sqlx::query(
        "SELECT wallet, realized_pnl_usd, trades, wins, volume_usd
           FROM leaderboard
          ORDER BY realized_pnl_usd DESC
          LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("could not read the leaderboard")?;

    Ok(rows
        .into_iter()
        .map(|r| LeaderRow {
            wallet: addr(r.get("wallet")),
            realized_pnl_usd: r.get("realized_pnl_usd"),
            trades: r.get("trades"),
            wins: r.get("wins"),
            volume_usd: r.get("volume_usd"),
        })
        .collect())
}

/// One wallet's scorecard, or `None` if the wallet has no stats. `recent` bounds
/// the realised trail.
pub async fn scorecard(
    pool: &PgPool,
    wallet: &Address20,
    recent: i64,
) -> anyhow::Result<Option<Scorecard>> {
    let Some(s) = sqlx::query(
        "SELECT realized_pnl_usd, trades, wins, volume_usd, excluded
           FROM wallet_stats WHERE wallet = $1",
    )
    .bind(wallet.as_slice())
    .fetch_optional(pool)
    .await
    .context("could not read wallet stats")?
    else {
        return Ok(None);
    };

    let positions = sqlx::query(
        "SELECT token, qty_held, cost_basis_usd
           FROM wallet_positions
          WHERE wallet = $1 AND qty_held > 0
          ORDER BY cost_basis_usd DESC",
    )
    .bind(wallet.as_slice())
    .fetch_all(pool)
    .await
    .context("could not read open positions")?
    .into_iter()
    .map(|r| OpenPosition {
        token: addr(r.get("token")),
        qty_held: r.get("qty_held"),
        cost_basis_usd: r.get("cost_basis_usd"),
    })
    .collect();

    let recent_realized = sqlx::query(
        "SELECT sell_block, token, qty_consumed, proceeds_usd, realized_pnl_usd
           FROM lot_consumptions
          WHERE wallet = $1
          ORDER BY sell_block DESC, consume_seq DESC
          LIMIT $2",
    )
    .bind(wallet.as_slice())
    .bind(recent)
    .fetch_all(pool)
    .await
    .context("could not read the realised trail")?
    .into_iter()
    .map(|r| RealizedTrade {
        sell_block: r.get("sell_block"),
        token: addr(r.get("token")),
        qty: r.get("qty_consumed"),
        proceeds_usd: r.get("proceeds_usd"),
        realized_pnl_usd: r.get("realized_pnl_usd"),
    })
    .collect();

    Ok(Some(Scorecard {
        wallet: *wallet,
        realized_pnl_usd: s.get("realized_pnl_usd"),
        trades: s.get("trades"),
        wins: s.get("wins"),
        volume_usd: s.get("volume_usd"),
        excluded: s.get("excluded"),
        open_positions: positions,
        recent_realized,
    }))
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

// ---------------------------------------------------------------------------
// New-pool capture (#103)
// ---------------------------------------------------------------------------

/// Record a pool discovered from a factory `PoolCreated`. `is_indexed = false`
/// (discovery never widens ingestion), with a risk scorecard. Idempotent —
/// returns whether this call inserted a new row.
#[allow(clippy::too_many_arguments)]
pub async fn capture_new_pool(
    pool: &PgPool,
    address: &Address20,
    token0: &Address20,
    token1: &Address20,
    fee: i32,
    tick_spacing: i32,
    created_block: i64,
    risk_flags: &serde_json::Value,
) -> anyhow::Result<bool> {
    let inserted = sqlx::query(
        "INSERT INTO pools
            (address, token0, token1, fee, tick_spacing, created_block, is_indexed, risk_flags)
         VALUES ($1, $2, $3, $4, $5, $6, false, $7)
         ON CONFLICT (address) DO NOTHING
         RETURNING 1",
    )
    .bind(address.as_slice())
    .bind(token0.as_slice())
    .bind(token1.as_slice())
    .bind(fee)
    .bind(tick_spacing)
    .bind(created_block)
    .bind(sqlx::types::Json(risk_flags))
    .fetch_optional(pool)
    .await
    .context("could not capture new pool")?;
    Ok(inserted.is_some())
}

// ---------------------------------------------------------------------------
// Candle downsampler (#112)
// ---------------------------------------------------------------------------

// Roll finer candles up into a coarser table. open = earliest fine bucket's
// open, close = latest fine bucket's close, high/low widen, volumes and counts
// sum. Only *complete* coarse buckets are rolled (strictly before the current
// coarse interval), so a still-filling bucket is never frozen half-done. On
// conflict the coarse row is *recomputed* (set from EXCLUDED, not accumulated),
// so re-running over the same fine set is idempotent; once the fine rows are
// pruned, the coarse group is empty and the existing coarse row is left as-is.
fn roll_sql(src: &str, dst: &str, unit: &str) -> String {
    format!(
        "INSERT INTO {dst} (pool, bucket, open, high, low, close, volume0, volume1, trade_count)
         SELECT pool,
                date_trunc('{unit}', bucket) AS b,
                (array_agg(open ORDER BY bucket))[1]        AS open,
                max(high) AS high,
                min(low)  AS low,
                (array_agg(close ORDER BY bucket DESC))[1]  AS close,
                sum(volume0) AS volume0,
                sum(volume1) AS volume1,
                sum(trade_count) AS trade_count
           FROM {src}
          WHERE bucket < date_trunc('{unit}', now())
          GROUP BY pool, date_trunc('{unit}', bucket)
         ON CONFLICT (pool, bucket) DO UPDATE SET
             open        = EXCLUDED.open,
             high        = EXCLUDED.high,
             low         = EXCLUDED.low,
             close       = EXCLUDED.close,
             volume0     = EXCLUDED.volume0,
             volume1     = EXCLUDED.volume1,
             trade_count = EXCLUDED.trade_count"
    )
}

/// Roll 1m candles into 1h and 1h into 1d. Idempotent; only complete buckets.
pub async fn downsample(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(&roll_sql("ohlcv_1m", "ohlcv_1h", "hour"))
        .execute(pool)
        .await
        .context("could not roll 1m into 1h")?;
    sqlx::query(&roll_sql("ohlcv_1h", "ohlcv_1d", "day"))
        .execute(pool)
        .await
        .context("could not roll 1h into 1d")?;
    Ok(())
}
