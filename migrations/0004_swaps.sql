-- Raw Uniswap V3 Swap events. The single table that drives the disk budget.
--
-- PARTITIONING
-- ------------
-- Partitioned by day on block_time. This exists for one reason: retention.
-- Dropping a day of history has to be `DROP TABLE swaps_20260722`, which is an
-- instant catalogue update, not `DELETE FROM swaps WHERE ...`, which rewrites
-- pages, bloats the heap and leaves the work to VACUUM.
--
-- PRIMARY KEY
-- -----------
-- The natural dedupe key for a chain event is (tx_hash, log_index) — globally
-- unique, and exactly what makes a replayed block a no-op via ON CONFLICT.
-- Postgres requires the partition key to be part of every unique constraint,
-- so the declared key is (block_time, tx_hash, log_index). This does not weaken
-- dedupe: a given log always arrives with the same block_time, so the conflict
-- still fires on replay.
--
-- The one case where block_time differs for the same (tx_hash, log_index) is a
-- reorg re-including the transaction in a different block. That is handled by
-- the reorg rewind deleting the orphaned rows before the new ones are written,
-- not by this constraint.
--
-- TYPES
-- -----
-- Amounts are NUMERIC. Uniswap deals in int256/uint160, which overflows BIGINT,
-- and money is never floating point.
--
-- INDEX PLAN (deliberately deferred)
-- ----------------------------------
-- These are NOT created here. Building them before the historical backfill
-- would mean maintaining a b-tree on every one of millions of bulk inserts;
-- creating them afterwards is several times faster and yields a denser tree.
-- They land in a later migration once M3 backfill completes:
--
--     CREATE INDEX swaps_pool_block_idx   ON swaps (pool, block_number DESC);
--     CREATE INDEX swaps_sender_block_idx ON swaps (sender, block_number DESC);
--     CREATE INDEX swaps_block_idx        ON swaps (block_number);
--
-- On a partitioned parent these become partitioned indexes, so each new day
-- partition inherits them automatically.

CREATE TABLE swaps (
    block_time      TIMESTAMPTZ NOT NULL,
    tx_hash         BYTEA       NOT NULL,
    log_index       INTEGER     NOT NULL,

    block_number    BIGINT      NOT NULL,
    pool            BYTEA       NOT NULL,
    sender          BYTEA       NOT NULL,
    recipient       BYTEA       NOT NULL,

    -- Signed: negative is the token leaving the pool.
    amount0         NUMERIC     NOT NULL,
    amount1         NUMERIC     NOT NULL,

    sqrt_price_x96  NUMERIC     NOT NULL,
    liquidity       NUMERIC     NOT NULL,
    tick            INTEGER     NOT NULL,

    PRIMARY KEY (block_time, tx_hash, log_index)
) PARTITION BY RANGE (block_time);

-- No DEFAULT partition on purpose. A default partition would silently swallow
-- rows whose day has no partition, hiding the fact that the partition
-- maintenance job stopped running — and then block the creation of that day's
-- real partition later. A loud insert failure is the better outcome.

COMMENT ON TABLE  swaps IS 'Raw Swap events, day-partitioned, rolling retention window.';
COMMENT ON COLUMN swaps.amount0 IS 'Signed token0 delta from the pool perspective. NUMERIC because int256 overflows BIGINT.';
