-- Price candles, three resolutions.
--
-- Computed on write from the same transaction that inserts the swaps, so a
-- replayed block produces a zero delta and the candles stay correct without a
-- separate reconciliation job.
--
-- Three tables rather than one with a `resolution` column: each has its own
-- retention (1m for days, 1h for months, 1d forever) and partition-free DELETE
-- of one resolution should never scan the others. They are also written at
-- different times — 1m on every swap, the coarser ones by rollup.
--
-- Retention chain (M8): 1m keeps ~7d -> rolled up into 1h, which keeps ~90d ->
-- rolled up into 1d, which is small enough to keep forever. The candle tables
-- are what survive after the raw swaps behind them are dropped.

CREATE TABLE ohlcv_1m (
    pool         BYTEA       NOT NULL,
    bucket       TIMESTAMPTZ NOT NULL,   -- start of the interval, UTC-aligned

    open         NUMERIC NOT NULL,
    high         NUMERIC NOT NULL,
    low          NUMERIC NOT NULL,
    close        NUMERIC NOT NULL,

    volume0      NUMERIC NOT NULL,
    volume1      NUMERIC NOT NULL,
    trade_count  INTEGER NOT NULL,

    PRIMARY KEY (pool, bucket)
);

CREATE TABLE ohlcv_1h (LIKE ohlcv_1m INCLUDING ALL);
CREATE TABLE ohlcv_1d (LIKE ohlcv_1m INCLUDING ALL);

-- No secondary index here on purpose. The chart query is "latest N buckets for
-- one pool", i.e. ORDER BY bucket DESC with pool fixed, and the primary key on
-- (pool, bucket) already serves it — Postgres scans a b-tree backwards just as
-- cheaply as forwards. A separate (pool, bucket DESC) index would be a second
-- copy of the same tree, paid for on every write.

COMMENT ON TABLE  ohlcv_1m IS 'One-minute candles. Base resolution, written on every swap, kept ~7 days.';
COMMENT ON COLUMN ohlcv_1m.bucket IS 'Interval start, truncated UTC. A swap belongs to exactly one bucket per resolution.';
