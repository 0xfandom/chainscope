-- Liquidity lifecycle events: Mint, Burn, Collect.
--
-- Same shape and same reasoning as swaps (see 0004): day-partitioned on
-- block_time for DROP-based retention, natural key widened with the partition
-- key, NUMERIC amounts.
--
-- Three event types share one table because they carry the same fields and are
-- always queried together as "what happened to this position". A `kind` column
-- with a CHECK is cheaper than three near-identical tables and three unions.
--
-- INDEX PLAN (deferred until after backfill, same reasoning as swaps):
--     CREATE INDEX liq_events_pool_block_idx  ON liq_events (pool, block_number DESC);
--     CREATE INDEX liq_events_owner_block_idx ON liq_events (owner, block_number DESC);

CREATE TABLE liq_events (
    block_time    TIMESTAMPTZ NOT NULL,
    tx_hash       BYTEA       NOT NULL,
    log_index     INTEGER     NOT NULL,

    block_number  BIGINT      NOT NULL,
    pool          BYTEA       NOT NULL,
    kind          TEXT        NOT NULL,
    owner         BYTEA       NOT NULL,

    tick_lower    INTEGER     NOT NULL,
    tick_upper    INTEGER     NOT NULL,

    -- Liquidity delta for mint/burn; zero for collect, which moves only fees.
    amount        NUMERIC     NOT NULL,
    amount0       NUMERIC     NOT NULL,
    amount1       NUMERIC     NOT NULL,

    PRIMARY KEY (block_time, tx_hash, log_index),
    CONSTRAINT liq_events_kind_check CHECK (kind IN ('mint', 'burn', 'collect'))
) PARTITION BY RANGE (block_time);

COMMENT ON TABLE  liq_events IS 'Raw Mint/Burn/Collect events, day-partitioned, rolling retention window.';
COMMENT ON COLUMN liq_events.amount IS 'Liquidity delta. Always zero for collect, which transfers accrued fees only.';
