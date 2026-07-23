-- One row per market we know about. Permanent and tiny (hundreds of rows).
--
-- Two populations live here:
--   1. pools we deliberately index (top-N by volume)
--   2. pools the sniffer discovered from factory PoolCreated events, which may
--      never be promoted to indexed
-- is_indexed separates them, so discovery never silently widens ingestion.

CREATE TABLE pools (
    address            BYTEA PRIMARY KEY,          -- 20 bytes, raw, not hex text

    token0             BYTEA  NOT NULL,
    token1             BYTEA  NOT NULL,
    fee                INTEGER NOT NULL,           -- hundredths of a bip: 500, 3000, 10000
    tick_spacing       INTEGER NOT NULL,

    -- Denormalised token metadata. Copied in once at discovery so that reading
    -- a swap never needs a join to a tokens table for the common case.
    token0_symbol      TEXT,
    token0_decimals    SMALLINT,
    token1_symbol      TEXT,
    token1_decimals    SMALLINT,

    created_block      BIGINT,                     -- block of PoolCreated
    discovered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Sniffer scorecard fields, filled in M7.
    first_liquidity_usd NUMERIC,
    risk_flags          JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- False = discovered but not ingested. Promotion to true is a deliberate act.
    is_indexed         BOOLEAN NOT NULL DEFAULT FALSE
);

-- Small table, but the sniffer scans "recently discovered, not yet indexed".
CREATE INDEX pools_discovered_at_idx ON pools (discovered_at DESC);

COMMENT ON TABLE  pools IS 'Every known Uniswap V3 pool; is_indexed marks the ones we ingest.';
COMMENT ON COLUMN pools.address IS 'Raw 20-byte address. All addresses in this schema are BYTEA, never hex text.';
