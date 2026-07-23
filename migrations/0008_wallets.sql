-- The smart-money brain's permanent state.
--
-- These two tables are the actual product. Raw swaps are fuel that gets burned
-- into them and then dropped; these stay forever and remain small — one row per
-- wallet, one per wallet-token pair.
--
-- Both are updated inside the SAME transaction that writes the swap rows. That
-- coupling is what makes a reorg rewind correct: deleting the orphaned swaps
-- and reversing their effect on PnL happen atomically, so the brain can never
-- be left believing in a block that no longer exists.

CREATE TABLE wallet_positions (
    wallet         BYTEA NOT NULL,
    token          BYTEA NOT NULL,

    qty_held       NUMERIC NOT NULL DEFAULT 0,
    cost_basis_usd NUMERIC NOT NULL DEFAULT 0,

    -- FIFO purchase lots: [{"qty": "...", "price_usd": "...", "block": N}, ...]
    -- JSONB rather than a lots table because lots are only ever read and
    -- rewritten as a whole list for one wallet-token pair, never queried across
    -- wallets. A separate table would add a join and a row per buy for no gain.
    lots           JSONB NOT NULL DEFAULT '[]'::jsonb,

    updated_block  BIGINT,

    PRIMARY KEY (wallet, token)
);

CREATE TABLE wallet_stats (
    wallet            BYTEA PRIMARY KEY,

    realized_pnl_usd  NUMERIC NOT NULL DEFAULT 0,
    trades            INTEGER NOT NULL DEFAULT 0,
    wins              INTEGER NOT NULL DEFAULT 0,
    volume_usd        NUMERIC NOT NULL DEFAULT 0,
    avg_size_usd      NUMERIC NOT NULL DEFAULT 0,

    last_active_block BIGINT,

    -- Set by the wash-trade heuristics. Excluded from the leaderboard rather
    -- than deleted, so the exclusion stays auditable.
    excluded          BOOLEAN NOT NULL DEFAULT FALSE
);

-- win_rate is wins/trades — derived, so it is not stored. Storing it would mean
-- two columns that can disagree.

-- The leaderboard query: top wallets by realised profit. One row per wallet
-- means this index stays small however much history is processed.
CREATE INDEX wallet_stats_pnl_idx
    ON wallet_stats (realized_pnl_usd DESC)
    WHERE excluded = FALSE;

COMMENT ON TABLE  wallet_positions IS 'Per wallet-token holdings and open FIFO cost-basis lots.';
COMMENT ON TABLE  wallet_stats     IS 'Per-wallet realised performance. Source of the leaderboard and watchlist.';
COMMENT ON COLUMN wallet_positions.lots IS 'FIFO buy lots, oldest first. A sell consumes from the front.';
