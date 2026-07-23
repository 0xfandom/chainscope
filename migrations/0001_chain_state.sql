-- Ingestion bookkeeping. Exactly one row, forever.
--
-- Why a single row instead of a cursors table keyed by name: every write
-- transaction touches this row, and a single row means the cursor update is a
-- single UPDATE with no key lookup and no chance of two rows disagreeing about
-- what has been processed.
--
-- Finality lives here as ONE NUMBER, not as a per-row flag on swaps/blocks.
-- Finality is monotonic: once block N is final, every block below it is final
-- forever. So "is this row final?" is the comparison
--     row.block_number <= chain_state.finalized_height
-- A per-row flag would mean rewriting thousands of rows every twelve seconds
-- to record a fact that is one integer away.

CREATE TABLE chain_state (
    id                SMALLINT PRIMARY KEY DEFAULT 1,

    -- Head of the live pipeline: highest block whose events are written.
    live_cursor       BIGINT,

    -- Contiguous done-prefix of the historical backfill. Everything at or
    -- below this is complete; nothing above it is assumed complete.
    backfill_cursor   BIGINT,

    -- Highest block considered irreversible (head_height - finality_depth).
    finalized_height  BIGINT,

    -- Chain tip as last observed. Lag = head_height - live_cursor.
    head_height       BIGINT,

    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The whole point: makes a second row impossible at the database level.
    CONSTRAINT chain_state_singleton CHECK (id = 1)
);

-- Cursors start NULL, meaning "nothing processed yet" — distinct from 0, which
-- would claim the genesis block was already handled.
INSERT INTO chain_state (id) VALUES (1);

COMMENT ON TABLE  chain_state IS 'Singleton row: ingestion cursors and finality height.';
COMMENT ON COLUMN chain_state.live_cursor      IS 'Highest block written by the live pipeline; NULL = not started.';
COMMENT ON COLUMN chain_state.backfill_cursor  IS 'Contiguous done-prefix of the backfill; NULL = not started.';
COMMENT ON COLUMN chain_state.finalized_height IS 'Rows at or below this block are irreversible. Monotonic.';
