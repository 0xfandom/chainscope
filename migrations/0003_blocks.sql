-- Recent block headers, kept only deep enough to detect and unwind a reorg.
--
-- This is not an archive. The only question it answers is:
--     "does the chain I recorded still match the chain the node reports?"
-- Walking back from the tip comparing parent_hash finds the fork point; rows
-- older than the finality window are pruned because a finalised block can
-- never be the answer to that question.
--
-- No status column. Finality is chain_state.finalized_height (see 0001).

CREATE TABLE blocks (
    number       BIGINT PRIMARY KEY,
    block_hash   BYTEA       NOT NULL,
    parent_hash  BYTEA       NOT NULL,
    block_time   TIMESTAMPTZ NOT NULL
);

-- The reorg walk goes backwards from the tip, so descending order is the
-- access pattern. The PK already serves it; this comment marks the intent.

COMMENT ON TABLE  blocks IS 'Rolling window of recent headers, used only for reorg detection.';
COMMENT ON COLUMN blocks.parent_hash IS 'Link that makes fork detection possible: our block N parent must equal our block N-1 hash.';
