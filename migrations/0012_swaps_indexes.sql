-- Secondary indexes on swaps, deferred from 0004 until after the M3 backfill so
-- the bulk load did not pay to maintain a b-tree on every insert.
--
-- The read API paginates swaps for one pool newest-first, by keyset on
-- (block_number, log_index). This composite, descending on both, serves that
-- exactly: the WHERE narrows to the pool, and the ORDER BY ... DESC LIMIT walks
-- the tree backwards with no sort. It is the index the p99 target rests on.
--
-- On the partitioned parent this becomes a partitioned index, so every existing
-- and future day partition inherits it automatically.

CREATE INDEX IF NOT EXISTS swaps_pool_keyset_idx
    ON swaps (pool, block_number DESC, log_index DESC);

-- "Swaps by wallet" (recipient) history, same keyset shape.
CREATE INDEX IF NOT EXISTS swaps_recipient_keyset_idx
    ON swaps (recipient, block_number DESC, log_index DESC);
