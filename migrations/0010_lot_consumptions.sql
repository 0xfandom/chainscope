-- Lot-consumption ledger: the reversible record behind FIFO PnL.
--
-- WHY THIS TABLE EXISTS
-- ---------------------
-- `wallet_positions.lots` (0008) holds only the *current* open lots. The moment
-- a sell consumes lots from the FIFO front, the consumed lots are gone from that
-- snapshot — the JSONB carries no memory of what was drawn down.
--
-- A reorg above the fork point has to un-consume *exactly* what the orphaned
-- sells took. FIFO is path-dependent, so — unlike a candle bucket (M4 #47) — a
-- wallet's lots cannot be recomputed from the recent survivors: the acquiring
-- swaps may sit below the retention window and already be dropped.
--
-- Realised PnL, though, is a sum of recorded contributions. If every drawdown is
-- written down with enough to rebuild the lot it came from, a reorg becomes an
-- exact inverse: restore what each above-fork sell consumed, drop what each
-- above-fork buy opened. That is the whole job of this ledger. 0008's comment
-- ("reverse their effect atomically") assumed this record; here it is.
--
-- GRANULARITY
-- -----------
-- One row per (sell, consumed lot). A single sell can draw down several lots, so
-- the key carries a `consume_seq` discriminator. Each row stores the consumed
-- lot's descriptor (`qty_consumed`, `lot_unit_cost_usd`, `lot_acquired_block`) —
-- enough to prepend the exact lot back into the JSONB queue in FIFO order — plus
-- the realised numbers to back out of `wallet_stats`.
--
-- ACCESS PATTERN
-- --------------
-- Append on every sell (inside the writer tx, same commit as the swap + stats).
-- Range-delete on reorg: `WHERE sell_block > fork`. Prunable below the finality
-- line, where a block can no longer be reorged — same lifecycle as alerts_sent.
--
-- This does not overturn 0008's "lots as JSONB, not a table" call: open positions
-- are still read and rewritten as a whole list per wallet-token. This ledger is a
-- different concern with a different access pattern, so it earns its own table.

CREATE TABLE lot_consumptions (
    sell_tx             BYTEA   NOT NULL,
    sell_log            INTEGER NOT NULL,
    consume_seq         INTEGER NOT NULL,   -- 0,1,2… for the lots one sell drew down

    wallet              BYTEA   NOT NULL,
    token               BYTEA   NOT NULL,

    -- The consumed lot, enough to reconstruct it on reversal.
    qty_consumed        NUMERIC NOT NULL,
    lot_unit_cost_usd   NUMERIC NOT NULL,
    lot_acquired_block  BIGINT  NOT NULL,

    -- The realised outcome of this drawdown, to back out of wallet_stats.
    proceeds_usd        NUMERIC NOT NULL,
    realized_pnl_usd    NUMERIC NOT NULL,

    sell_block          BIGINT  NOT NULL,

    -- Natural key. A replayed sell re-derives the same (tx, log, seq), so
    -- ON CONFLICT DO NOTHING makes the fold idempotent, exactly like the raw
    -- swap insert it rides alongside.
    PRIMARY KEY (sell_tx, sell_log, consume_seq)
);

-- The reorg reversal is `... WHERE sell_block > fork`, so it needs this index to
-- avoid scanning the whole ledger. Also the axis the finality pruner deletes on.
CREATE INDEX lot_consumptions_sell_block_idx ON lot_consumptions (sell_block);

COMMENT ON TABLE  lot_consumptions IS 'Reversible per-drawdown ledger: one row per (sell, consumed FIFO lot). Enables exact reorg reversal of realised PnL.';
COMMENT ON COLUMN lot_consumptions.consume_seq        IS 'Discriminates the multiple lots a single sell draws down.';
COMMENT ON COLUMN lot_consumptions.lot_unit_cost_usd  IS 'Unit cost of the consumed lot, so the lot can be rebuilt on reversal.';
COMMENT ON COLUMN lot_consumptions.realized_pnl_usd   IS 'proceeds_usd for this drawdown minus qty_consumed * lot_unit_cost_usd.';
