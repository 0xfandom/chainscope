-- Alert dedupe ledger.
--
-- The pipeline is at-least-once: a crash between "sent the Telegram message"
-- and "committed the cursor" means the block is processed again on restart.
-- Without this table that replay would re-send the alert.
--
-- alert_key is derived from the event that caused the alert, not from the time
-- it was sent — 'watchlist:<tx_hash>:<log_index>'. Deterministic, so the same
-- event always produces the same key, and the INSERT ... ON CONFLICT DO NOTHING
-- decides whether this is the first time. Sending happens only if the insert
-- reported a new row.

CREATE TABLE alerts_sent (
    alert_key  TEXT PRIMARY KEY,
    sent_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Pruned once the causing block is final and can no longer be replayed.
CREATE INDEX alerts_sent_sent_at_idx ON alerts_sent (sent_at);

COMMENT ON TABLE  alerts_sent IS 'Idempotency ledger: one row per alert already delivered.';
COMMENT ON COLUMN alerts_sent.alert_key IS 'Deterministic key like watchlist:<tx_hash>:<log_index>. Same event, same key, every replay.';
