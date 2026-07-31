-- The smart-money watchlist: the top wallets by realised profit.
--
-- A materialised view, not a live query, for two reasons: it is read-hot (the
-- API and the alert engine both hit it), and the top-100 set is a *thing the
-- system commits to* — the wallets the M8 alerter subscribes to. Materialising
-- it makes "who is on the watchlist" an explicit, refreshable snapshot rather
-- than a query that quietly shifts under the alerter's feet between reads.
--
-- Wash wallets are excluded here (not just in the query) so the snapshot is the
-- watchlist, full stop. The unique index on wallet lets a later REFRESH run
-- CONCURRENTLY if the refresh ever starts to matter; today the table is 100 rows
-- and a plain REFRESH is instant.
--
-- WITH NO DATA: the view is empty until the first refresh, so the migration does
-- not depend on wallet_stats being populated.

CREATE MATERIALIZED VIEW leaderboard AS
    SELECT wallet,
           realized_pnl_usd,
           trades,
           wins,
           volume_usd
      FROM wallet_stats
     WHERE excluded = FALSE
     ORDER BY realized_pnl_usd DESC
     LIMIT 100
    WITH NO DATA;

CREATE UNIQUE INDEX leaderboard_wallet_idx ON leaderboard (wallet);

COMMENT ON MATERIALIZED VIEW leaderboard IS 'Top-100 wallets by realised PnL, wash-excluded. The persisted smart-money watchlist; refresh to update.';
