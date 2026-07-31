//! Detectors: read recent activity for the watchlist and turn it into alerts.

use bigdecimal::{BigDecimal, FromPrimitive};
use chainscope_core::types::{Address20, SwapRow};
use chainscope_indexer::pnl::{classify, Classified, Numeraire, PoolMeta};
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::engine::Alerter;

fn addr(bytes: Vec<u8>) -> Address20 {
    bytes.try_into().unwrap_or([0; 20])
}

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

/// The top-`n` wallets on the watchlist, from the leaderboard snapshot. An
/// unpopulated matview (before the first maintenance refresh) is an empty
/// watchlist, not an error.
pub async fn watchlist(pool: &PgPool, n: i64) -> anyhow::Result<Vec<Address20>> {
    let result = sqlx::query("SELECT wallet FROM leaderboard ORDER BY realized_pnl_usd DESC LIMIT $1")
        .bind(n)
        .fetch_all(pool)
        .await;
    let rows = match result {
        Ok(rows) => rows,
        Err(e) if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("55000") => {
            return Ok(Vec::new());
        }
        Err(e) => return Err(e.into()),
    };
    Ok(rows.into_iter().map(|r| addr(r.get("wallet"))).collect())
}

/// Resolve the WETH/USD reference from our own candles, if the numeraire names a
/// WETH price pool.
async fn weth_usd(pool: &PgPool, numeraire: &Numeraire) -> anyhow::Result<Option<BigDecimal>> {
    let Some(p) = numeraire.weth_price_pool else {
        return Ok(None);
    };
    Ok(
        sqlx::query_scalar("SELECT close FROM ohlcv_1m WHERE pool = $1 ORDER BY bucket DESC LIMIT 1")
            .bind(p.as_slice())
            .fetch_optional(pool)
            .await?,
    )
}

/// Watchlist-move detector: a watched wallet's swap valued at or above the
/// threshold fires one alert. Rescans a bounded recent window each poll; the
/// `alerts_sent` dedupe absorbs the overlap.
pub async fn watchlist_moves(a: &Alerter) -> anyhow::Result<usize> {
    let watch = watchlist(&a.pool, a.config.watchlist_size).await?;
    if watch.is_empty() {
        return Ok(0);
    }
    let head: Option<i64> = sqlx::query_scalar("SELECT live_cursor FROM chain_state WHERE id = 1")
        .fetch_one(&a.pool)
        .await?;
    let floor = head.unwrap_or(0) - a.config.move_lookback_blocks;

    let weth = weth_usd(&a.pool, &a.config.numeraire).await?;
    let price = a.config.numeraire.pricer(weth);
    let threshold = BigDecimal::from_f64(a.config.move_threshold_usd)
        .ok_or_else(|| anyhow::anyhow!("bad move threshold"))?;

    let watch_bytes: Vec<Vec<u8>> = watch.iter().map(|w| w.to_vec()).collect();
    let rows = sqlx::query(
        "SELECT s.tx_hash, s.log_index, s.recipient, s.pool,
                s.amount0::text AS a0, s.amount1::text AS a1,
                s.sqrt_price_x96::text AS sp, s.tick,
                p.token0, p.token1, p.token0_decimals AS d0, p.token1_decimals AS d1
           FROM swaps s JOIN pools p ON p.address = s.pool
          WHERE s.recipient = ANY($1) AND s.block_number > $2",
    )
    .bind(&watch_bytes)
    .bind(floor)
    .fetch_all(&a.pool)
    .await?;

    let mut sent = 0;
    for r in rows {
        let (Some(d0), Some(d1)) = (r.get::<Option<i16>, _>("d0"), r.get::<Option<i16>, _>("d1"))
        else {
            continue; // unknown decimals: cannot price
        };
        let meta = PoolMeta {
            token0: addr(r.get("token0")),
            token1: addr(r.get("token1")),
            token0_decimals: d0 as u8,
            token1_decimals: d1 as u8,
        };
        let swap = SwapRow {
            tx_hash: r.get::<Vec<u8>, _>("tx_hash").try_into().unwrap_or([0; 32]),
            log_index: r.get::<i32, _>("log_index") as u32,
            pool: addr(r.get("pool")),
            sender: [0; 20],
            recipient: addr(r.get("recipient")),
            amount0: parse_bd(r.get("a0")),
            amount1: parse_bd(r.get("a1")),
            sqrt_price_x96: parse_bd(r.get("sp")),
            liquidity: BigDecimal::from(0),
            tick: r.get("tick"),
        };

        if let Classified::Priced(t) = classify(&swap, &meta, &price) {
            if t.value_usd >= threshold {
                let key = format!("move:{}:{}", hex0x(&swap.tx_hash), swap.log_index);
                let text = format!(
                    "\u{1f4b0} watchlist move\nwallet {}\nbought {} {}\nfor ${}",
                    hex0x(&t.wallet),
                    t.bought_qty.round(4),
                    hex0x(&t.bought),
                    t.value_usd.round(2),
                );
                if a.dispatch(&key, &text).await? {
                    sent += 1;
                }
            }
        }
    }
    Ok(sent)
}

fn parse_bd(s: String) -> BigDecimal {
    use std::str::FromStr;
    BigDecimal::from_str(&s).unwrap_or_else(|_| BigDecimal::from(0))
}

/// Cluster-buy detector: when `cluster_size` or more distinct watched wallets buy
/// the same token within a window, fire one alert.
///
/// Recency is bounded by a block window (cheap); the "within the window" test is
/// the span of buy *times* per token. A growing cluster does not re-alert: the
/// dedupe key buckets on the cluster's first-buy time, which is stable as more
/// wallets join. Simple form — a tight sub-cluster hidden inside a wider spread
/// of buys for the same token is missed (the span check sees the whole spread).
pub async fn cluster_buys(a: &Alerter) -> anyhow::Result<usize> {
    let watch = watchlist(&a.pool, a.config.watchlist_size).await?;
    if (watch.len() as i64) < a.config.cluster_size {
        return Ok(0);
    }
    let head: Option<i64> = sqlx::query_scalar("SELECT live_cursor FROM chain_state WHERE id = 1")
        .fetch_one(&a.pool)
        .await?;
    // ~12s/block; scan a block window a little wider than the time window.
    let floor = head.unwrap_or(0) - (a.config.cluster_window_secs / 12 + 100);
    let watch_bytes: Vec<Vec<u8>> = watch.iter().map(|w| w.to_vec()).collect();

    let rows = sqlx::query(
        "WITH buys AS (
             SELECT s.recipient AS wallet,
                    CASE WHEN s.amount0 < 0 THEN p.token0 ELSE p.token1 END AS token,
                    s.block_time
               FROM swaps s
               JOIN pools p ON p.address = s.pool
              WHERE s.recipient = ANY($1) AND s.block_number > $2
         )
         SELECT token,
                count(DISTINCT wallet) AS n,
                extract(epoch FROM min(block_time))::bigint AS first_ts
           FROM buys
          GROUP BY token
         HAVING count(DISTINCT wallet) >= $3
            AND max(block_time) - min(block_time) <= make_interval(secs => $4)",
    )
    .bind(&watch_bytes)
    .bind(floor)
    .bind(a.config.cluster_size)
    .bind(a.config.cluster_window_secs as f64)
    .fetch_all(&a.pool)
    .await?;

    let mut sent = 0;
    for r in rows {
        let token: Vec<u8> = r.get("token");
        let n: i64 = r.get("n");
        let first_ts: i64 = r.get("first_ts");
        // Bucket on the first-buy time so a cluster gaining members keeps one key.
        let bucket = first_ts / a.config.cluster_window_secs.max(1);
        let key = format!("cluster:{}:{}", hex0x(&token), bucket);
        let text = format!(
            "\u{1f41d} cluster buy\n{n} watched wallets bought {}\nwithin {}s",
            hex0x(&token),
            a.config.cluster_window_secs,
        );
        if a.dispatch(&key, &text).await? {
            sent += 1;
        }
    }
    Ok(sent)
}

/// New-pool detector: freshly discovered pools (from the sniffer) emit a risk
/// scorecard, once each.
pub async fn new_pools(a: &Alerter) -> anyhow::Result<usize> {
    let rows = sqlx::query(
        "SELECT address, token0, token1, fee, risk_flags::text AS risk
           FROM pools WHERE is_indexed = false
          ORDER BY discovered_at DESC LIMIT 200",
    )
    .fetch_all(&a.pool)
    .await?;

    let mut sent = 0;
    for r in rows {
        let address: Vec<u8> = r.get("address");
        let key = format!("newpool:{}", hex0x(&address));
        let text = format!(
            "\u{1f195} new pool\npool {}\ntoken0 {}\ntoken1 {}\nfee {}\nrisk {}",
            hex0x(&address),
            hex0x(&r.get::<Vec<u8>, _>("token0")),
            hex0x(&r.get::<Vec<u8>, _>("token1")),
            r.get::<i32, _>("fee"),
            r.get::<String, _>("risk"),
        );
        if a.dispatch(&key, &text).await? {
            sent += 1;
        }
    }
    Ok(sent)
}
