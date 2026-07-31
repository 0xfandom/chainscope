//! The API's read-only data access.
//!
//! Its own queries against the shared schema, so the API stays independent of the
//! indexer binary (and its Kafka client). Read-only in intent — nothing here
//! writes.

use serde::Serialize;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use crate::dto::{Page, PoolDto, SwapDto};
use crate::error::ApiError;
use crate::pagination::Keyset;
use crate::util::hex0x;

/// Open the read pool.
pub async fn connect(url: &str, max_connections: u32) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await?;
    Ok(pool)
}

/// A cheap liveness probe.
pub async fn ping(pool: &PgPool) -> Result<(), ApiError> {
    sqlx::query("SELECT 1")
        .execute(pool)
        .await
        .map_err(|_| ApiError::Unavailable)?;
    Ok(())
}

/// Ingestion progress — the operational heartbeat.
#[derive(Debug, Serialize)]
pub struct Status {
    pub head_height: Option<i64>,
    pub finalized_height: Option<i64>,
    pub live_cursor: Option<i64>,
    pub backfill_cursor: Option<i64>,
    /// How far the live pipeline is behind the head, when both are known.
    pub lag: Option<i64>,
}

/// Read the singleton `chain_state` row.
pub async fn status(pool: &PgPool) -> Result<Status, ApiError> {
    let row = sqlx::query(
        "SELECT head_height, finalized_height, live_cursor, backfill_cursor
           FROM chain_state WHERE id = 1",
    )
    .fetch_one(pool)
    .await?;

    let head_height: Option<i64> = row.get("head_height");
    let live_cursor: Option<i64> = row.get("live_cursor");
    let lag = match (head_height, live_cursor) {
        (Some(h), Some(c)) => Some((h - c).max(0)),
        _ => None,
    };

    Ok(Status {
        head_height,
        finalized_height: row.get("finalized_height"),
        live_cursor,
        backfill_cursor: row.get("backfill_cursor"),
        lag,
    })
}

// ---------------------------------------------------------------------------
// Pools and swaps (#86)
// ---------------------------------------------------------------------------

fn pool_from_row(r: &sqlx::postgres::PgRow) -> PoolDto {
    PoolDto {
        address: hex0x(&r.get::<Vec<u8>, _>("address")),
        token0: hex0x(&r.get::<Vec<u8>, _>("token0")),
        token1: hex0x(&r.get::<Vec<u8>, _>("token1")),
        fee: r.get("fee"),
        tick_spacing: r.get("tick_spacing"),
        token0_symbol: r.get("token0_symbol"),
        token0_decimals: r.get("token0_decimals"),
        token1_symbol: r.get("token1_symbol"),
        token1_decimals: r.get("token1_decimals"),
        is_indexed: r.get("is_indexed"),
    }
}

const POOL_COLS: &str = "address, token0, token1, fee, tick_spacing, \
     token0_symbol, token0_decimals, token1_symbol, token1_decimals, is_indexed";

/// The indexed pools.
pub async fn list_pools(pool: &PgPool) -> Result<Vec<PoolDto>, ApiError> {
    let rows = sqlx::query(&format!(
        "SELECT {POOL_COLS} FROM pools WHERE is_indexed = true ORDER BY address"
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(pool_from_row).collect())
}

/// One pool by address, indexed or not.
pub async fn get_pool(pool: &PgPool, address: &[u8; 20]) -> Result<Option<PoolDto>, ApiError> {
    let row = sqlx::query(&format!("SELECT {POOL_COLS} FROM pools WHERE address = $1"))
        .bind(address.as_slice())
        .fetch_optional(pool)
        .await?;
    Ok(row.as_ref().map(pool_from_row))
}

/// A keyset page of a pool's swaps, newest-first.
pub async fn swaps_page(
    pool: &PgPool,
    address: &[u8; 20],
    after: Option<Keyset>,
    limit: i64,
) -> Result<Page<SwapDto>, ApiError> {
    // Fetch one extra to learn whether another page exists.
    let rows = sqlx::query(
        "SELECT block_number, log_index,
                extract(epoch FROM block_time)::bigint AS ts,
                tx_hash, pool, sender, recipient,
                amount0::text AS amount0, amount1::text AS amount1,
                sqrt_price_x96::text AS sqrt_price_x96, tick
           FROM swaps
          WHERE pool = $1
            AND ($2::bigint IS NULL
                 OR block_number < $2
                 OR (block_number = $2 AND log_index < $3))
          ORDER BY block_number DESC, log_index DESC
          LIMIT $4",
    )
    .bind(address.as_slice())
    .bind(after.map(|k| k.block_number))
    .bind(after.map(|k| k.log_index))
    .bind(limit + 1)
    .fetch_all(pool)
    .await?;

    let has_more = rows.len() as i64 > limit;
    let kept = &rows[..rows.len().min(limit as usize)];

    let items: Vec<SwapDto> = kept
        .iter()
        .map(|r| SwapDto {
            block_number: r.get("block_number"),
            log_index: r.get("log_index"),
            block_time: r.get("ts"),
            tx_hash: hex0x(&r.get::<Vec<u8>, _>("tx_hash")),
            pool: hex0x(&r.get::<Vec<u8>, _>("pool")),
            sender: hex0x(&r.get::<Vec<u8>, _>("sender")),
            recipient: hex0x(&r.get::<Vec<u8>, _>("recipient")),
            amount0: r.get("amount0"),
            amount1: r.get("amount1"),
            sqrt_price_x96: r.get("sqrt_price_x96"),
            tick: r.get("tick"),
        })
        .collect();

    let next_cursor = if has_more {
        items.last().map(|s| {
            Keyset {
                block_number: s.block_number,
                log_index: s.log_index,
            }
            .encode()
        })
    } else {
        None
    };

    Ok(Page { items, next_cursor })
}

// ---------------------------------------------------------------------------
// OHLCV candles (#87)
// ---------------------------------------------------------------------------

/// Map a resolution string to its candle table. Fixed set, so formatting the
/// name into the query is safe (no user string reaches the SQL).
fn candle_table(resolution: &str) -> Result<&'static str, ApiError> {
    match resolution {
        "1m" => Ok("ohlcv_1m"),
        "1h" => Ok("ohlcv_1h"),
        "1d" => Ok("ohlcv_1d"),
        _ => Err(ApiError::bad_request("resolution must be one of 1m, 1h, 1d")),
    }
}

/// A keyset page of a pool's candles at one resolution, newest-first.
pub async fn candles_page(
    pool: &PgPool,
    address: &[u8; 20],
    resolution: &str,
    before: Option<i64>,
    limit: i64,
) -> Result<Page<crate::dto::CandleDto>, ApiError> {
    let table = candle_table(resolution)?;
    let rows = sqlx::query(&format!(
        "SELECT extract(epoch FROM bucket)::bigint AS bucket,
                open::text AS open, high::text AS high, low::text AS low,
                close::text AS close, volume0::text AS volume0,
                volume1::text AS volume1, trade_count
           FROM {table}
          WHERE pool = $1
            AND ($2::bigint IS NULL OR bucket < to_timestamp($2))
          ORDER BY bucket DESC
          LIMIT $3"
    ))
    .bind(address.as_slice())
    .bind(before)
    .bind(limit + 1)
    .fetch_all(pool)
    .await?;

    let has_more = rows.len() as i64 > limit;
    let kept = &rows[..rows.len().min(limit as usize)];
    let items: Vec<crate::dto::CandleDto> = kept
        .iter()
        .map(|r| crate::dto::CandleDto {
            bucket: r.get("bucket"),
            open: r.get("open"),
            high: r.get("high"),
            low: r.get("low"),
            close: r.get("close"),
            volume0: r.get("volume0"),
            volume1: r.get("volume1"),
            trade_count: r.get("trade_count"),
        })
        .collect();

    let next_cursor = if has_more {
        items.last().map(|c| crate::pagination::encode_bucket(c.bucket))
    } else {
        None
    };
    Ok(Page { items, next_cursor })
}

// ---------------------------------------------------------------------------
// Wallet scorecard and trade history (#88)
// ---------------------------------------------------------------------------

use crate::dto::{OpenPositionDto, RealizedTradeDto, ScorecardDto};

fn realized_from_row(r: &sqlx::postgres::PgRow) -> RealizedTradeDto {
    RealizedTradeDto {
        sell_block: r.get("sell_block"),
        consume_seq: r.get("consume_seq"),
        token: hex0x(&r.get::<Vec<u8>, _>("token")),
        qty_consumed: r.get("qty_consumed"),
        proceeds_usd: r.get("proceeds_usd"),
        realized_pnl_usd: r.get("realized_pnl_usd"),
    }
}

/// One wallet's scorecard, or `None` when it has no stats row.
pub async fn wallet_scorecard(
    pool: &PgPool,
    wallet: &[u8; 20],
) -> Result<Option<ScorecardDto>, ApiError> {
    let Some(s) = sqlx::query(
        "SELECT realized_pnl_usd::text AS realized, trades, wins, \
                volume_usd::text AS volume, avg_size_usd::text AS avg, excluded \
           FROM wallet_stats WHERE wallet = $1",
    )
    .bind(wallet.as_slice())
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };

    let open_positions = sqlx::query(
        "SELECT token, qty_held::text AS qty, cost_basis_usd::text AS cost \
           FROM wallet_positions WHERE wallet = $1 AND qty_held > 0 \
          ORDER BY cost_basis_usd DESC",
    )
    .bind(wallet.as_slice())
    .fetch_all(pool)
    .await?
    .iter()
    .map(|r| OpenPositionDto {
        token: hex0x(&r.get::<Vec<u8>, _>("token")),
        qty_held: r.get("qty"),
        cost_basis_usd: r.get("cost"),
    })
    .collect();

    let recent_realized = sqlx::query(
        "SELECT sell_block, consume_seq, token, qty_consumed::text AS qty_consumed, \
                proceeds_usd::text AS proceeds_usd, realized_pnl_usd::text AS realized_pnl_usd \
           FROM lot_consumptions WHERE wallet = $1 \
          ORDER BY sell_block DESC, consume_seq DESC LIMIT 10",
    )
    .bind(wallet.as_slice())
    .fetch_all(pool)
    .await?
    .iter()
    .map(realized_from_row)
    .collect();

    Ok(Some(ScorecardDto {
        wallet: hex0x(wallet.as_slice()),
        realized_pnl_usd: s.get("realized"),
        trades: s.get("trades"),
        wins: s.get("wins"),
        volume_usd: s.get("volume"),
        avg_size_usd: s.get("avg"),
        excluded: s.get("excluded"),
        open_positions,
        recent_realized,
    }))
}

/// A keyset page of a wallet's realised trades, newest-first.
pub async fn wallet_trades_page(
    pool: &PgPool,
    wallet: &[u8; 20],
    after: Option<Keyset>,
    limit: i64,
) -> Result<Page<RealizedTradeDto>, ApiError> {
    let rows = sqlx::query(
        "SELECT sell_block, consume_seq, token, qty_consumed::text AS qty_consumed, \
                proceeds_usd::text AS proceeds_usd, realized_pnl_usd::text AS realized_pnl_usd \
           FROM lot_consumptions \
          WHERE wallet = $1 \
            AND ($2::bigint IS NULL \
                 OR sell_block < $2 \
                 OR (sell_block = $2 AND consume_seq < $3)) \
          ORDER BY sell_block DESC, consume_seq DESC \
          LIMIT $4",
    )
    .bind(wallet.as_slice())
    .bind(after.map(|k| k.block_number))
    .bind(after.map(|k| k.log_index))
    .bind(limit + 1)
    .fetch_all(pool)
    .await?;

    let has_more = rows.len() as i64 > limit;
    let items: Vec<RealizedTradeDto> = rows[..rows.len().min(limit as usize)]
        .iter()
        .map(realized_from_row)
        .collect();
    let next_cursor = if has_more {
        items.last().map(|t| {
            Keyset {
                block_number: t.sell_block,
                log_index: t.consume_seq,
            }
            .encode()
        })
    } else {
        None
    };
    Ok(Page { items, next_cursor })
}

// ---------------------------------------------------------------------------
// Leaderboard and new pools (#89)
// ---------------------------------------------------------------------------

use crate::dto::{LeaderRowDto, NewPoolDto};

/// The watchlist: top wallets by realised PnL, wash-excluded, from the matview.
///
/// The materialised view ships `WITH NO DATA`, so before its first refresh a
/// SELECT raises `55000` (object not in prerequisite state). That is "no
/// watchlist yet", not an error to the client — return an empty list.
pub async fn leaderboard(pool: &PgPool, limit: i64) -> Result<Vec<LeaderRowDto>, ApiError> {
    let result = sqlx::query(
        "SELECT wallet, realized_pnl_usd::text AS pnl, trades, wins, volume_usd::text AS volume \
           FROM leaderboard ORDER BY realized_pnl_usd DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await;

    let rows = match result {
        Ok(rows) => rows,
        Err(e) if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("55000") => {
            return Ok(Vec::new());
        }
        Err(e) => return Err(e.into()),
    };

    Ok(rows
        .iter()
        .map(|r| LeaderRowDto {
            wallet: hex0x(&r.get::<Vec<u8>, _>("wallet")),
            realized_pnl_usd: r.get("pnl"),
            trades: r.get("trades"),
            wins: r.get("wins"),
            volume_usd: r.get("volume"),
        })
        .collect())
}

/// Recently discovered pools, newest-first, keyset-paginated on discovery time.
pub async fn new_pools_page(
    pool: &PgPool,
    before: Option<i64>,
    limit: i64,
) -> Result<Page<NewPoolDto>, ApiError> {
    let rows = sqlx::query(
        "SELECT address, token0, token1, fee, token0_symbol, token1_symbol, created_block, \
                extract(epoch FROM discovered_at)::bigint AS discovered_at, is_indexed \
           FROM pools \
          WHERE ($1::bigint IS NULL OR discovered_at < to_timestamp($1)) \
          ORDER BY discovered_at DESC \
          LIMIT $2",
    )
    .bind(before)
    .bind(limit + 1)
    .fetch_all(pool)
    .await?;

    let has_more = rows.len() as i64 > limit;
    let items: Vec<NewPoolDto> = rows[..rows.len().min(limit as usize)]
        .iter()
        .map(|r| NewPoolDto {
            address: hex0x(&r.get::<Vec<u8>, _>("address")),
            token0: hex0x(&r.get::<Vec<u8>, _>("token0")),
            token1: hex0x(&r.get::<Vec<u8>, _>("token1")),
            fee: r.get("fee"),
            token0_symbol: r.get("token0_symbol"),
            token1_symbol: r.get("token1_symbol"),
            created_block: r.get("created_block"),
            discovered_at: r.get("discovered_at"),
            is_indexed: r.get("is_indexed"),
        })
        .collect();
    let next_cursor = if has_more {
        items.last().map(|p| crate::pagination::encode_bucket(p.discovered_at))
    } else {
        None
    };
    Ok(Page { items, next_cursor })
}

// ---------------------------------------------------------------------------
// Disk footprint (for /metrics, #123)
// ---------------------------------------------------------------------------

/// On-disk size split into transient raw events and permanent aggregates.
#[derive(Debug, Default)]
pub struct Footprint {
    pub raw_bytes: i64,
    pub aggregate_bytes: i64,
}

/// Measure the footprint. Raw parents are partitioned, so their day children are
/// summed via a LIKE match; the API reads it read-only for the metrics scrape.
pub async fn footprint(pool: &PgPool) -> Result<Footprint, ApiError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT c.relname, pg_total_relation_size(c.oid)
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = 'public'
            AND c.relkind IN ('r', 'p')
            AND (c.relname IN ('swaps','liq_events','ohlcv_1m','ohlcv_1h','ohlcv_1d',
                               'wallet_positions','wallet_stats','lot_consumptions',
                               'pools','blocks','chain_state','alerts_sent')
                 OR c.relname LIKE 'swaps\\_%' OR c.relname LIKE 'liq\\_events\\_%')",
    )
    .fetch_all(pool)
    .await?;

    let mut fp = Footprint::default();
    for (name, bytes) in rows {
        if name == "swaps"
            || name == "liq_events"
            || name.starts_with("swaps_")
            || name.starts_with("liq_events_")
        {
            fp.raw_bytes += bytes;
        } else {
            fp.aggregate_bytes += bytes;
        }
    }
    Ok(fp)
}
