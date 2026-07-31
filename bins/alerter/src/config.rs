//! Alerter configuration, from the environment (`.env`, gitignored), like the
//! rest of the stack. Validated at startup so a missing token fails before the
//! first poll, not on the first alert.

use std::collections::HashSet;
use std::time::Duration;

use chainscope_core::types::Address20;
use chainscope_indexer::pnl::Numeraire;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub telegram_bot_token: String,
    pub telegram_chat_id: String,
    pub poll_interval: Duration,
    /// Watchlist-move threshold in USD.
    pub move_threshold_usd: f64,
    /// How many recent blocks the move detector rescans each poll. Dedupe makes
    /// the overlap between polls harmless.
    pub move_lookback_blocks: i64,
    /// How many distinct wallets make a cluster.
    pub cluster_size: i64,
    /// Cluster window in seconds.
    pub cluster_window_secs: i64,
    /// How many top wallets to watch.
    pub watchlist_size: i64,
    /// Which tokens can be priced in USD — same policy as the indexer's [pnl].
    pub numeraire: Numeraire,
}

const DEFAULT_POLL_SECS: u64 = 15;
const DEFAULT_MOVE_USD: f64 = 25_000.0;
const DEFAULT_MOVE_LOOKBACK: i64 = 300;
const DEFAULT_CLUSTER_SIZE: i64 = 3;
const DEFAULT_CLUSTER_WINDOW_SECS: i64 = 7_200; // 2h
const DEFAULT_WATCHLIST_SIZE: i64 = 100;

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = req("DATABASE_URL")?;
        let telegram_bot_token = req("TELEGRAM_BOT_TOKEN")?;
        let telegram_chat_id = req("TELEGRAM_CHAT_ID")?;
        Ok(Self {
            database_url,
            telegram_bot_token,
            telegram_chat_id,
            poll_interval: Duration::from_secs(parse_or("ALERTER_POLL_SECS", DEFAULT_POLL_SECS)?),
            move_threshold_usd: parse_or("ALERTER_MOVE_USD", DEFAULT_MOVE_USD)?,
            move_lookback_blocks: parse_or("ALERTER_MOVE_LOOKBACK_BLOCKS", DEFAULT_MOVE_LOOKBACK)?,
            cluster_size: parse_or("ALERTER_CLUSTER_SIZE", DEFAULT_CLUSTER_SIZE)?,
            cluster_window_secs: parse_or("ALERTER_CLUSTER_WINDOW_SECS", DEFAULT_CLUSTER_WINDOW_SECS)?,
            watchlist_size: parse_or("ALERTER_WATCHLIST_SIZE", DEFAULT_WATCHLIST_SIZE)?,
            numeraire: numeraire_from_env()?,
        })
    }
}

/// Build the pricing numeraire from the environment. Absent → prices nothing, so
/// the move detector finds no priceable swaps and stays quiet (a safe default).
fn numeraire_from_env() -> anyhow::Result<Numeraire> {
    let mut stables = HashSet::new();
    if let Ok(csv) = std::env::var("NUMERAIRE_STABLES") {
        for s in csv.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            stables.insert(parse_address(s)?);
        }
    }
    let weth = match std::env::var("NUMERAIRE_WETH") {
        Ok(s) => Some(parse_address(&s)?),
        Err(_) => None,
    };
    let weth_price_pool = match std::env::var("NUMERAIRE_WETH_PRICE_POOL") {
        Ok(s) => Some(parse_address(&s)?),
        Err(_) => None,
    };
    Ok(Numeraire {
        stables,
        weth,
        weth_price_pool,
    })
}

fn parse_address(s: &str) -> anyhow::Result<Address20> {
    let body = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    let mut out = [0u8; 20];
    if body.len() != 40 {
        anyhow::bail!("address must be 40 hex chars: {s}");
    }
    hex::decode_to_slice(body, &mut out).map_err(|_| anyhow::anyhow!("address not hex: {s}"))?;
    Ok(out)
}

fn req(key: &str) -> anyhow::Result<String> {
    let v = std::env::var(key).map_err(|_| anyhow::anyhow!("{key} is required"))?;
    if v.is_empty() {
        anyhow::bail!("{key} is empty");
    }
    Ok(v)
}

fn parse_or<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T> {
    match std::env::var(key) {
        Ok(v) => v.parse().map_err(|_| anyhow::anyhow!("{key} is not a valid value: {v}")),
        Err(_) => Ok(default),
    }
}
