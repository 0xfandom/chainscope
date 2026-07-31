//! Alerter configuration, from the environment (`.env`, gitignored), like the
//! rest of the stack. Validated at startup so a missing token fails before the
//! first poll, not on the first alert.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub telegram_bot_token: String,
    pub telegram_chat_id: String,
    pub poll_interval: Duration,
    /// Watchlist-move threshold in USD.
    pub move_threshold_usd: f64,
    /// How many distinct wallets make a cluster.
    pub cluster_size: i64,
    /// Cluster window in seconds.
    pub cluster_window_secs: i64,
    /// How many top wallets to watch.
    pub watchlist_size: i64,
}

const DEFAULT_POLL_SECS: u64 = 15;
const DEFAULT_MOVE_USD: f64 = 25_000.0;
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
            cluster_size: parse_or("ALERTER_CLUSTER_SIZE", DEFAULT_CLUSTER_SIZE)?,
            cluster_window_secs: parse_or("ALERTER_CLUSTER_WINDOW_SECS", DEFAULT_CLUSTER_WINDOW_SECS)?,
            watchlist_size: parse_or("ALERTER_WATCHLIST_SIZE", DEFAULT_WATCHLIST_SIZE)?,
        })
    }
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
