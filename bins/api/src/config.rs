//! API configuration.
//!
//! Deliberately small and environment-driven: the read API is a thin service, so
//! it does not need the indexer's layered TOML machinery. Everything is validated
//! at startup so a bad value fails before a socket is opened.

use std::net::SocketAddr;
use std::time::Duration;

/// Validated API configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub database_url: String,
    pub max_connections: u32,
    /// TTL for the hot-stats cache (M7-6).
    pub cache_ttl: Duration,
}

const DEFAULT_BIND: &str = "0.0.0.0:8080";
const DEFAULT_MAX_CONNECTIONS: u32 = 16;
const DEFAULT_CACHE_TTL_SECS: u64 = 5;

impl Config {
    /// Read and validate configuration from the environment.
    pub fn from_env() -> anyhow::Result<Self> {
        let bind = std::env::var("API_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_owned());
        let bind: SocketAddr = bind
            .parse()
            .map_err(|_| anyhow::anyhow!("API_BIND is not a valid socket address: {bind}"))?;

        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL is required"))?;
        if database_url.is_empty() {
            anyhow::bail!("DATABASE_URL is empty");
        }

        let max_connections = parse_or("API_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS)?;
        if max_connections == 0 {
            anyhow::bail!("API_MAX_CONNECTIONS must be at least 1");
        }

        let cache_ttl_secs = parse_or("API_CACHE_TTL_SECS", DEFAULT_CACHE_TTL_SECS)?;

        Ok(Self {
            bind,
            database_url,
            max_connections,
            cache_ttl: Duration::from_secs(cache_ttl_secs),
        })
    }
}

fn parse_or<T>(key: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
{
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .map_err(|_| anyhow::anyhow!("{key} is not a valid number: {v}")),
        Err(_) => Ok(default),
    }
}
