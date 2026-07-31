//! Response shapes. Addresses and hashes are rendered `0x`-hex; NUMERIC amounts
//! are decimal strings, never floats.

use serde::Serialize;

/// One page of a keyset-paginated collection.
#[derive(Debug, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// Opaque cursor for the next page, or null at the end.
    pub next_cursor: Option<String>,
}

/// A known pool.
#[derive(Debug, Serialize)]
pub struct PoolDto {
    pub address: String,
    pub token0: String,
    pub token1: String,
    pub fee: i32,
    pub tick_spacing: i32,
    pub token0_symbol: Option<String>,
    pub token0_decimals: Option<i16>,
    pub token1_symbol: Option<String>,
    pub token1_decimals: Option<i16>,
    pub is_indexed: bool,
}

/// One OHLCV candle.
#[derive(Debug, Serialize)]
pub struct CandleDto {
    /// Interval start, unix epoch seconds.
    pub bucket: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume0: String,
    pub volume1: String,
    pub trade_count: i32,
}

/// One swap.
#[derive(Debug, Serialize)]
pub struct SwapDto {
    pub block_number: i64,
    pub log_index: i32,
    pub block_time: i64,
    pub tx_hash: String,
    pub pool: String,
    pub sender: String,
    pub recipient: String,
    pub amount0: String,
    pub amount1: String,
    pub sqrt_price_x96: String,
    pub tick: i32,
}
