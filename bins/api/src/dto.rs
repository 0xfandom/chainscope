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

/// One leaderboard entry.
#[derive(Debug, Serialize)]
pub struct LeaderRowDto {
    pub wallet: String,
    pub realized_pnl_usd: String,
    pub trades: i32,
    pub wins: i32,
    pub volume_usd: String,
}

/// A recently discovered pool.
#[derive(Debug, Serialize)]
pub struct NewPoolDto {
    pub address: String,
    pub token0: String,
    pub token1: String,
    pub fee: i32,
    pub token0_symbol: Option<String>,
    pub token1_symbol: Option<String>,
    pub created_block: Option<i64>,
    /// Discovery time, unix epoch seconds.
    pub discovered_at: i64,
    pub is_indexed: bool,
}

/// A wallet's still-open position in one token.
#[derive(Debug, Serialize)]
pub struct OpenPositionDto {
    pub token: String,
    pub qty_held: String,
    pub cost_basis_usd: String,
}

/// One realised drawdown in a wallet's history.
#[derive(Debug, Serialize)]
pub struct RealizedTradeDto {
    pub sell_block: i64,
    pub consume_seq: i32,
    pub token: String,
    pub qty_consumed: String,
    pub proceeds_usd: String,
    pub realized_pnl_usd: String,
}

/// A wallet's full PnL picture.
#[derive(Debug, Serialize)]
pub struct ScorecardDto {
    pub wallet: String,
    pub realized_pnl_usd: String,
    pub trades: i32,
    pub wins: i32,
    pub volume_usd: String,
    pub avg_size_usd: String,
    pub excluded: bool,
    pub open_positions: Vec<OpenPositionDto>,
    pub recent_realized: Vec<RealizedTradeDto>,
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
