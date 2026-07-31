//! Keyset pagination.
//!
//! Deep pages must stay O(limit), not O(offset) — that is the whole reason the
//! p99 target is reachable at 200 rps. So a page is bounded by the *value* of the
//! last row seen, not a row count to skip: `WHERE (block_number, log_index) <
//! (last_block, last_log)`, which the descending composite index walks directly.
//!
//! The cursor is opaque to clients — hex of the 12 bytes `(i64 block, i32 log)`.
//! It carries no offset and cannot drift when rows are inserted between reads.

use crate::error::ApiError;

/// Default and hard-max page sizes. A client cannot ask for an unbounded page.
pub const DEFAULT_LIMIT: u32 = 50;
pub const MAX_LIMIT: u32 = 500;

/// The keyset position of the last row a page returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Keyset {
    pub block_number: i64,
    pub log_index: i32,
}

impl Keyset {
    /// Encode to an opaque cursor: hex of `block_number` (8 bytes) then
    /// `log_index` (4 bytes), big-endian.
    pub fn encode(&self) -> String {
        let mut bytes = [0u8; 12];
        bytes[..8].copy_from_slice(&self.block_number.to_be_bytes());
        bytes[8..].copy_from_slice(&self.log_index.to_be_bytes());
        hex::encode(bytes)
    }

    /// Decode a cursor, or 400 if it is malformed.
    pub fn decode(cursor: &str) -> Result<Self, ApiError> {
        let bytes = hex::decode(cursor)
            .ok()
            .filter(|b| b.len() == 12)
            .ok_or_else(|| ApiError::bad_request("malformed cursor"))?;
        let block_number = i64::from_be_bytes(bytes[..8].try_into().unwrap());
        let log_index = i32::from_be_bytes(bytes[8..].try_into().unwrap());
        Ok(Keyset {
            block_number,
            log_index,
        })
    }
}

/// Clamp a requested page size into `[1, MAX_LIMIT]`, defaulting when absent.
pub fn clamp_limit(requested: Option<u32>) -> i64 {
    requested.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as i64
}

/// Decode an optional cursor parameter.
pub fn decode_cursor(cursor: &Option<String>) -> Result<Option<Keyset>, ApiError> {
    match cursor {
        Some(c) => Ok(Some(Keyset::decode(c)?)),
        None => Ok(None),
    }
}

/// A single-column keyset over a candle `bucket`, carried as its unix epoch.
pub fn encode_bucket(epoch: i64) -> String {
    hex::encode(epoch.to_be_bytes())
}

/// Decode an optional bucket cursor into a unix epoch, or 400 if malformed.
pub fn decode_bucket(cursor: &Option<String>) -> Result<Option<i64>, ApiError> {
    match cursor {
        None => Ok(None),
        Some(c) => {
            let bytes = hex::decode(c)
                .ok()
                .filter(|b| b.len() == 8)
                .ok_or_else(|| ApiError::bad_request("malformed cursor"))?;
            Ok(Some(i64::from_be_bytes(bytes.try_into().unwrap())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips() {
        let k = Keyset {
            block_number: 19_000_000,
            log_index: 42,
        };
        assert_eq!(Keyset::decode(&k.encode()).unwrap(), k);
    }

    #[test]
    fn a_bad_cursor_is_rejected() {
        assert!(matches!(Keyset::decode("zzz"), Err(ApiError::BadRequest(_))));
        assert!(matches!(Keyset::decode("00"), Err(ApiError::BadRequest(_))));
    }

    #[test]
    fn limit_is_clamped() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT as i64);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(99_999)), MAX_LIMIT as i64);
    }
}
