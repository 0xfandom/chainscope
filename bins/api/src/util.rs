//! Small shared helpers for the HTTP edge.

use crate::error::ApiError;

/// Render raw bytes as a `0x`-prefixed hex string for JSON.
pub fn hex0x(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Parse a path/query address into 20 bytes, or 400 if it is not a valid address.
pub fn parse_address(s: &str) -> Result<[u8; 20], ApiError> {
    let body = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    let mut out = [0u8; 20];
    if body.len() != 40 {
        return Err(ApiError::bad_request("address must be 20 bytes (40 hex chars)"));
    }
    hex::decode_to_slice(body, &mut out)
        .map_err(|_| ApiError::bad_request("address is not valid hex"))?;
    Ok(out)
}
