//! Map a decoded Uniswap V3 event to the storage row shapes in
//! `chainscope-core`.
//!
//! This is the second half of decoding: `decode` (in `decode.rs`) turns bytes
//! into an `alloy`-typed event; this turns that event into a `SwapRow` or
//! `LiqRow` — the chain-agnostic shapes the writer stores. It is the last place
//! an `alloy` type appears on the ingest path: everything it returns is core
//! types, so from here on nothing downstream knows what Ethereum is.
//!
//! Two correctness details live here:
//!   * Amounts convert to `BigDecimal` (matching the `NUMERIC` columns) via the
//!     integers' decimal `Display`, never through a float. Sign is preserved —
//!     a negative `amount0` is the token leaving the pool.
//!   * The pool address is the log's own `address` (the contract that emitted
//!     it), not anything inside the event payload.
//!
//! `map_log` classifies every log into exactly one of four outcomes, so the
//! caller (the transformer, #22) can store the rows, ignore what M2 does not
//! persist, and count what it could not decode.

use std::str::FromStr;

use alloy::primitives::Address;
use bigdecimal::BigDecimal;
use chainscope_core::types::{Address20, LiqKind, LiqRow, RawLog, SwapRow};

use crate::decode::{decode, Burn, Collect, DecodedEvent, Mint, Swap};

/// What one log turned into.
///
/// Naming `Ignored` and `Unknown` as distinct outcomes is deliberate: a factory
/// `PoolCreated` is understood but not stored in M2 (`Ignored`), whereas an
/// unrecognised signature is a decoder gap to be counted (`Unknown`). Collapsing
/// them would hide the gap.
#[derive(Debug, Clone, PartialEq)]
pub enum Mapped {
    Swap(SwapRow),
    Liq(LiqRow),
    /// Decoded but intentionally not persisted in M2. Carries the event name so
    /// the reason is visible in a log line.
    Ignored(&'static str),
    /// `topics[0]` matched no signature we index — a miss for the caller to
    /// count (`indexer_unknown_events_total`).
    Unknown,
}

/// Decode a raw log and map it to its storage row, or classify why it produced
/// none.
pub fn map_log(log: &RawLog) -> Mapped {
    match decode(log) {
        Some(DecodedEvent::Swap(s)) => Mapped::Swap(swap_row(log, &s)),
        Some(DecodedEvent::Mint(m)) => Mapped::Liq(mint_row(log, &m)),
        Some(DecodedEvent::Burn(b)) => Mapped::Liq(burn_row(log, &b)),
        Some(DecodedEvent::Collect(c)) => Mapped::Liq(collect_row(log, &c)),
        // Understood, but acting on new pools is the sniffer's job (M7).
        Some(DecodedEvent::PoolCreated(_)) => Mapped::Ignored("PoolCreated"),
        None => Mapped::Unknown,
    }
}

/// Convert any alloy integer to `BigDecimal` through its decimal string.
///
/// Uniswap amounts are `int256`/`uint256`/`uint160`, which overflow every Rust
/// integer, so the trip is through text, not a numeric cast — and never a float.
/// The `Display` of these types is exact base-10, so the parse cannot fail.
fn bd<T: std::fmt::Display>(v: T) -> BigDecimal {
    BigDecimal::from_str(&v.to_string()).expect("alloy integer Display is exact base-10")
}

fn a20(a: Address) -> Address20 {
    a.into_array()
}

fn swap_row(log: &RawLog, s: &Swap) -> SwapRow {
    SwapRow {
        tx_hash: log.tx_hash,
        log_index: log.log_index,
        pool: log.address,
        sender: a20(s.sender),
        recipient: a20(s.recipient),
        amount0: bd(s.amount0),
        amount1: bd(s.amount1),
        sqrt_price_x96: bd(s.sqrtPriceX96),
        liquidity: bd(s.liquidity),
        tick: s.tick.as_i32(),
    }
}

fn mint_row(log: &RawLog, m: &Mint) -> LiqRow {
    LiqRow {
        tx_hash: log.tx_hash,
        log_index: log.log_index,
        pool: log.address,
        kind: LiqKind::Mint,
        owner: a20(m.owner),
        tick_lower: m.tickLower.as_i32(),
        tick_upper: m.tickUpper.as_i32(),
        amount: bd(m.amount),
        amount0: bd(m.amount0),
        amount1: bd(m.amount1),
    }
}

fn burn_row(log: &RawLog, b: &Burn) -> LiqRow {
    LiqRow {
        tx_hash: log.tx_hash,
        log_index: log.log_index,
        pool: log.address,
        kind: LiqKind::Burn,
        owner: a20(b.owner),
        tick_lower: b.tickLower.as_i32(),
        tick_upper: b.tickUpper.as_i32(),
        amount: bd(b.amount),
        amount0: bd(b.amount0),
        amount1: bd(b.amount1),
    }
}

fn collect_row(log: &RawLog, c: &Collect) -> LiqRow {
    LiqRow {
        tx_hash: log.tx_hash,
        log_index: log.log_index,
        pool: log.address,
        kind: LiqKind::Collect,
        owner: a20(c.owner),
        tick_lower: c.tickLower.as_i32(),
        tick_upper: c.tickUpper.as_i32(),
        // Collect reports no liquidity delta — it withdraws already-owed tokens —
        // so the liquidity `amount` is zero; the token amounts carry the value.
        amount: BigDecimal::from(0),
        amount0: bd(c.amount0),
        amount1: bd(c.amount1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainscope_core::types::{Address20, Hash32};

    fn h(hex: &str) -> Hash32 {
        let mut out = [0u8; 32];
        hex::decode_to_slice(hex.trim_start_matches("0x"), &mut out).unwrap();
        out
    }
    fn addr(hex: &str) -> Address20 {
        let mut out = [0u8; 20];
        hex::decode_to_slice(hex.trim_start_matches("0x"), &mut out).unwrap();
        out
    }
    fn bytes(hex: &str) -> Vec<u8> {
        hex::decode(hex.trim_start_matches("0x")).unwrap()
    }
    fn bd_str(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    // The same real mainnet Swap used in decode.rs: block 25601357,
    // tx 0xe18a0332…60eb5dc, logIndex 39, USDC/WETH 0.05% pool.
    fn swap_log() -> RawLog {
        RawLog {
            address: addr("8ad599c3a0ff1de082011efddc58f1908eb6e6d8"),
            topics: vec![
                h("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"),
                h("00000000000000000000000006cff7088619c7178f5e14f0b119458d08d2f5ef"),
                h("00000000000000000000000006cff7088619c7178f5e14f0b119458d08d2f5ef"),
            ],
            data: bytes(
                "000000000000000000000000000000000000000000000000000000000002252a\
                 ffffffffffffffffffffffffffffffffffffffffffffffffffffbcaca64264d6\
                 00000000000000000000000000000000000059c52649e6ea40cba55920aa8452\
                 000000000000000000000000000000000000000000000000167c18e4d07ef6ef\
                 000000000000000000000000000000000000000000000000000000000003109a",
            ),
            tx_hash: h("e18a03325588278d1d9605c762339598b31f34a5f8b2fd62a7ff0bfed60eb5dc"),
            log_index: 39,
        }
    }

    #[test]
    fn a_swap_maps_to_a_swap_row_with_exact_values() {
        let Mapped::Swap(r) = map_log(&swap_log()) else {
            panic!("expected a swap row");
        };
        // Identity comes from the log, pool is the log's emitter address.
        assert_eq!(r.tx_hash, swap_log().tx_hash);
        assert_eq!(r.log_index, 39);
        assert_eq!(r.pool, addr("8ad599c3a0ff1de082011efddc58f1908eb6e6d8"));
        assert_eq!(r.sender, addr("06cff7088619c7178f5e14f0b119458d08d2f5ef"));
        assert_eq!(r.recipient, addr("06cff7088619c7178f5e14f0b119458d08d2f5ef"));
        // Amounts exact, sign preserved.
        assert_eq!(r.amount0, bd_str("140586"));
        assert_eq!(r.amount1, bd_str("-74025266944810"));
        assert_eq!(r.sqrt_price_x96, bd_str("1820754252512732283398282170500178"));
        assert_eq!(r.liquidity, bd_str("1620197336976127727"));
        assert_eq!(r.tick, 200858);
    }

    #[test]
    fn a_mint_maps_to_a_liq_row_with_the_mint_kind() {
        // A well-formed Mint, hand-built by ABI: owner + tickLower + tickUpper
        // are indexed (topics), then (amount, amount0, amount1) in the data.
        let log = RawLog {
            address: addr("88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"),
            topics: vec![
                h("7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"),
                h("000000000000000000000000c36442b4a4522e871399cd717abdd847ab11fe88"),
                h("0000000000000000000000000000000000000000000000000000000000030d40"), // 200000
                h("0000000000000000000000000000000000000000000000000000000000031128"), // 201000
            ],
            // Mint's `sender` is NOT indexed, so it leads the data:
            // sender, amount=1000000 (uint128), amount0=250, amount1=400.
            data: bytes(
                "000000000000000000000000c36442b4a4522e871399cd717abdd847ab11fe88\
                 00000000000000000000000000000000000000000000000000000000000f4240\
                 00000000000000000000000000000000000000000000000000000000000000fa\
                 0000000000000000000000000000000000000000000000000000000000000190",
            ),
            tx_hash: h("8d0f000000000000000000000000000000000000000000000000000000000012"),
            log_index: 12,
        };

        let Mapped::Liq(r) = map_log(&log) else {
            panic!("expected a liq row");
        };
        assert_eq!(r.kind, LiqKind::Mint);
        assert_eq!(r.owner, addr("c36442b4a4522e871399cd717abdd847ab11fe88"));
        assert_eq!(r.tick_lower, 200000);
        assert_eq!(r.tick_upper, 201000);
        assert_eq!(r.amount, bd_str("1000000"));
        assert_eq!(r.amount0, bd_str("250"));
        assert_eq!(r.amount1, bd_str("400"));
    }

    #[test]
    fn a_collect_has_zero_liquidity_amount() {
        // Collect: [sig, owner, tickLower, tickUpper] indexed; data = (recipient, amount0, amount1)
        let log = RawLog {
            address: addr("88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"),
            topics: vec![
                h("70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0"),
                h("000000000000000000000000c36442b4a4522e871399cd717abdd847ab11fe88"),
                h("0000000000000000000000000000000000000000000000000000000000030d40"),
                h("0000000000000000000000000000000000000000000000000000000000031128"),
            ],
            data: bytes(
                "000000000000000000000000c36442b4a4522e871399cd717abdd847ab11fe88\
                 00000000000000000000000000000000000000000000000000000000000003e8\
                 00000000000000000000000000000000000000000000000000000000000007d0",
            ),
            tx_hash: h("c0110000000000000000000000000000000000000000000000000000000000ec"),
            log_index: 7,
        };
        let Mapped::Liq(r) = map_log(&log) else {
            panic!("expected a liq row");
        };
        assert_eq!(r.kind, LiqKind::Collect);
        assert_eq!(r.amount, bd_str("0"), "collect carries no liquidity delta");
        assert_eq!(r.amount0, bd_str("1000"));
        assert_eq!(r.amount1, bd_str("2000"));
    }

    #[test]
    fn a_pool_created_is_ignored_not_stored() {
        // Just needs the PoolCreated signature in topic0 to classify; payload
        // does not have to decode for the Ignored branch we assert here — but
        // build it valid anyway.
        let log = RawLog {
            address: addr("1f98431c8ad98523631ae4a59f267346ea31f984"),
            topics: vec![
                h("783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"),
                h("000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
                h("000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
                h("00000000000000000000000000000000000000000000000000000000000001f4"),
            ],
            data: bytes(
                "000000000000000000000000000000000000000000000000000000000000000a\
                 0000000000000000000000008ad599c3a0ff1de082011efddc58f1908eb6e6d8",
            ),
            tx_hash: h("f00d000000000000000000000000000000000000000000000000000000000001"),
            log_index: 0,
        };
        assert_eq!(map_log(&log), Mapped::Ignored("PoolCreated"));
    }

    #[test]
    fn an_unknown_event_is_classified_unknown() {
        let mut log = swap_log();
        log.topics[0] = h("dead00000000000000000000000000000000000000000000000000000000beef");
        assert_eq!(map_log(&log), Mapped::Unknown);
    }
}
