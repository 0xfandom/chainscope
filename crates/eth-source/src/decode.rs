//! Uniswap V3 event decoding: raw log bytes in, one typed event out.
//!
//! This is a pure function of a single log — no network, no database, no state.
//! That is what makes it exhaustively testable offline against captured mainnet
//! fixtures, and it is why decoding lives here in the Ethereum crate rather than
//! in `chainscope-core`: the `sol!`-generated types are `alloy` types, and the
//! core crate depends on no chain library. The mapping from these typed events
//! to the core `SwapRow`/`LiqRow` shapes happens one layer up (issue #21); this
//! module's output deliberately still speaks `alloy`.
//!
//! A log is identified by `topics[0]`, the keccak hash of the event signature.
//! `decode` dispatches on it and returns `None` for anything it does not know,
//! so an unrecognised event is a caller-visible miss to be counted, never a
//! panic and never a silent success.

use alloy::{primitives::B256, sol, sol_types::SolEvent};
use chainscope_core::types::RawLog;

// The canonical Uniswap V3 pool and factory events. `sol!` generates, for each,
// a struct with typed fields and a `SolEvent` impl carrying its signature hash
// and a decoder — the same ABI the contracts were compiled from, so a field can
// only be read the way the chain wrote it.
sol! {
    /// Emitted by a pool on every swap.
    #[derive(Debug, PartialEq)]
    event Swap(
        address indexed sender,
        address indexed recipient,
        int256 amount0,
        int256 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick
    );

    /// Liquidity added to a position.
    #[derive(Debug, PartialEq)]
    event Mint(
        address sender,
        address indexed owner,
        int24 indexed tickLower,
        int24 indexed tickUpper,
        uint128 amount,
        uint256 amount0,
        uint256 amount1
    );

    /// Liquidity removed from a position.
    #[derive(Debug, PartialEq)]
    event Burn(
        address indexed owner,
        int24 indexed tickLower,
        int24 indexed tickUpper,
        uint128 amount,
        uint256 amount0,
        uint256 amount1
    );

    /// Owed tokens collected from a position.
    #[derive(Debug, PartialEq)]
    event Collect(
        address indexed owner,
        address recipient,
        int24 indexed tickLower,
        int24 indexed tickUpper,
        uint128 amount0,
        uint128 amount1
    );

    /// Emitted by the factory when a new pool is created. Decoded here so the
    /// sniffer (M7) can act on it; M2 only records that it decodes.
    #[derive(Debug, PartialEq)]
    event PoolCreated(
        address indexed token0,
        address indexed token1,
        uint24 indexed fee,
        int24 tickSpacing,
        address pool
    );
}

/// One decoded Uniswap V3 event. Still `alloy`-typed on purpose — the mapping to
/// the storage row shapes is issue #21.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedEvent {
    Swap(Swap),
    Mint(Mint),
    Burn(Burn),
    Collect(Collect),
    PoolCreated(PoolCreated),
}

impl DecodedEvent {
    /// A short, stable name for the event, for logs and metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Swap(_) => "Swap",
            Self::Mint(_) => "Mint",
            Self::Burn(_) => "Burn",
            Self::Collect(_) => "Collect",
            Self::PoolCreated(_) => "PoolCreated",
        }
    }
}

/// Decode one raw log into a typed event, or `None` if its signature is not one
/// we index.
///
/// `None` covers both an empty-topic (anonymous) log and any `topics[0]` we do
/// not recognise. The caller treats `None` as an unknown-event miss to count,
/// which is how a decoder gap becomes visible instead of silent.
pub fn decode(log: &RawLog) -> Option<DecodedEvent> {
    let topic0 = B256::from_slice(log.topics.first()?);

    // Compared by equality rather than matched, because an associated const is
    // not usable as a match pattern.
    if topic0 == Swap::SIGNATURE_HASH {
        decode_one::<Swap>(log).map(DecodedEvent::Swap)
    } else if topic0 == Mint::SIGNATURE_HASH {
        decode_one::<Mint>(log).map(DecodedEvent::Mint)
    } else if topic0 == Burn::SIGNATURE_HASH {
        decode_one::<Burn>(log).map(DecodedEvent::Burn)
    } else if topic0 == Collect::SIGNATURE_HASH {
        decode_one::<Collect>(log).map(DecodedEvent::Collect)
    } else if topic0 == PoolCreated::SIGNATURE_HASH {
        decode_one::<PoolCreated>(log).map(DecodedEvent::PoolCreated)
    } else {
        None
    }
}

/// Run one `SolEvent` decoder over our `RawLog`'s topics and data.
///
/// A log whose `topics[0]` matches the signature but whose remaining topics or
/// data do not fit the ABI is malformed; returning `None` lets the caller count
/// it as a miss rather than aborting the whole block.
fn decode_one<E: SolEvent>(log: &RawLog) -> Option<E> {
    let topics: Vec<B256> = log.topics.iter().map(|t| B256::from_slice(t)).collect();
    E::decode_raw_log(topics, &log.data).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, I256, U160};
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

    // A real USDC/WETH 0.05% Swap, captured off mainnet with eth_getLogs:
    //   block 25601357, tx 0xe18a0332…60eb5dc, logIndex 39.
    // topics[0] is the Swap signature; [1]/[2] are the indexed sender/recipient
    // (the same router here). The five data words are amount0, amount1,
    // sqrtPriceX96, liquidity, tick — verified against a hand-decode below.
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
    fn a_swap_log_decodes_to_the_exact_field_values() {
        let ev = decode(&swap_log()).expect("swap must decode");
        let DecodedEvent::Swap(s) = ev else {
            panic!("expected a Swap, got {:?}", ev.kind());
        };
        // Values hand-decoded from the on-chain data words.
        assert_eq!(s.amount0, I256::try_from(140586i64).unwrap());
        assert_eq!(s.amount1, I256::try_from(-74025266944810i64).unwrap());
        assert_eq!(
            s.sqrtPriceX96,
            U160::from(1820754252512732283398282170500178u128)
        );
        assert_eq!(s.liquidity, 1620197336976127727u128);
        assert_eq!(s.tick.as_i32(), 200858);
        let router = Address::from(addr("06cff7088619c7178f5e14f0b119458d08d2f5ef"));
        assert_eq!(s.sender, router);
        assert_eq!(s.recipient, router);
    }

    #[test]
    fn an_unknown_topic0_returns_none_not_a_panic() {
        let mut log = swap_log();
        // A topic0 that matches no known signature.
        log.topics[0] = h("dead00000000000000000000000000000000000000000000000000000000beef");
        assert!(decode(&log).is_none());
    }

    #[test]
    fn a_log_with_no_topics_returns_none() {
        let mut log = swap_log();
        log.topics.clear();
        assert!(decode(&log).is_none());
    }

    #[test]
    fn a_swap_topic0_with_truncated_data_returns_none_not_a_panic() {
        let mut log = swap_log();
        log.data.truncate(10); // not enough bytes for five words
        assert!(decode(&log).is_none());
    }

    #[test]
    fn every_event_signature_hash_is_distinct() {
        let sigs = [
            Swap::SIGNATURE_HASH,
            Mint::SIGNATURE_HASH,
            Burn::SIGNATURE_HASH,
            Collect::SIGNATURE_HASH,
            PoolCreated::SIGNATURE_HASH,
        ];
        for i in 0..sigs.len() {
            for j in (i + 1)..sigs.len() {
                assert_ne!(sigs[i], sigs[j], "signature hashes must be unique");
            }
        }
    }

    #[test]
    fn the_swap_signature_hash_is_the_known_uniswap_v3_constant() {
        // The canonical Uniswap V3 Swap topic0. If alloy ever computed this
        // differently, decoding real logs would silently stop matching.
        assert_eq!(
            Swap::SIGNATURE_HASH,
            B256::from(h("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"))
        );
    }
}
