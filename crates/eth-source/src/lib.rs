//! chainscope-eth-source — the Ethereum implementation of [`ChainSource`].
//!
//! This is the only crate that knows what Ethereum is. It speaks JSON-RPC
//! through `alloy`, and converts what comes back into the plain byte-array
//! types in `chainscope-core`. Nothing downstream ever sees an `alloy` type.
//!
//! ## Only standard JSON-RPC
//!
//! `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getLogs`. No provider
//! "enhanced" endpoints, however convenient. Those are another indexer's output
//! — consuming them would make our input someone else's opinion of the chain
//! rather than the chain itself, and would silently tie correctness to a vendor.

pub mod decode;

pub use decode::{decode, DecodedEvent};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    providers::{Provider, RootProvider},
    rpc::types::{Filter, Log},
    transports::{RpcError, TransportErrorKind},
};
use async_trait::async_trait;
use chainscope_core::{
    source::{ChainSource, SourceError},
    types::{Address20, BlockUnit, Hash32, RawLog},
};

/// Ethereum finalises after two epochs of 32 slots. Used only when the node
/// cannot answer `finalized` itself.
const ETH_FINALITY_DEPTH: u64 = 64;

pub struct EthSource {
    provider: RootProvider,

    /// Contracts whose logs we want.
    ///
    /// Held here rather than passed per call because "what do we watch" is
    /// configuration. It also keeps `eth_getLogs` narrow: asking for every log
    /// in a range and filtering client-side would move megabytes per block and
    /// burn the RPC quota that is the real constraint on this project.
    watched: Vec<Address>,
}

impl EthSource {
    /// `watched` is the pool set plus the factory. An empty list is allowed and
    /// means "every log", which is only ever wanted in a test; whether it is
    /// sensible is a configuration question, already answered upstream.
    pub fn new(rpc_url: &url::Url, watched: &[Address20]) -> Self {
        Self {
            // `RootProvider` rather than the builder's default stack: the
            // recommended fillers exist to complete outgoing transactions with
            // gas, nonce and chain id. This source only ever reads, so those
            // are pure overhead and one more thing that could make a request we
            // did not ask for.
            provider: RootProvider::new_http(rpc_url.clone()),
            watched: watched.iter().map(|a| Address::from(*a)).collect(),
        }
    }

    fn filter(&self, from: u64, to: u64) -> Filter {
        let f = Filter::new().from_block(from).to_block(to);
        if self.watched.is_empty() {
            f
        } else {
            f.address(self.watched.clone())
        }
    }
}

#[async_trait]
impl ChainSource for EthSource {
    async fn latest_block(&self) -> Result<u64, SourceError> {
        self.provider
            .get_block_number()
            .await
            .map_err(|e| classify(e, None))
    }

    async fn finalized_block(&self) -> Result<u64, SourceError> {
        let block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .map_err(|e| classify(e, None))?
            .ok_or_else(|| {
                // A node that does not track finality answers null rather than
                // failing. Fatal on purpose: quietly falling back to a guessed
                // depth would make finality a lie told confidently.
                SourceError::Fatal(
                    "node returned no block for the `finalized` tag; it may not support it".into(),
                )
            })?;
        Ok(block.header.number)
    }

    async fn fetch_block(&self, number: u64) -> Result<BlockUnit, SourceError> {
        let block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .await
            .map_err(|e| classify(e, None))?
            .ok_or(SourceError::BlockNotFound { number })?;

        // Two calls, not one. `eth_getBlockByNumber` returns transactions, not
        // logs — there is no standard method returning both, and going via
        // receipts would mean one request per transaction.
        let logs = self.fetch_logs(number, number).await?;

        Ok(BlockUnit {
            number: block.header.number,
            hash: block.header.hash.into(),
            parent_hash: block.header.parent_hash.into(),
            timestamp: block.header.timestamp as i64,
            logs,
        })
    }

    async fn fetch_logs(&self, from: u64, to: u64) -> Result<Vec<RawLog>, SourceError> {
        let logs = self
            .provider
            .get_logs(&self.filter(from, to))
            .await
            .map_err(|e| classify(e, Some((from, to))))?;

        logs.into_iter().map(convert_log).collect()
    }

    async fn block_hash(&self, number: u64) -> Result<Hash32, SourceError> {
        let block = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .await
            .map_err(|e| classify(e, None))?
            .ok_or(SourceError::BlockNotFound { number })?;
        Ok(block.header.hash.into())
    }

    fn finality_depth(&self) -> u64 {
        ETH_FINALITY_DEPTH
    }
}

/// Convert one `alloy` log into the transport-neutral form.
///
/// A log without a transaction hash or index cannot be keyed. That only happens
/// for pending logs, which we never ask for, so it is fatal rather than skipped
/// — silently dropping such a log would punch a hole in the natural key.
fn convert_log(log: Log) -> Result<RawLog, SourceError> {
    let missing = |what: &str| SourceError::Fatal(format!("log is missing its {what}"));

    Ok(RawLog {
        address: log.address().into(),
        topics: log.topics().iter().map(|t| (*t).into()).collect(),
        data: log.data().data.to_vec(),
        tx_hash: log
            .transaction_hash
            .ok_or_else(|| missing("transaction hash"))?
            .into(),
        log_index: log
            .log_index
            .ok_or_else(|| missing("log index"))?
            .try_into()
            .map_err(|_| SourceError::Fatal("log index does not fit in u32".into()))?,
    })
}

/// Turn a provider failure into something the caller can act on.
///
/// Classified by JSON-RPC error code where one is meaningful, and by message
/// text where it is not. Text matching is unpleasant, but providers disagree
/// about which code means "your range is too wide" — some send -32005, some
/// -32602, public nodes often send a plain -32000 with prose. The alternative
/// is treating a shrinkable request as a hard failure, which stalls the
/// backfill on exactly the dense ranges that matter most.
fn classify(err: RpcError<TransportErrorKind>, range: Option<(u64, u64)>) -> SourceError {
    if let RpcError::ErrorResp(payload) = &err {
        let text = payload.message.to_ascii_lowercase();
        let looks_like_range_limit = text.contains("too large")
            || text.contains("too many")
            || text.contains("range")
            || text.contains("more than")
            || text.contains("limit exceeded")
            || text.contains("query timeout");

        if let (true, Some((from, to))) = (looks_like_range_limit, range) {
            return SourceError::RangeTooLarge { from, to };
        }

        // -32005 is the conventional "request limits exceeded", which providers
        // also use for plain rate limiting.
        if payload.code == -32005 {
            return match range {
                Some((from, to)) => SourceError::RangeTooLarge { from, to },
                None => SourceError::Transient(payload.message.to_string()),
            };
        }

        // Anything else the node explicitly rejected is a request that will be
        // rejected again if repeated unchanged.
        return SourceError::Fatal(format!("rpc error {}: {}", payload.code, payload.message));
    }

    // Everything else is transport level: timeouts, resets, 5xx, DNS. The
    // request was fine and the world was briefly not.
    SourceError::Transient(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// USDC/WETH 0.05%, one of the pools this project indexes.
    const POOL: Address20 = [
        0x8a, 0xd5, 0x99, 0xc3, 0xa0, 0xff, 0x1d, 0xe0, 0x82, 0x01, 0x1e, 0xfd, 0xdc, 0x58, 0xf1,
        0x90, 0x8e, 0xb6, 0xe6, 0xd8,
    ];

    fn source(endpoint: &str) -> EthSource {
        EthSource::new(&endpoint.parse().unwrap(), &[POOL])
    }

    #[test]
    fn a_filter_without_watched_addresses_is_unrestricted() {
        let s = EthSource::new(&"https://example.invalid".parse().unwrap(), &[]);
        assert!(s.filter(1, 2).address.is_empty());
    }

    #[test]
    fn a_filter_with_watched_addresses_narrows_the_query() {
        assert!(!source("https://example.invalid").filter(1, 2).address.is_empty());
    }

    #[test]
    fn range_rejections_are_classified_as_shrinkable() {
        let payload = alloy::rpc::json_rpc::ErrorPayload {
            code: -32005,
            message: "query returned more than 10000 results".into(),
            data: None,
        };
        let err = classify(RpcError::ErrorResp(payload), Some((100, 200)));
        assert!(matches!(err, SourceError::RangeTooLarge { from: 100, to: 200 }));
        // Shrinkable is not retryable: the request itself has to change.
        assert!(!err.is_retryable());
    }

    #[test]
    fn transport_failures_are_classified_as_transient() {
        let err = classify(RpcError::Transport(TransportErrorKind::BackendGone), Some((1, 2)));
        assert!(matches!(err, SourceError::Transient(_)));
        assert!(err.is_retryable());
    }

    #[test]
    fn an_unrecognised_rpc_rejection_is_fatal_rather_than_retried_forever() {
        let payload = alloy::rpc::json_rpc::ErrorPayload {
            code: -32601,
            message: "method not found".into(),
            data: None,
        };
        let err = classify(RpcError::ErrorResp(payload), None);
        assert!(matches!(err, SourceError::Fatal(_)));
        assert!(!err.is_retryable());
    }

    // -----------------------------------------------------------------------
    // Network tests, ignored by default so an offline machine still passes:
    //   cargo test -p chainscope-eth-source -- --ignored --nocapture
    // -----------------------------------------------------------------------

    // Chosen by probing, not by reputation. Free endpoints differ wildly in
    // how much history they will serve `eth_getLogs` for: publicnode refuses
    // beyond roughly 128 blocks, 1rpc caps the range width, llamarpc was
    // returning 521s, and ankr now requires a key. These two answer.
    const ENDPOINT_A: &str = "https://rpc.flashbots.net";
    const ENDPOINT_B: &str = "https://eth.drpc.org";

    /// A block old enough to be settled, recent enough that every free
    /// endpoint here will still serve its logs.
    ///
    /// A fixed historical block number cannot be used: free providers gate old
    /// data behind a token, so the test would be measuring a billing tier
    /// rather than this code. Deep history needs a paid archive endpoint, which
    /// is an M3 concern.
    async fn settled_block(s: &EthSource) -> u64 {
        s.latest_block().await.unwrap() - 200
    }

    /// Rather than assert a hash copied from a block explorer, ask two
    /// independent providers for the same block and require them to agree. Two
    /// providers will not produce the same wrong answer if the byte conversion
    /// dropped or reordered something.
    #[tokio::test]
    #[ignore = "requires network"]
    async fn two_providers_agree_on_a_known_block() {
        let a_src = source(ENDPOINT_A);
        let number = settled_block(&a_src).await;

        let a = a_src.fetch_block(number).await.unwrap();
        let b = source(ENDPOINT_B).fetch_block(number).await.unwrap();

        assert_eq!(a.number, number);
        assert_eq!(a.hash, b.hash, "providers disagree on the block hash");
        assert_eq!(a.parent_hash, b.parent_hash);
        assert_eq!(a.timestamp, b.timestamp);
        assert_ne!(a.hash, [0u8; 32], "hash should not be all zeroes");

        println!("block      {number}");
        println!("hash       0x{}", hex(&a.hash));
        println!("parent     0x{}", hex(&a.parent_hash));
        println!("timestamp  {}", a.timestamp);
        println!("logs       {}", a.logs.len());
    }

    /// The property the reorg detector rests on: block N names block N-1.
    #[tokio::test]
    #[ignore = "requires network"]
    async fn parent_hash_links_to_the_previous_block() {
        let s = source(ENDPOINT_A);
        let n = settled_block(&s).await;

        let child = s.fetch_block(n).await.unwrap();
        let parent = s.block_hash(n - 1).await.unwrap();

        assert_eq!(
            child.parent_hash, parent,
            "parent_hash must equal the hash of the previous block"
        );
    }

    #[tokio::test]
    #[ignore = "requires network"]
    async fn finalized_trails_the_tip_by_a_plausible_margin() {
        let s = source(ENDPOINT_A);
        let tip = s.latest_block().await.unwrap();
        let finalized = s.finalized_block().await.unwrap();

        assert!(finalized < tip, "finalized {finalized} should trail tip {tip}");
        let lag = tip - finalized;
        assert!(lag < 200, "finality lag of {lag} blocks looks wrong");
        println!("tip {tip}, finalized {finalized}, lag {lag} blocks");
    }

    #[tokio::test]
    #[ignore = "requires network"]
    async fn logs_are_filtered_to_the_watched_pool() {
        let s = source(ENDPOINT_A);
        let to = settled_block(&s).await;
        let logs = s.fetch_logs(to - 100, to).await.unwrap();
        assert!(!logs.is_empty(), "expected logs from a busy pool");
        assert!(
            logs.iter().all(|l| l.address == POOL),
            "a log from an unwatched address came back"
        );
        println!("{} logs, all from the watched pool", logs.len());
    }

    #[tokio::test]
    #[ignore = "requires network"]
    async fn a_block_beyond_the_tip_is_not_found_rather_than_transient() {
        let s = source(ENDPOINT_A);
        let tip = s.latest_block().await.unwrap();
        let err = s.fetch_block(tip + 1_000_000).await.unwrap_err();
        assert!(matches!(err, SourceError::BlockNotFound { .. }), "got {err:?}");
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
