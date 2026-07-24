//! The transformer: decode a block's logs into a `RowBatch`.
//!
//! The middle stage of the pipeline. It pulls a `BlockUnit` from the raw seam,
//! runs every watched log through the decoder, and publishes exactly one
//! `RowBatch` per block to the row seam.
//!
//! ```text
//! producer --BlockUnit--> [ transformer ] --RowBatch--> writer
//! ```
//!
//! It is deliberately stateless with respect to the chain: decoding a block
//! depends only on that block, so this stage keeps no cursor and touches no
//! database. All of the exactly-once machinery stays in the writer. Its only
//! running state is a count of logs it could not decode, which is a health
//! signal, not correctness.
//!
//! Like the writer, it holds no cancellation token. Shutdown reaches it as a
//! closed upstream: when the producer stops and drops its sink, `recv` returns
//! `None`, this stage drops its own sink, and that closes the stream into the
//! writer. One block in is always one block out — even an empty one — so the
//! writer advances the cursor past blocks that produced nothing and never
//! re-scans them.

use std::collections::HashSet;

use chainscope_core::{types::Address20, BlockUnit, EventSink, EventSource, RowBatch};
use chainscope_eth_source::{map_log, Mapped};

pub struct Transformer {
    source: Box<dyn EventSource<BlockUnit>>,
    sink: Box<dyn EventSink<RowBatch>>,
    /// Pools plus the factory. A log from anything else is not ours and is
    /// skipped before decoding; a log from one of these that fails to decode is
    /// a real gap and is counted.
    watched: HashSet<Address20>,
    /// Logs from a watched contract whose signature we do not index. Not an
    /// error — a visibility signal, reported at shutdown. The Prometheus counter
    /// (`indexer_unknown_events_total`) is wired when the metrics stack lands.
    unknown_total: u64,
}

impl Transformer {
    pub fn new(
        source: Box<dyn EventSource<BlockUnit>>,
        sink: Box<dyn EventSink<RowBatch>>,
        watched: impl IntoIterator<Item = Address20>,
    ) -> Self {
        Self {
            source,
            sink,
            watched: watched.into_iter().collect(),
            unknown_total: 0,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        tracing::info!(watched = self.watched.len(), "transformer started");

        loop {
            let Some(delivery) = self.source.recv().await? else {
                // Upstream finished and closed. Returning drops our sink, which
                // closes the stream into the writer — no sentinel, no signal.
                tracing::info!(
                    unknown_total = self.unknown_total,
                    "stream ended; transformer stopping"
                );
                return Ok(());
            };

            let batch = self.transform(&delivery.payload);

            // Backpressure point: if the writer is behind, this suspends. If the
            // writer is gone entirely, the stream is closed and we stop — the
            // supervisor surfaces the writer's own failure, not a duplicate here.
            if self.sink.publish(batch).await.is_err() {
                tracing::info!("downstream closed; transformer stopping");
                return Ok(());
            }

            // Ack the input only after the output is published, mirroring the
            // writer's ack-after-commit: a durable transport must not be told a
            // block is handled before its rows have moved on.
            self.source.ack(delivery.receipt).await.ok();
        }
    }

    /// Decode one block's watched logs into a `RowBatch`.
    ///
    /// Always returns a batch, even when the block produced no rows. An empty
    /// batch still carries the block's identity forward so the writer advances
    /// the cursor and does not re-scan the block on the next run.
    fn transform(&mut self, block: &BlockUnit) -> RowBatch {
        let mut swaps = Vec::new();
        let mut liq_events = Vec::new();

        for log in &block.logs {
            // Not one of our contracts — the RPC filter should have excluded it
            // already, but a synthetic source or a widened filter might not, and
            // decoding a stranger's log would only invent unknown-event noise.
            if !self.watched.contains(&log.address) {
                continue;
            }

            match map_log(log) {
                Mapped::Swap(s) => swaps.push(s),
                Mapped::Liq(l) => liq_events.push(l),
                // Understood, deliberately not stored in M2 (factory PoolCreated).
                Mapped::Ignored(_) => {}
                Mapped::Unknown => {
                    self.unknown_total += 1;
                    tracing::warn!(
                        pool = %hex::encode(log.address),
                        tx = %hex::encode(log.tx_hash),
                        log_index = log.log_index,
                        "unknown event from a watched contract; not decoded"
                    );
                }
            }
        }

        RowBatch {
            block_number: block.number,
            block_hash: block.hash,
            parent_hash: block.parent_hash,
            block_time: block.timestamp,
            swaps,
            liq_events,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainscope_core::types::{Hash32, RawLog};

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

    const POOL: &str = "8ad599c3a0ff1de082011efddc58f1908eb6e6d8";

    // The real mainnet Swap log used throughout the decoder tests.
    fn swap_log() -> RawLog {
        RawLog {
            address: addr(POOL),
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

    fn block(number: u64, logs: Vec<RawLog>) -> BlockUnit {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&number.to_be_bytes());
        BlockUnit {
            number,
            hash,
            parent_hash: [0u8; 32],
            timestamp: 1_700_000_000 + number as i64,
            logs,
        }
    }

    /// Build a transformer whose input and output we drive directly, plus the
    /// input sink and the output source.
    fn harness(
        watched: Vec<Address20>,
    ) -> (
        Box<dyn EventSink<BlockUnit>>,
        Transformer,
        Box<dyn EventSource<RowBatch>>,
    ) {
        let (in_sink, in_source) =
            chainscope_core::build_transport::<BlockUnit>(chainscope_core::TransportKind::Channel, 64);
        let (out_sink, out_source) =
            chainscope_core::build_transport::<RowBatch>(chainscope_core::TransportKind::Channel, 64);
        (in_sink, Transformer::new(in_source, out_sink, watched), out_source)
    }

    #[tokio::test]
    async fn a_watched_swap_becomes_a_row_in_the_batch() {
        let (in_sink, t, mut out) = harness(vec![addr(POOL)]);
        in_sink.publish(block(100, vec![swap_log()])).await.unwrap();
        drop(in_sink);
        let handle = tokio::spawn(t.run());

        let batch = out.recv().await.unwrap().unwrap().payload;
        assert_eq!(batch.block_number, 100);
        assert_eq!(batch.swaps.len(), 1);
        assert_eq!(batch.liq_events.len(), 0);
        assert_eq!(batch.swaps[0].amount0.to_string(), "140586");

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_log_from_an_unwatched_contract_is_skipped() {
        // Watch a different address than the log's pool.
        let (in_sink, t, mut out) = harness(vec![addr("1111111111111111111111111111111111111111")]);
        in_sink.publish(block(101, vec![swap_log()])).await.unwrap();
        drop(in_sink);
        let handle = tokio::spawn(t.run());

        let batch = out.recv().await.unwrap().unwrap().payload;
        // Present but empty: the block still flows so the cursor advances.
        assert_eq!(batch.block_number, 101);
        assert!(batch.swaps.is_empty() && batch.liq_events.is_empty());

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn an_empty_block_still_produces_a_batch() {
        let (in_sink, t, mut out) = harness(vec![addr(POOL)]);
        in_sink.publish(block(102, vec![])).await.unwrap();
        drop(in_sink);
        let handle = tokio::spawn(t.run());

        let batch = out.recv().await.unwrap().unwrap().payload;
        assert_eq!(batch.block_number, 102);
        assert!(batch.is_empty());

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn an_unknown_event_from_a_watched_pool_is_counted_not_stored() {
        let mut unknown = swap_log();
        unknown.topics[0] = h("dead00000000000000000000000000000000000000000000000000000000beef");

        let (in_sink, t, mut out) = harness(vec![addr(POOL)]);
        in_sink.publish(block(103, vec![unknown])).await.unwrap();
        drop(in_sink);
        let handle = tokio::spawn(t.run());

        let batch = out.recv().await.unwrap().unwrap().payload;
        assert!(batch.is_empty(), "an undecodable event stores nothing");

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_closed_input_closes_the_output() {
        let (in_sink, t, mut out) = harness(vec![addr(POOL)]);
        drop(in_sink); // producer never sent anything and finished
        let handle = tokio::spawn(t.run());

        // No batch, and the stream ends rather than hanging.
        assert!(out.recv().await.unwrap().is_none());
        handle.await.unwrap().unwrap();
    }
}
