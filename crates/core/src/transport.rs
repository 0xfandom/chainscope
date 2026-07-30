//! The transport seam.
//!
//! Stages never call each other. A stage publishes to an `EventSink` and reads
//! from an `EventSource`, and has no idea what is underneath. Today that is a
//! bounded `tokio::sync::mpsc` channel inside one process. In M5 it becomes a
//! Redpanda topic and the stages become separate processes. Only the two impls
//! and one factory function change; no stage does.
//!
//! Cutting this seam now costs a few dozen lines. Not cutting it makes M5 a
//! rewrite, because "send to the next stage" would be spelled differently in
//! every stage that does it.
//!
//! ## Why the traits look the way they do
//!
//! **Batches, not single events.** The writer commits one block per database
//! transaction, so a per-event interface would either lie about the boundary or
//! force reassembly on the far side.
//!
//! **`recv` returns `Option`.** `None` means the upstream stage finished and
//! closed, which is how a clean shutdown propagates: no sentinel value, no
//! separate control channel.
//!
//! **There is an `ack`.** In-memory delivery is done the moment the consumer
//! commits its database transaction, so `ChannelSource::ack` does nothing. A
//! log-based transport is different: the consumer must tell the broker how far
//! it has read, or a restart replays from the beginning. Leaving `ack` out
//! would mean adding it in M5 — and adding a method to this trait is exactly
//! the "touching stage logic" this issue exists to prevent. It costs one no-op
//! today and saves the seam later.
//!
//! **Receipts are opaque.** `Receipt` is a stream and a position. A channel
//! fills it with a local sequence number, Kafka with a partition and an offset.
//! Consumers pass it back without interpreting it.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::types::{BlockUnit, Envelope, RowBatch};

/// Failures that are the transport's fault, not the payload's.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The far end is gone. For a producer this is normal shutdown; for a
    /// consumer mid-stream it is not.
    #[error("transport closed")]
    Closed,

    /// Anything a real broker can fail with. Unused by the channel transport,
    /// which is why it carries a string rather than a typed enum — the shape of
    /// broker errors is not knowable until there is a broker.
    #[error("transport backend error: {0}")]
    Backend(String),
}

/// Where a message sat in the stream, in whatever terms the transport uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    /// Channel: always 0. Kafka: the partition.
    pub stream: u32,
    /// Channel: a local sequence number. Kafka: the offset.
    pub position: u64,
}

/// A payload plus the receipt the consumer must hand back once it is durably
/// processed.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery<T> {
    pub payload: T,
    pub receipt: Receipt,
}

/// What a payload needs to be to travel over a log transport.
///
/// The in-memory channel moves a `T` by value and needs nothing from it. A log
/// transport cannot: it has to turn the payload into bytes to append it, read it
/// back from bytes on the far side, and pick a partition to keep related events
/// ordered. `Wire` is exactly those three abilities, and no more.
///
/// It lives here, not in `types`, on purpose — `types` states the payloads as
/// plain data with no transport concern, and the bytes-on-the-log format is a
/// transport concern. Defining it once here is what keeps the wire format from
/// drifting between the stage that publishes and the stage that reads.
pub trait Wire: Send + Sync + 'static {
    /// Serialise for the log. The format is `bincode` and is fixed here so the
    /// bytes a producer writes are exactly the bytes a consumer decodes.
    fn to_bytes(&self) -> Result<Vec<u8>, TransportError>;

    /// Reconstruct from the bytes `to_bytes` produced.
    fn from_bytes(bytes: &[u8]) -> Result<Self, TransportError>
    where
        Self: Sized;

    /// The partition key. Everything sharing a key lands on one partition and
    /// stays ordered; different keys spread across partitions and run in
    /// parallel.
    ///
    /// Keyed by block number for now, which keeps a block's unit on a single
    /// partition and its stream ordered by height — what the writer's per-block
    /// cursor needs. Per-*pool* keying, which the revert broadcast (#61) leans
    /// on, arrives with the event-level topics; it belongs to that change, not
    /// this one.
    fn partition_key(&self) -> Vec<u8>;
}

/// One place defines the format for every `Wire` type, so "serialise" cannot
/// mean two different things in two stages.
fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, TransportError> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| TransportError::Backend(format!("encode: {e}")))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, TransportError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(value, _read)| value)
        .map_err(|e| TransportError::Backend(format!("decode: {e}")))
}

impl Wire for BlockUnit {
    fn to_bytes(&self) -> Result<Vec<u8>, TransportError> {
        encode(self)
    }
    fn from_bytes(bytes: &[u8]) -> Result<Self, TransportError> {
        decode(bytes)
    }
    fn partition_key(&self) -> Vec<u8> {
        self.number.to_be_bytes().to_vec()
    }
}

impl Wire for RowBatch {
    fn to_bytes(&self) -> Result<Vec<u8>, TransportError> {
        encode(self)
    }
    fn from_bytes(bytes: &[u8]) -> Result<Self, TransportError> {
        decode(bytes)
    }
    fn partition_key(&self) -> Vec<u8> {
        self.block_number.to_be_bytes().to_vec()
    }
}

/// The envelope is `Wire` whenever its payload is: encode the whole enum, and
/// key a `Data` by its payload's key so a payload keeps the partition it would
/// have had unwrapped. A `Revert` is chain-wide — #61 broadcasts a copy to every
/// partition — so its key is only a fallback for a single-partition topic; key
/// it by the fork block to stay deterministic.
impl<T> Wire for Envelope<T>
where
    T: Wire + serde::Serialize + serde::de::DeserializeOwned,
{
    fn to_bytes(&self) -> Result<Vec<u8>, TransportError> {
        encode(self)
    }
    fn from_bytes(bytes: &[u8]) -> Result<Self, TransportError> {
        decode(bytes)
    }
    fn partition_key(&self) -> Vec<u8> {
        match self {
            Envelope::Data(payload) => payload.partition_key(),
            Envelope::Revert { from_block } => from_block.to_be_bytes().to_vec(),
        }
    }
}

/// The publishing half of the seam.
///
/// `&self` rather than `&mut self` so several producer tasks can share one sink
/// without a lock — which is what a fan-out backfill needs.
#[async_trait]
pub trait EventSink<T>: Send + Sync {
    /// Publish one batch.
    ///
    /// This is the backpressure point. When the downstream stage is behind,
    /// this call suspends the caller rather than buffering. That is the whole
    /// mechanism: a fetcher that outruns the writer gets slowed down instead of
    /// growing a queue until the process dies.
    async fn publish(&self, batch: T) -> Result<(), TransportError>;
}

/// The consuming half of the seam.
#[async_trait]
pub trait EventSource<T>: Send {
    /// Next batch, or `None` once the producer has finished and closed.
    async fn recv(&mut self) -> Result<Option<Delivery<T>>, TransportError>;

    /// Mark everything up to and including `receipt` as durably processed.
    ///
    /// Called after the consumer's own transaction commits, never before —
    /// acknowledging first would turn a crash into silent data loss.
    async fn ack(&mut self, receipt: Receipt) -> Result<(), TransportError>;
}

// ---------------------------------------------------------------------------
// In-memory channel transport (phase 1)
// ---------------------------------------------------------------------------

/// Bounded, in-process, one producer group to one consumer.
pub struct ChannelSink<T> {
    tx: mpsc::Sender<Delivery<T>>,
    /// Shared so that cloned sinks keep issuing distinct receipts.
    next_position: Arc<AtomicU64>,
}

impl<T> Clone for ChannelSink<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            next_position: Arc::clone(&self.next_position),
        }
    }
}

pub struct ChannelSource<T> {
    rx: mpsc::Receiver<Delivery<T>>,
}

/// Create a bounded channel pair.
///
/// `capacity` is not a tuning detail. It is the size of the buffer that stands
/// between a fast producer and a slow consumer, and the reason `publish`
/// suspends instead of allocating. An unbounded channel here would let the
/// fetcher run ahead until memory ran out.
pub fn channel<T>(capacity: usize) -> (ChannelSink<T>, ChannelSource<T>) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (
        ChannelSink {
            tx,
            next_position: Arc::new(AtomicU64::new(0)),
        },
        ChannelSource { rx },
    )
}

#[async_trait]
impl<T: Send + 'static> EventSink<T> for ChannelSink<T> {
    async fn publish(&self, batch: T) -> Result<(), TransportError> {
        let receipt = Receipt {
            stream: 0,
            position: self.next_position.fetch_add(1, Ordering::Relaxed),
        };
        self.tx
            .send(Delivery {
                payload: batch,
                receipt,
            })
            .await
            .map_err(|_| TransportError::Closed)
    }
}

#[async_trait]
impl<T: Send + 'static> EventSource<T> for ChannelSource<T> {
    async fn recv(&mut self) -> Result<Option<Delivery<T>>, TransportError> {
        Ok(self.rx.recv().await)
    }

    async fn ack(&mut self, _receipt: Receipt) -> Result<(), TransportError> {
        // Nothing to do. In one process the message is already gone from the
        // channel, and durability is the consumer's database transaction. The
        // method exists so that the Kafka implementation, where this commits an
        // offset, is a new impl rather than a new trait method.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------

/// Which transport to build. Chosen once, from configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    /// Bounded in-memory channels, single process. Phase 1.
    Channel,
    /// A Redpanda (Kafka-API) log, stages as separate processes. Phase 2 (M5).
    Kafka,
}

impl TransportKind {
    /// Parse a configuration value.
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        match s.trim().to_ascii_lowercase().as_str() {
            "channel" => Ok(Self::Channel),
            "kafka" | "redpanda" => Ok(Self::Kafka),
            _ => Err("must be \"channel\" or \"kafka\""),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Channel => "channel",
            Self::Kafka => "kafka",
        }
    }
}

/// Everything the factory needs to build one seam. The `kind` alone is not
/// enough for a log — it needs brokers, a topic, and a consumer group — so the
/// construction parameters travel with it rather than being reconstructed at the
/// one call site. Borrowed, because the factory only reads them.
pub enum TransportSpec<'a> {
    /// Bounded in-memory channel of `capacity`.
    Channel { capacity: usize },
    /// A Kafka topic on `brokers`, consumed under `group_id`.
    Kafka {
        brokers: &'a str,
        topic: &'a str,
        group_id: &'a str,
    },
}

/// A built seam: the publishing half and the consuming half, both boxed so the
/// concrete transport stays invisible above this line.
pub type TransportPair<T> = (Box<dyn EventSink<T>>, Box<dyn EventSource<T>>);

/// The one place a concrete transport is named.
///
/// Every stage receives `Box<dyn EventSink<T>>` and `Box<dyn EventSource<T>>`
/// from here, so swapping transports is a configuration change. If a stage ever
/// needs to know which transport it is on, the seam has leaked.
///
/// Fallible, unlike the phase-1 channel factory it replaces: opening a broker
/// connection can fail, and the caller learns that here at startup rather than
/// on the first publish.
pub fn build_transport<T: Wire>(
    spec: TransportSpec<'_>,
) -> Result<TransportPair<T>, TransportError> {
    match spec {
        TransportSpec::Channel { capacity } => {
            let (sink, source) = channel::<T>(capacity);
            Ok((Box::new(sink), Box::new(source)))
        }
        #[cfg(feature = "kafka")]
        TransportSpec::Kafka {
            brokers,
            topic,
            group_id,
        } => {
            let sink = kafka::KafkaSink::<T>::new(brokers, topic)?;
            let source = kafka::KafkaSource::<T>::new(brokers, topic, group_id)?;
            Ok((Box::new(sink), Box::new(source)))
        }
        #[cfg(not(feature = "kafka"))]
        TransportSpec::Kafka { .. } => Err(TransportError::Backend(
            "this build has no kafka transport; rebuild with --features kafka".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Redpanda / Kafka log transport (phase 2) — feature-gated on `kafka`
// ---------------------------------------------------------------------------

/// The Redpanda-backed sink and source, over `rdkafka` (a Rust wrapper around
/// the librdkafka C client). Behind the `kafka` feature so only the indexer,
/// which actually runs the log, compiles the C library.
#[cfg(feature = "kafka")]
pub mod kafka {
    use std::{marker::PhantomData, time::Duration};

    use async_trait::async_trait;
    use rdkafka::{
        config::ClientConfig,
        consumer::{CommitMode, Consumer, StreamConsumer},
        message::Message,
        producer::{FutureProducer, FutureRecord},
        Offset, TopicPartitionList,
    };

    use super::{Delivery, EventSink, EventSource, Receipt, TransportError, Wire};

    /// How long a `publish` waits for the broker to accept a record before it is
    /// an error rather than backpressure.
    const PUBLISH_TIMEOUT: Duration = Duration::from_secs(30);

    fn backend(e: impl std::fmt::Display) -> TransportError {
        TransportError::Backend(e.to_string())
    }

    /// The publishing half, over an `rdkafka` `FutureProducer`.
    ///
    /// `PhantomData<fn(T)>` rather than `PhantomData<T>`: the sink only ever
    /// *consumes* a `T`, so this variance is correct and, unlike `PhantomData<T>`,
    /// it stays `Send + Sync` without requiring `T: Sync`.
    pub struct KafkaSink<T> {
        producer: FutureProducer,
        topic: String,
        _payload: PhantomData<fn(T)>,
    }

    impl<T: Wire> KafkaSink<T> {
        pub fn new(brokers: &str, topic: &str) -> Result<Self, TransportError> {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                // Bound the in-flight buffer so a fast producer is suspended by
                // a slow broker instead of growing unboundedly — the same
                // backpressure the bounded channel gives, one layer down.
                .set("queue.buffering.max.messages", "100000")
                .set("message.timeout.ms", "30000")
                .create()
                .map_err(backend)?;
            Ok(Self {
                producer,
                topic: topic.to_string(),
                _payload: PhantomData,
            })
        }
    }

    #[async_trait]
    impl<T: Wire> EventSink<T> for KafkaSink<T> {
        async fn publish(&self, batch: T) -> Result<(), TransportError> {
            let payload = batch.to_bytes()?;
            let key = batch.partition_key();
            let record = FutureRecord::to(&self.topic).payload(&payload).key(&key);
            self.producer
                .send(record, PUBLISH_TIMEOUT)
                .await
                .map_err(|(e, _owned_message)| backend(e))?;
            Ok(())
        }
    }

    /// The consuming half, over an `rdkafka` `StreamConsumer`.
    pub struct KafkaSource<T> {
        consumer: StreamConsumer,
        topic: String,
        _payload: PhantomData<fn() -> T>,
    }

    impl<T: Wire> KafkaSource<T> {
        pub fn new(brokers: &str, topic: &str, group_id: &str) -> Result<Self, TransportError> {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("group.id", group_id)
                // Offsets are committed by `ack` and nowhere else — a commit
                // before the consumer's own transaction lands would turn a crash
                // into silent data loss, so librdkafka's background auto-commit is
                // off and `ack` commits synchronously after the durable write.
                .set("enable.auto.commit", "false")
                // A brand-new group with no committed offset starts at the head
                // of the retained log, not at whatever is arriving now.
                .set("auto.offset.reset", "earliest")
                .create()
                .map_err(backend)?;
            consumer.subscribe(&[topic]).map_err(backend)?;
            Ok(Self {
                consumer,
                topic: topic.to_string(),
                _payload: PhantomData,
            })
        }
    }

    #[async_trait]
    impl<T: Wire> EventSource<T> for KafkaSource<T> {
        async fn recv(&mut self) -> Result<Option<Delivery<T>>, TransportError> {
            // Unlike the channel, a subscribed consumer has no "closed" end — the
            // log is unbounded in time, so this waits for the next record rather
            // than ever returning `None`. Shutdown is driven by cancelling the
            // task, exactly as it is for the finality poller.
            let borrowed = self.consumer.recv().await.map_err(backend)?;
            let payload = borrowed
                .payload()
                .ok_or_else(|| TransportError::Backend("record has no payload".into()))?;
            let value = T::from_bytes(payload)?;
            let receipt = Receipt {
                stream: borrowed.partition() as u32,
                position: borrowed.offset() as u64,
            };
            Ok(Some(Delivery {
                payload: value,
                receipt,
            }))
        }

        async fn ack(&mut self, receipt: Receipt) -> Result<(), TransportError> {
            // The offset is the distributed cursor. Committing it here — after the
            // caller's own durable write has returned (the writer commits its DB
            // transaction, then acks; see `Writer::run`) — is what makes
            // offset-as-cursor exactly-once: a crash between the DB commit and this
            // replays the message, and the idempotent forward apply (ON CONFLICT +
            // RETURNING-derived state) folds the replay to nothing.
            //
            // A Kafka committed offset names the *next* record to read, so commit
            // `position + 1`. Synchronous, so the resume point is durable on the
            // broker before this returns; auto-commit stays off so nothing is ever
            // committed ahead of a durable write.
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(
                &self.topic,
                receipt.stream as i32,
                Offset::Offset(receipt.position as i64 + 1),
            )
            .map_err(backend)?;
            self.consumer.commit(&tpl, CommitMode::Sync).map_err(backend)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn round_trip_preserves_payload_and_order() {
        let (sink, mut source) = channel::<u32>(8);

        for i in 0..5 {
            sink.publish(i).await.unwrap();
        }
        drop(sink);

        let mut got = Vec::new();
        while let Some(d) = source.recv().await.unwrap() {
            source.ack(d.receipt).await.unwrap();
            got.push(d.payload);
        }
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn receipts_are_distinct_and_increasing() {
        let (sink, mut source) = channel::<&str>(8);
        sink.publish("a").await.unwrap();
        sink.publish("b").await.unwrap();

        let first = source.recv().await.unwrap().unwrap().receipt;
        let second = source.recv().await.unwrap().unwrap().receipt;
        assert!(second.position > first.position);
    }

    /// The acceptance criterion for this issue: a full channel must suspend the
    /// producer, not grow.
    #[tokio::test]
    async fn a_full_channel_blocks_the_producer() {
        let (sink, mut source) = channel::<u32>(1);

        // Capacity 1: this one is buffered and returns immediately.
        sink.publish(1).await.unwrap();

        // The consumer is stalled, so this one has nowhere to go. If the
        // transport were unbounded it would return immediately and the process
        // would be one step closer to running out of memory.
        let blocked = timeout(Duration::from_millis(100), sink.publish(2)).await;
        assert!(blocked.is_err(), "publish should have suspended, it returned");

        // Drain one, and the producer can proceed — backpressure released, not
        // an error.
        let first = source.recv().await.unwrap().unwrap();
        assert_eq!(first.payload, 1);
        timeout(Duration::from_millis(100), sink.publish(2))
            .await
            .expect("publish should proceed once space exists")
            .unwrap();
    }

    #[tokio::test]
    async fn closing_the_sink_ends_the_stream() {
        let (sink, mut source) = channel::<u32>(4);
        sink.publish(7).await.unwrap();
        drop(sink);

        assert_eq!(source.recv().await.unwrap().unwrap().payload, 7);
        // None, not an error: the producer finishing is a normal shutdown.
        assert!(source.recv().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn publishing_to_a_dropped_consumer_reports_closed() {
        let (sink, source) = channel::<u32>(4);
        drop(source);
        assert!(matches!(
            sink.publish(1).await,
            Err(TransportError::Closed)
        ));
    }

    #[tokio::test]
    async fn a_cloned_sink_shares_the_receipt_sequence() {
        let (sink, mut source) = channel::<u32>(8);
        let second = sink.clone();

        sink.publish(1).await.unwrap();
        second.publish(2).await.unwrap();

        let a = source.recv().await.unwrap().unwrap().receipt.position;
        let b = source.recv().await.unwrap().unwrap().receipt.position;
        assert_ne!(a, b, "two producers must not issue the same receipt");
    }

    #[test]
    fn transport_kind_parses_channel_and_kafka() {
        assert_eq!(TransportKind::parse("channel").unwrap(), TransportKind::Channel);
        assert_eq!(TransportKind::parse(" CHANNEL ").unwrap(), TransportKind::Channel);
        assert_eq!(TransportKind::parse("kafka").unwrap(), TransportKind::Kafka);
        assert_eq!(TransportKind::parse("redpanda").unwrap(), TransportKind::Kafka);
        assert!(TransportKind::parse("carrier pigeon").is_err());
    }

    #[test]
    fn wire_round_trips_a_row_batch_through_bytes() {
        use crate::types::{LiqKind, LiqRow, RowBatch, SwapRow};
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        let batch = RowBatch {
            block_number: 21_000_000,
            block_hash: [7u8; 32],
            parent_hash: [6u8; 32],
            block_time: 1_700_000_000,
            swaps: vec![SwapRow {
                tx_hash: [1u8; 32],
                log_index: 3,
                pool: [2u8; 20],
                sender: [3u8; 20],
                recipient: [4u8; 20],
                // A big signed amount and a uint160-scale price — the values that
                // overflow every integer, which is why they are `BigDecimal` and
                // why the wire format has to preserve them exactly.
                amount0: BigDecimal::from_str("-123456789012345678901234567890").unwrap(),
                amount1: BigDecimal::from_str("987654321098765432109876543210").unwrap(),
                sqrt_price_x96: BigDecimal::from_str("1461446703485210103287273052203988822378723970342").unwrap(),
                liquidity: BigDecimal::from_str("340282366920938463463374607431768211455").unwrap(),
                tick: -887272,
            }],
            liq_events: vec![LiqRow {
                tx_hash: [9u8; 32],
                log_index: 5,
                pool: [2u8; 20],
                kind: LiqKind::Burn,
                owner: [8u8; 20],
                tick_lower: -600,
                tick_upper: 600,
                amount: BigDecimal::from_str("42").unwrap(),
                amount0: BigDecimal::from_str("0").unwrap(),
                amount1: BigDecimal::from_str("1").unwrap(),
            }],
        };

        let bytes = batch.to_bytes().unwrap();
        let back = RowBatch::from_bytes(&bytes).unwrap();
        assert_eq!(batch, back, "a RowBatch must survive the wire byte-identically");
        assert_eq!(batch.partition_key(), 21_000_000u64.to_be_bytes().to_vec());
    }
}
