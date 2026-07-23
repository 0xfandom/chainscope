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
}

impl TransportKind {
    /// Parse a configuration value.
    ///
    /// `kafka` is named here and rejected on purpose: "not implemented until
    /// M5" is a better answer than "unknown value", because the reader is
    /// asking a reasonable question.
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        match s.trim().to_ascii_lowercase().as_str() {
            "channel" => Ok(Self::Channel),
            "kafka" | "redpanda" => {
                Err("the log-based transport arrives in M5; use \"channel\" for now")
            }
            _ => Err("must be \"channel\""),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Channel => "channel",
        }
    }
}

/// The one place a concrete transport is named.
///
/// Every stage receives `Box<dyn EventSink<T>>` and `Box<dyn EventSource<T>>`
/// from here, so swapping transports is a configuration change. If a stage ever
/// needs to know which transport it is on, the seam has leaked.
pub fn build<T: Send + 'static>(
    kind: TransportKind,
    capacity: usize,
) -> (Box<dyn EventSink<T>>, Box<dyn EventSource<T>>) {
    match kind {
        TransportKind::Channel => {
            let (sink, source) = channel::<T>(capacity);
            (Box::new(sink), Box::new(source))
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
        let (sink, mut source) = build::<u32>(TransportKind::Channel, 8);

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
        let (sink, mut source) = build::<u32>(TransportKind::Channel, 1);

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
    fn transport_kind_parses_and_explains_kafka() {
        assert_eq!(TransportKind::parse("channel").unwrap(), TransportKind::Channel);
        assert_eq!(TransportKind::parse(" CHANNEL ").unwrap(), TransportKind::Channel);
        assert!(TransportKind::parse("kafka").unwrap_err().contains("M5"));
        assert!(TransportKind::parse("carrier pigeon").is_err());
    }
}
