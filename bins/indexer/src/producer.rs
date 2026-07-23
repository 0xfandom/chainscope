//! The producer: follow the chain head, one block at a time.
//!
//! Deliberately sequential. It reads the live cursor, walks forward block by
//! block, and publishes each one into the seam. Throughput is M3's problem and
//! reorg detection is M4's; this stage exists to be *correct and resumable*
//! first, and every shortcut taken here is one that a later milestone can undo
//! without changing the shape of what crosses the seam.
//!
//! The parent hash is already carried in every `BlockUnit` even though nothing
//! reads it yet. That is the point: when M4 adds reorg detection it changes the
//! consumer, not the producer and not the message.

use std::{sync::Arc, time::Duration};

use chainscope_core::{
    source::{ChainSource, SourceError},
    BlockUnit, EventSink,
};
use rand::Rng;
use tokio_util::sync::CancellationToken;
use tracing::{field, info_span, Instrument};

/// How a transient failure is retried.
///
/// Only [`SourceError::Transient`] is retried. `RangeTooLarge` needs a
/// different request, `BlockNotFound` needs a different block, and `Fatal`
/// needs a human — retrying any of them is a busy-wait dressed up as
/// resilience.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base: Duration,
    pub max: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 6,
            base: Duration::from_millis(250),
            // Free providers rate-limit for tens of seconds. Backing off past
            // this stops being politeness and starts being an outage.
            max: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Exponential backoff with full jitter: the delay is a random point in
    /// `[0, exponential)` rather than the exponential itself.
    ///
    /// Jitter matters even with a single producer. Without it, a provider that
    /// rate-limits us produces a retry at exactly 250ms, 500ms, 1s… and every
    /// process that hit the same limit at the same moment retries in lockstep,
    /// recreating the burst that caused the limit. Randomising spreads the
    /// retries out.
    fn delay(&self, attempt: u32) -> Duration {
        let exp = self
            .base
            .saturating_mul(2u32.saturating_pow(attempt))
            .min(self.max);
        let millis = exp.as_millis() as u64;
        Duration::from_millis(rand::rng().random_range(0..=millis.max(1)))
    }
}

/// Run one fallible source call, retrying only what is worth retrying.
///
/// Cancellation is checked while sleeping, not only between attempts: a
/// shutdown during a 30-second backoff should not wait 30 seconds.
async fn with_retry<T, F, Fut>(
    policy: &RetryPolicy,
    cancel: &CancellationToken,
    what: &str,
    mut op: F,
) -> Result<T, SourceError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, SourceError>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if !e.is_retryable() => return Err(e),
            Err(e) => {
                attempt += 1;
                if attempt >= policy.max_attempts {
                    tracing::warn!(what, attempts = attempt, error = %e, "giving up after retries");
                    return Err(e);
                }
                let delay = policy.delay(attempt);
                tracing::warn!(what, attempt, ?delay, error = %e, "transient failure, retrying");
                tokio::select! {
                    _ = cancel.cancelled() => return Err(e),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

pub struct Producer {
    source: Arc<dyn ChainSource>,
    sink: Box<dyn EventSink<BlockUnit>>,
    /// Where to resume. `None` means nothing has been processed yet.
    live_cursor: Option<u64>,
    /// Used only when there is no cursor. Zero means "start at the head".
    configured_start: u64,
    poll_interval: Duration,
    retry: RetryPolicy,
    cancel: CancellationToken,
}

impl Producer {
    pub fn new(
        source: Arc<dyn ChainSource>,
        sink: Box<dyn EventSink<BlockUnit>>,
        live_cursor: Option<u64>,
        configured_start: u64,
        poll_interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            source,
            sink,
            live_cursor,
            configured_start,
            poll_interval,
            retry: RetryPolicy::default(),
            cancel,
        }
    }

    /// Decide the first block to fetch.
    ///
    /// A stored cursor always wins — that is what makes a restart resume rather
    /// than repeat. With no cursor and no configured start, the live follower
    /// begins at the current head rather than at block zero: walking history
    /// one block at a time would take years, and history is the backfill's job
    /// (M3), not this stage's.
    async fn resume_point(&self) -> Result<u64, SourceError> {
        if let Some(c) = self.live_cursor {
            return Ok(c + 1);
        }
        if self.configured_start > 0 {
            return Ok(self.configured_start);
        }
        let tip = with_retry(&self.retry, &self.cancel, "latest_block", || {
            self.source.latest_block()
        })
        .await?;
        tracing::info!(tip, "no stored cursor; starting the live follower at the head");
        Ok(tip)
    }

    /// Follow the head until cancelled.
    ///
    /// Consumes `self` so that the sink is dropped on return. Dropping the sink
    /// closes the channel, which is how the downstream stage learns the stream
    /// ended — no sentinel message, no separate shutdown channel.
    pub async fn run(self) -> anyhow::Result<()> {
        let mut next = self.resume_point().await?;
        tracing::info!(next, "producer started");

        while !self.cancel.is_cancelled() {
            let tip = match with_retry(&self.retry, &self.cancel, "latest_block", || {
                self.source.latest_block()
            })
            .await
            {
                Ok(t) => t,
                Err(e) if self.cancel.is_cancelled() => {
                    tracing::debug!(error = %e, "head poll abandoned during shutdown");
                    break;
                }
                Err(e) => return Err(e.into()),
            };

            if next > tip {
                // Caught up. Sleeping here rather than spinning is the whole
                // reason this is a poll loop and not a busy loop.
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = tokio::time::sleep(self.poll_interval) => {}
                }
                continue;
            }

            while next <= tip && !self.cancel.is_cancelled() {
                match self.fetch_and_publish(next).await? {
                    Published::Yes => next += 1,

                    // The node does not have a block it just told us existed.
                    // Providers behind a load balancer disagree by a block or
                    // two, so re-polling the head is the right answer and
                    // retrying the same request against the same view is not.
                    //
                    // Waiting a full poll interval first is deliberate. Going
                    // straight back to the head would spin: the block is still
                    // missing, the tip still says it exists, and the loop turns
                    // into a hot retry against the provider that can never make
                    // progress until the node catches up.
                    Published::NotYetAvailable => {
                        tracing::debug!(number = next, "block not available yet; waiting for the next poll");
                        tokio::select! {
                            _ = self.cancel.cancelled() => {}
                            _ = tokio::time::sleep(self.poll_interval) => {}
                        }
                        break;
                    }

                    Published::SinkClosed => {
                        tracing::info!("downstream closed; producer stopping");
                        return Ok(());
                    }
                }
            }
        }

        tracing::info!(next, "producer stopped");
        Ok(())
    }

    async fn fetch_and_publish(&self, number: u64) -> anyhow::Result<Published> {
        let span = info_span!(
            "block",
            number,
            hash = field::Empty,
            logs = field::Empty,
        );

        let fetched = with_retry(&self.retry, &self.cancel, "fetch_block", || {
            self.source.fetch_block(number)
        })
        .instrument(span.clone())
        .await;

        let unit = match fetched {
            Ok(u) => u,
            Err(SourceError::BlockNotFound { .. }) => return Ok(Published::NotYetAvailable),
            Err(e) if self.cancel.is_cancelled() => {
                tracing::debug!(error = %e, "fetch abandoned during shutdown");
                return Ok(Published::SinkClosed);
            }
            Err(e) => return Err(e.into()),
        };

        span.record("hash", hex::encode(unit.hash));
        span.record("logs", unit.logs.len());
        let _enter = span.enter();
        tracing::info!("fetched");

        // The backpressure point. If the consumer is behind, this suspends.
        //
        // Cancellation races it, because a shutdown must not wait for a full
        // channel to drain — if nothing is reading, an un-raced publish would
        // block until the process was killed.
        tokio::select! {
            _ = self.cancel.cancelled() => Ok(Published::SinkClosed),
            r = self.sink.publish(unit) => match r {
                Ok(()) => Ok(Published::Yes),
                Err(_) => Ok(Published::SinkClosed),
            }
        }
    }
}

enum Published {
    Yes,
    NotYetAvailable,
    SinkClosed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chainscope_core::{types::Hash32, EventSource, RawLog};
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    };

    /// A `ChainSource` that answers from memory and can be told to fail.
    ///
    /// Worth the fifty lines: it makes retry behaviour, ordering and resume
    /// point deterministic. Testing those against a live endpoint would mean
    /// asserting on someone else's uptime.
    struct MockSource {
        tip: AtomicU64,
        /// Number of transient failures to emit before answering normally.
        fail_next: Mutex<u32>,
        fetch_calls: AtomicU64,
        /// Blocks that do not exist even though they are below the tip.
        missing: Mutex<Vec<u64>>,
    }

    impl MockSource {
        fn new(tip: u64) -> Arc<Self> {
            Arc::new(Self {
                tip: AtomicU64::new(tip),
                fail_next: Mutex::new(0),
                fetch_calls: AtomicU64::new(0),
                missing: Mutex::new(Vec::new()),
            })
        }

        fn failing(tip: u64, times: u32) -> Arc<Self> {
            let s = Self::new(tip);
            *s.fail_next.lock().unwrap() = times;
            s
        }

        fn take_failure(&self) -> Option<SourceError> {
            let mut f = self.fail_next.lock().unwrap();
            if *f > 0 {
                *f -= 1;
                Some(SourceError::Transient("mock outage".into()))
            } else {
                None
            }
        }
    }

    fn hash_for(n: u64) -> Hash32 {
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&n.to_be_bytes());
        h
    }

    #[async_trait]
    impl ChainSource for MockSource {
        async fn latest_block(&self) -> Result<u64, SourceError> {
            if let Some(e) = self.take_failure() {
                return Err(e);
            }
            Ok(self.tip.load(Ordering::SeqCst))
        }

        async fn finalized_block(&self) -> Result<u64, SourceError> {
            Ok(self.tip.load(Ordering::SeqCst).saturating_sub(64))
        }

        async fn fetch_block(&self, number: u64) -> Result<BlockUnit, SourceError> {
            self.fetch_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(e) = self.take_failure() {
                return Err(e);
            }
            if self.missing.lock().unwrap().contains(&number) {
                return Err(SourceError::BlockNotFound { number });
            }
            Ok(BlockUnit {
                number,
                hash: hash_for(number),
                parent_hash: hash_for(number.saturating_sub(1)),
                timestamp: 1_700_000_000 + number as i64,
                logs: vec![],
            })
        }

        async fn fetch_logs(&self, _from: u64, _to: u64) -> Result<Vec<RawLog>, SourceError> {
            Ok(vec![])
        }

        async fn block_hash(&self, number: u64) -> Result<Hash32, SourceError> {
            Ok(hash_for(number))
        }

        fn finality_depth(&self) -> u64 {
            64
        }
    }

    fn fast_producer(
        source: Arc<MockSource>,
        cursor: Option<u64>,
        start: u64,
        cancel: CancellationToken,
    ) -> (Producer, Box<dyn EventSource<BlockUnit>>) {
        let (sink, src) = chainscope_core::build_transport::<BlockUnit>(
            chainscope_core::TransportKind::Channel,
            64,
        );
        let mut p = Producer::new(
            source,
            sink,
            cursor,
            start,
            Duration::from_millis(5),
            cancel,
        );
        // Keep the test suite fast; the policy shape is asserted separately.
        p.retry = RetryPolicy {
            max_attempts: 6,
            base: Duration::from_millis(1),
            max: Duration::from_millis(4),
        };
        (p, src)
    }

    #[tokio::test]
    async fn publishes_every_block_from_the_cursor_to_the_tip_in_order() {
        let cancel = CancellationToken::new();
        let (p, mut rx) = fast_producer(MockSource::new(105), Some(100), 0, cancel.clone());
        let handle = tokio::spawn(p.run());

        let mut got = Vec::new();
        while got.len() < 5 {
            got.push(rx.recv().await.unwrap().unwrap().payload.number);
        }
        cancel.cancel();
        handle.await.unwrap().unwrap();

        // Cursor 100 means block 100 is done, so the next block is 101.
        assert_eq!(got, vec![101, 102, 103, 104, 105]);
    }

    #[tokio::test]
    async fn a_stored_cursor_wins_over_the_configured_start() {
        let cancel = CancellationToken::new();
        let (p, mut rx) = fast_producer(MockSource::new(60), Some(50), 10, cancel.clone());
        let handle = tokio::spawn(p.run());

        let first = rx.recv().await.unwrap().unwrap().payload.number;
        cancel.cancel();
        handle.await.unwrap().unwrap();

        assert_eq!(first, 51, "resumed from the configured start, not the cursor");
    }

    #[tokio::test]
    async fn with_no_cursor_and_no_configured_start_it_begins_at_the_head() {
        let cancel = CancellationToken::new();
        let (p, mut rx) = fast_producer(MockSource::new(900), None, 0, cancel.clone());
        let handle = tokio::spawn(p.run());

        let first = rx.recv().await.unwrap().unwrap().payload.number;
        cancel.cancel();
        handle.await.unwrap().unwrap();

        assert_eq!(first, 900, "a fresh live follower should start at the head");
    }

    /// The acceptance criterion: a simulated RPC failure is retried, not fatal.
    #[tokio::test]
    async fn transient_failures_are_retried_rather_than_crashing() {
        let source = MockSource::failing(101, 3);
        let cancel = CancellationToken::new();
        let (p, mut rx) = fast_producer(source.clone(), Some(100), 0, cancel.clone());
        let handle = tokio::spawn(p.run());

        let got = rx.recv().await.unwrap().unwrap().payload.number;
        cancel.cancel();
        handle.await.unwrap().expect("producer must not crash");

        assert_eq!(got, 101);
        assert_eq!(
            *source.fail_next.lock().unwrap(),
            0,
            "every injected failure should have been consumed by a retry"
        );
    }

    #[tokio::test]
    async fn a_fatal_error_is_not_retried() {
        struct AlwaysFatal;
        #[async_trait]
        impl ChainSource for AlwaysFatal {
            async fn latest_block(&self) -> Result<u64, SourceError> {
                Err(SourceError::Fatal("method not found".into()))
            }
            async fn finalized_block(&self) -> Result<u64, SourceError> {
                unreachable!()
            }
            async fn fetch_block(&self, _: u64) -> Result<BlockUnit, SourceError> {
                unreachable!()
            }
            async fn fetch_logs(&self, _: u64, _: u64) -> Result<Vec<RawLog>, SourceError> {
                unreachable!()
            }
            async fn block_hash(&self, _: u64) -> Result<Hash32, SourceError> {
                unreachable!()
            }
            fn finality_depth(&self) -> u64 {
                64
            }
        }

        let (sink, _rx) = chainscope_core::build_transport::<BlockUnit>(
            chainscope_core::TransportKind::Channel,
            8,
        );
        let p = Producer::new(
            Arc::new(AlwaysFatal),
            sink,
            Some(1),
            0,
            Duration::from_millis(5),
            CancellationToken::new(),
        );
        assert!(p.run().await.is_err(), "a fatal source error must surface");
    }

    /// A block the node claims not to have must not stall the follower forever
    /// or be skipped — it re-polls the head and tries again.
    #[tokio::test]
    async fn a_missing_block_is_re_polled_not_skipped() {
        let source = MockSource::new(102);
        source.missing.lock().unwrap().push(101);

        let cancel = CancellationToken::new();
        let (p, mut rx) = fast_producer(source.clone(), Some(100), 0, cancel.clone());
        let handle = tokio::spawn(p.run());

        // Let it spin a few poll cycles against the missing block.
        tokio::time::sleep(Duration::from_millis(60)).await;
        source.missing.lock().unwrap().clear();

        let first = rx.recv().await.unwrap().unwrap().payload.number;
        cancel.cancel();
        handle.await.unwrap().unwrap();

        assert_eq!(first, 101, "must not skip past the block it could not fetch");
    }

    #[tokio::test]
    async fn cancelling_stops_the_producer_and_closes_the_stream() {
        let cancel = CancellationToken::new();
        let (p, mut rx) = fast_producer(MockSource::new(100_000), Some(1), 0, cancel.clone());
        let handle = tokio::spawn(p.run());

        rx.recv().await.unwrap().unwrap();
        cancel.cancel();
        handle.await.unwrap().unwrap();

        // Drain whatever was already buffered, then the stream must end rather
        // than hang — the producer dropped its sink on the way out.
        while let Some(_d) = rx.recv().await.unwrap() {}
    }

    #[tokio::test]
    async fn the_producer_stops_when_the_consumer_goes_away() {
        let cancel = CancellationToken::new();
        let (p, rx) = fast_producer(MockSource::new(100_000), Some(1), 0, cancel.clone());
        drop(rx);
        // No cancellation: it must notice the closed sink by itself.
        tokio::time::timeout(Duration::from_secs(5), p.run())
            .await
            .expect("should return promptly")
            .unwrap();
    }

    #[test]
    fn backoff_grows_and_is_capped_and_jittered() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base: Duration::from_millis(100),
            max: Duration::from_millis(800),
        };
        // Full jitter means each delay is a random point below the exponential,
        // so assert the bound rather than an exact value.
        for attempt in 1..8 {
            let exp = policy
                .base
                .saturating_mul(2u32.pow(attempt))
                .min(policy.max);
            for _ in 0..50 {
                assert!(policy.delay(attempt) <= exp, "delay exceeded its ceiling");
            }
        }
        for _ in 0..50 {
            assert!(policy.delay(20) <= policy.max, "cap must hold at any attempt");
        }
    }
}
