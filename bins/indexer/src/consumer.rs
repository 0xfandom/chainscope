//! The writer: turn a stream of blocks into durable, exactly-once state.
//!
//! It pulls blocks from the seam, gathers them into batches, and writes each
//! batch in one transaction that also advances the cursor (see
//! [`crate::db::write_block_batch`]). The batching is purely for throughput —
//! one transaction per block would be correct but slow. Correctness lives
//! entirely in that transaction; this file only decides *when* to call it.
//!
//! Shutdown is not the writer's concern to detect. When the process stops it
//! cancels the producer, the producer drops its sink, and the stream closes.
//! The writer simply drains everything it was sent, flushes it — the in-flight
//! batch and its cursor included — and returns when the stream ends. There is
//! no shutdown signal to race, so a batch buffered at shutdown can never be
//! dropped: it is still in the stream, waiting to be drained.
//!
//! The cursor is advanced only inside the write transaction, never here.

use std::time::{Duration, Instant};

use chainscope_core::{BlockUnit, EventSource, Receipt};
use sqlx::postgres::PgPool;
use tokio::time::sleep;

pub struct Writer {
    pool: PgPool,
    source: Box<dyn EventSource<BlockUnit>>,
    max_batch: usize,
    flush_interval: Duration,
}

/// Why a batch stopped accumulating. `Closed` means the stream ended, so flush
/// what we have and stop; the others mean flush and keep going.
enum BatchEnd {
    Full,
    Timeout,
    Closed,
}

impl Writer {
    pub fn new(
        pool: PgPool,
        source: Box<dyn EventSource<BlockUnit>>,
        max_batch: usize,
        flush_interval: Duration,
    ) -> Self {
        Self {
            pool,
            source,
            max_batch: max_batch.max(1),
            flush_interval,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        tracing::info!(
            max_batch = self.max_batch,
            flush_ms = self.flush_interval.as_millis() as u64,
            "writer started"
        );
        let mut total: u64 = 0;

        loop {
            let mut batch: Vec<BlockUnit> = Vec::with_capacity(self.max_batch);
            let mut last_receipt: Option<Receipt> = None;
            let end = self.collect(&mut batch, &mut last_receipt).await?;

            if !batch.is_empty() {
                let first = batch.first().map(|b| b.number);
                let last = batch.last().map(|b| b.number);

                // Timed so #10 can turn this into a histogram; for now the
                // duration rides on the flush log line. A real metrics backend
                // is M10, not a reason to hold up the write path.
                let started = Instant::now();
                let written = crate::db::write_block_batch(&self.pool, &batch, false).await?;
                let elapsed = started.elapsed();
                total += written;

                // Acknowledge only after the commit returns. Acking earlier
                // would tell a durable transport the batch was safe before it
                // was, turning a crash into silent loss.
                if let Some(r) = last_receipt {
                    self.source.ack(r).await.ok();
                }

                tracing::info!(
                    from = first,
                    to = last,
                    rows = written,
                    duration_ms = elapsed.as_millis() as u64,
                    total,
                    "batch committed"
                );
            }

            match end {
                BatchEnd::Full | BatchEnd::Timeout => continue,
                BatchEnd::Closed => {
                    tracing::info!(total, "stream ended; writer stopping");
                    return Ok(());
                }
            }
        }
    }

    /// Fill `batch` until it is full, the flush interval elapses since the first
    /// block landed, or the stream closes.
    ///
    /// The flush timer starts when the *first* block of the batch arrives, not
    /// when collection begins, so an idle writer on a quiet chain does not flush
    /// empty batches on a timer — it simply waits for a block.
    async fn collect(
        &mut self,
        batch: &mut Vec<BlockUnit>,
        last_receipt: &mut Option<Receipt>,
    ) -> anyhow::Result<BatchEnd> {
        // Block until the first item. Nothing to flush until something arrives,
        // so there is no timer yet.
        match self.source.recv().await? {
            Some(d) => {
                batch.push(d.payload);
                *last_receipt = Some(d.receipt);
            }
            None => return Ok(BatchEnd::Closed),
        }

        if batch.len() >= self.max_batch {
            return Ok(BatchEnd::Full);
        }

        let deadline = sleep(self.flush_interval);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;
                _ = &mut deadline => return Ok(BatchEnd::Timeout),
                recv = self.source.recv() => match recv? {
                    Some(d) => {
                        batch.push(d.payload);
                        *last_receipt = Some(d.receipt);
                        if batch.len() >= self.max_batch {
                            return Ok(BatchEnd::Full);
                        }
                    }
                    None => return Ok(BatchEnd::Closed),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Batching logic here; the transactional guarantees are exercised against
    //! a real Postgres in `tests/writer_db.rs`, which is ignored by default.
    //! Splitting them keeps these fast and offline.

    use super::*;
    use chainscope_core::{types::Hash32, EventSink};

    fn block(n: u64) -> BlockUnit {
        let mut h: Hash32 = [0u8; 32];
        h[..8].copy_from_slice(&n.to_be_bytes());
        BlockUnit {
            number: n,
            hash: h,
            parent_hash: [0u8; 32],
            timestamp: 1_700_000_000 + n as i64,
            logs: vec![],
        }
    }

    /// A writer whose `collect` we drive without a database. The pool is created
    /// lazily and never connected, because `collect` never touches it.
    fn collector(max_batch: usize, flush: Duration) -> (Box<dyn EventSink<BlockUnit>>, Writer) {
        let (sink, source) =
            chainscope_core::build_transport::<BlockUnit>(chainscope_core::TransportKind::Channel, 256);
        let pool = PgPool::connect_lazy("postgres://unused").unwrap();
        (sink, Writer::new(pool, source, max_batch, flush))
    }

    #[tokio::test]
    async fn a_full_batch_stops_at_max_batch() {
        let (sink, mut w) = collector(3, Duration::from_secs(60));
        for n in 100..110 {
            sink.publish(block(n)).await.unwrap();
        }

        let mut batch = Vec::new();
        let mut r = None;
        let end = w.collect(&mut batch, &mut r).await.unwrap();

        assert!(matches!(end, BatchEnd::Full));
        assert_eq!(batch.iter().map(|b| b.number).collect::<Vec<_>>(), vec![100, 101, 102]);
        assert!(r.is_some(), "the last receipt must be captured for acking");
    }

    #[tokio::test]
    async fn a_partial_batch_flushes_on_the_timeout() {
        let (sink, mut w) = collector(100, Duration::from_millis(30));
        sink.publish(block(1)).await.unwrap();
        sink.publish(block(2)).await.unwrap();

        let mut batch = Vec::new();
        let mut r = None;
        let end = w.collect(&mut batch, &mut r).await.unwrap();

        // Two blocks, far short of 100, released by the timer rather than held.
        assert!(matches!(end, BatchEnd::Timeout));
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn a_closed_stream_still_flushes_what_it_already_has() {
        let (sink, mut w) = collector(100, Duration::from_secs(60));
        sink.publish(block(1)).await.unwrap();
        drop(sink); // producer finished — this is what shutdown looks like

        let mut batch = Vec::new();
        let mut r = None;
        let end = w.collect(&mut batch, &mut r).await.unwrap();

        assert!(matches!(end, BatchEnd::Closed));
        assert_eq!(batch.len(), 1, "a block already received must not be dropped on close");
    }

    #[tokio::test]
    async fn a_closed_stream_with_nothing_buffered_just_ends() {
        let (sink, mut w) = collector(100, Duration::from_secs(60));
        drop(sink);

        let mut batch = Vec::new();
        let mut r = None;
        let end = w.collect(&mut batch, &mut r).await.unwrap();

        assert!(matches!(end, BatchEnd::Closed));
        assert!(batch.is_empty());
    }
}
