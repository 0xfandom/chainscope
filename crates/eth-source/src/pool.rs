//! A [`ChainSource`] that fans one logical chain read across several endpoints.
//!
//! Backfill is where RPC quota is the real constraint: a single free endpoint
//! rate-limits within seconds under a ranged sweep. `ChainSource` (M1 #5)
//! already hides "which endpoint" behind one interface, so this is a new
//! implementation, not a new architecture — the producer, the transformer, and
//! the coming backfill driver all keep calling the same six methods, unaware
//! that there is now more than one endpoint underneath.
//!
//! ## What failover is, and what it is not
//!
//! A call is attempted against one provider; if that provider fails in a way a
//! *different* provider might not, the call rotates to the next healthy one and
//! retries. The pivot is the error taxonomy from `SourceError`, and it is the
//! whole reason those variants are typed rather than stringly:
//!
//!   * [`SourceError::Transient`] — rate limit, timeout, 5xx. This provider is
//!     briefly unwell; another may answer. **Fail over.**
//!   * [`SourceError::RangeTooLarge`] — the request is too wide. Every provider
//!     will say the same thing, and the fix is a smaller request, not a
//!     different endpoint. **Surface it** — it is the chunker's signal (#33),
//!     not a bad provider.
//!   * [`SourceError::BlockNotFound`] — the block is genuinely not there (ahead
//!     of the tip, or pruned). Rotating would only ask the same question of a
//!     node with the same answer, and the live producer already handles it by
//!     re-polling the head. **Surface it.**
//!   * [`SourceError::Fatal`] — a bug or misconfiguration. **Surface it**, never
//!     let failover paper over it.
//!
//! So the failover condition is exactly [`SourceError::is_retryable`]: only a
//! `Transient` fails over. That single predicate keeps this file honest — the
//! moment a new error variant is added, the compiler-visible decision of
//! "does this fail over" is already made for it.
//!
//! ## Health, and why a wedged endpoint is skipped rather than fought
//!
//! Each provider carries a count of consecutive failures. Once it crosses a
//! threshold the provider is considered wedged and is moved to the *back* of
//! the attempt order — tried only as a last resort when every other provider
//! has also failed. Any single success resets the count to zero, so a provider
//! that recovers rejoins the front on its next good answer. Without this, a
//! rate-limited endpoint at the front of a fixed order would be retried in
//! lockstep on every call and the pool would spend its quota re-confirming that
//! the same endpoint is still down.
//!
//! A sticky "preferred" pointer records the last provider that answered, so a
//! healthy pool does not round-robin needlessly — it keeps hitting the endpoint
//! that works until it stops working.

use std::{
    future::Future,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use chainscope_core::{
    source::{ChainSource, SourceError},
    types::{Address20, BlockUnit, Hash32, RawLog},
};

use crate::EthSource;

/// After this many consecutive failures a provider is treated as wedged and
/// tried last. Small on purpose: two strikes is enough to stop leading with an
/// endpoint that just rate-limited, and one success clears it immediately.
const DEFAULT_WEDGE_THRESHOLD: u32 = 2;

/// One endpoint plus its running health.
struct Provider {
    source: Arc<dyn ChainSource>,
    /// A stable, secret-free name for log lines — the host, never the full URL,
    /// because RPC providers put API keys in the path or query.
    label: String,
    /// Consecutive `Transient` failures since the last success. Reset to zero on
    /// any success. `Relaxed` is sufficient: this is a health hint that biases
    /// ordering, not a value any correctness decision hinges on.
    consecutive_failures: AtomicU32,
}

/// Several endpoints behind one `ChainSource`.
pub struct PooledSource {
    providers: Vec<Provider>,
    /// Index of the last provider that answered. The attempt order starts here,
    /// so a working pool sticks to what works instead of rotating for its own
    /// sake.
    preferred: AtomicUsize,
    wedge_threshold: u32,
}

impl PooledSource {
    /// Build a pool from already-constructed sources.
    ///
    /// Takes `Arc<dyn ChainSource>` rather than concrete `EthSource` so the
    /// failover logic can be tested against in-memory mocks — the pool cares
    /// only that each member answers the trait, not that it speaks to Ethereum.
    ///
    /// # Panics
    /// If `providers` is empty. Callers get their endpoint list from validated
    /// configuration, which already guarantees at least one, so an empty pool is
    /// a programming error rather than a runtime condition to handle.
    pub fn new(providers: Vec<(Arc<dyn ChainSource>, String)>) -> Self {
        assert!(!providers.is_empty(), "a pool needs at least one endpoint");
        Self {
            providers: providers
                .into_iter()
                .map(|(source, label)| Provider {
                    source,
                    label,
                    consecutive_failures: AtomicU32::new(0),
                })
                .collect(),
            preferred: AtomicUsize::new(0),
            wedge_threshold: DEFAULT_WEDGE_THRESHOLD,
        }
    }

    /// Build a pool of `EthSource`s, one per endpoint, all watching the same
    /// contracts. This is the constructor `main` uses.
    pub fn from_endpoints(endpoints: &[url::Url], watched: &[Address20]) -> Self {
        let providers = endpoints
            .iter()
            .map(|url| {
                let label = url.host_str().unwrap_or("unknown").to_owned();
                let source: Arc<dyn ChainSource> = Arc::new(EthSource::new(url, watched));
                (source, label)
            })
            .collect();
        Self::new(providers)
    }

    /// The order in which providers are tried for one call: healthy ones first
    /// starting from `preferred` and wrapping, then any wedged ones as a last
    /// resort. Wedged providers stay in the list — dropping them would mean a
    /// pool that never recovers once every endpoint has stumbled once.
    fn attempt_order(&self) -> Vec<usize> {
        let n = self.providers.len();
        let start = self.preferred.load(Ordering::Relaxed) % n;

        let mut healthy = Vec::with_capacity(n);
        let mut wedged = Vec::new();
        for k in 0..n {
            let i = (start + k) % n;
            if self.providers[i].consecutive_failures.load(Ordering::Relaxed) >= self.wedge_threshold
            {
                wedged.push(i);
            } else {
                healthy.push(i);
            }
        }
        healthy.extend(wedged);
        healthy
    }

    /// Run one source call across the pool, rotating on `Transient` failures.
    ///
    /// The operation is handed an owned `Arc<dyn ChainSource>` rather than a
    /// borrow so the future it returns owns its provider — that sidesteps the
    /// lifetime tangle of a borrowed `&dyn` living across an await inside a
    /// generic closure.
    async fn run<T, F, Fut>(&self, what: &str, op: F) -> Result<T, SourceError>
    where
        F: Fn(Arc<dyn ChainSource>) -> Fut,
        Fut: Future<Output = Result<T, SourceError>>,
    {
        let order = self.attempt_order();
        let mut last_transient: Option<SourceError> = None;

        for i in order {
            let provider = &self.providers[i];
            match op(Arc::clone(&provider.source)).await {
                Ok(value) => {
                    provider.consecutive_failures.store(0, Ordering::Relaxed);
                    self.preferred.store(i, Ordering::Relaxed);
                    return Ok(value);
                }
                // Only a transient failure is another provider's problem to try.
                Err(e) if e.is_retryable() => {
                    let fails = provider.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                    tracing::warn!(
                        what,
                        provider = %provider.label,
                        consecutive_failures = fails,
                        error = %e,
                        "provider transient failure; failing over to the next endpoint"
                    );
                    last_transient = Some(e);
                    continue;
                }
                // RangeTooLarge, BlockNotFound, Fatal: not a failover case. A
                // different endpoint would give the same answer, and masking a
                // Fatal behind a rotation is how a misconfiguration hides.
                Err(e) => return Err(e),
            }
        }

        // Every provider was transiently down. Report the last failure so the
        // caller's own retry/backoff can decide whether to wait and try again.
        Err(last_transient.unwrap_or_else(|| {
            SourceError::Transient("no endpoints available in the pool".into())
        }))
    }
}

#[async_trait]
impl ChainSource for PooledSource {
    async fn latest_block(&self) -> Result<u64, SourceError> {
        self.run("latest_block", |s| async move { s.latest_block().await })
            .await
    }

    async fn finalized_block(&self) -> Result<u64, SourceError> {
        self.run("finalized_block", |s| async move { s.finalized_block().await })
            .await
    }

    async fn fetch_block(&self, number: u64) -> Result<BlockUnit, SourceError> {
        self.run("fetch_block", move |s| async move { s.fetch_block(number).await })
            .await
    }

    async fn fetch_logs(&self, from: u64, to: u64) -> Result<Vec<RawLog>, SourceError> {
        self.run("fetch_logs", move |s| async move { s.fetch_logs(from, to).await })
            .await
    }

    async fn block_hash(&self, number: u64) -> Result<Hash32, SourceError> {
        self.run("block_hash", move |s| async move { s.block_hash(number).await })
            .await
    }

    fn finality_depth(&self) -> u64 {
        // A property of the chain, not of any one endpoint, so the first
        // provider's answer stands for the pool. All members index the same
        // chain — mixing chains in one pool is a configuration error out of
        // scope here.
        self.providers[0].source.finality_depth()
    }
}

#[cfg(test)]
impl PooledSource {
    /// Read one provider's live failure count. Test-only: the count is private
    /// health state, exposed here so the failover tests can assert on the
    /// mechanism directly rather than inferring it from behaviour.
    fn failures_for_test(&self, i: usize) -> u32 {
        self.providers[i].consecutive_failures.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A `ChainSource` whose every call returns a scripted outcome, so failover
    /// behaviour is deterministic without a network.
    struct MockSource {
        /// What `latest_block` should do on each successive call. Cycles on the
        /// last entry once exhausted, so "always fail" is a single element.
        script: Mutex<Vec<Outcome>>,
        calls: AtomicUsize,
    }

    #[derive(Clone)]
    enum Outcome {
        Ok(u64),
        Transient,
        Fatal,
        RangeTooLarge,
        NotFound,
    }

    impl MockSource {
        fn new(script: Vec<Outcome>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                calls: AtomicUsize::new(0),
            })
        }

        fn always(o: Outcome) -> Arc<Self> {
            Self::new(vec![o])
        }

        fn next_outcome(&self) -> Outcome {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let script = self.script.lock().unwrap();
            script[n.min(script.len() - 1)].clone()
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn as_outcome(&self) -> Result<u64, SourceError> {
            match self.next_outcome() {
                Outcome::Ok(v) => Ok(v),
                Outcome::Transient => Err(SourceError::Transient("mock outage".into())),
                Outcome::Fatal => Err(SourceError::Fatal("mock fatal".into())),
                Outcome::RangeTooLarge => Err(SourceError::RangeTooLarge { from: 1, to: 2 }),
                Outcome::NotFound => Err(SourceError::BlockNotFound { number: 1 }),
            }
        }
    }

    #[async_trait]
    impl ChainSource for MockSource {
        async fn latest_block(&self) -> Result<u64, SourceError> {
            self.as_outcome()
        }
        async fn finalized_block(&self) -> Result<u64, SourceError> {
            self.as_outcome()
        }
        async fn fetch_block(&self, _: u64) -> Result<BlockUnit, SourceError> {
            // Only the error path is exercised in these tests; a success would
            // need a full BlockUnit, which latest_block covers more cheaply.
            self.as_outcome().map(|_| unreachable!())
        }
        async fn fetch_logs(&self, _: u64, _: u64) -> Result<Vec<RawLog>, SourceError> {
            self.as_outcome().map(|_| Vec::new())
        }
        async fn block_hash(&self, _: u64) -> Result<Hash32, SourceError> {
            self.as_outcome().map(|_| [0u8; 32])
        }
        fn finality_depth(&self) -> u64 {
            64
        }
    }

    fn pool(members: Vec<Arc<MockSource>>) -> PooledSource {
        PooledSource::new(
            members
                .into_iter()
                .enumerate()
                .map(|(i, m)| (m as Arc<dyn ChainSource>, format!("mock{i}")))
                .collect(),
        )
    }

    /// The headline acceptance criterion: first endpoint always rate-limits,
    /// reads still succeed via the second.
    #[tokio::test]
    async fn a_rate_limited_first_endpoint_is_bypassed() {
        let bad = MockSource::always(Outcome::Transient);
        let good = MockSource::always(Outcome::Ok(123));
        let p = pool(vec![bad.clone(), good.clone()]);

        assert_eq!(p.latest_block().await.unwrap(), 123);
        assert!(bad.call_count() >= 1, "the bad endpoint should have been tried");
        assert!(good.call_count() >= 1, "the good endpoint should have answered");
    }

    /// A Fatal is never masked by failover — it stops at the first provider.
    #[tokio::test]
    async fn a_fatal_error_is_surfaced_not_masked() {
        let fatal = MockSource::always(Outcome::Fatal);
        let good = MockSource::always(Outcome::Ok(9));
        let p = pool(vec![fatal.clone(), good.clone()]);

        let err = p.latest_block().await.unwrap_err();
        assert!(matches!(err, SourceError::Fatal(_)));
        assert_eq!(good.call_count(), 0, "failover must not reach past a Fatal");
    }

    /// RangeTooLarge belongs to the chunker, not to failover — surfaced as-is.
    #[tokio::test]
    async fn range_too_large_is_surfaced_for_the_chunker() {
        let a = MockSource::always(Outcome::RangeTooLarge);
        let b = MockSource::always(Outcome::Ok(1));
        let p = pool(vec![a.clone(), b.clone()]);

        let err = p.fetch_logs(100, 5000).await.unwrap_err();
        assert!(matches!(err, SourceError::RangeTooLarge { .. }));
        assert_eq!(b.call_count(), 0, "a wide range is every provider's answer");
    }

    /// BlockNotFound is not a failover case either.
    #[tokio::test]
    async fn block_not_found_is_surfaced_not_retried_elsewhere() {
        let a = MockSource::always(Outcome::NotFound);
        let b = MockSource::always(Outcome::Ok(1));
        let p = pool(vec![a.clone(), b.clone()]);

        let err = p.block_hash(42).await.unwrap_err();
        assert!(matches!(err, SourceError::BlockNotFound { .. }));
        assert_eq!(b.call_count(), 0);
    }

    /// When every provider is transiently down the pool reports Transient, so
    /// the caller's own backoff can decide to wait — it does not invent a Fatal.
    #[tokio::test]
    async fn all_down_reports_transient() {
        let a = MockSource::always(Outcome::Transient);
        let b = MockSource::always(Outcome::Transient);
        let p = pool(vec![a, b]);

        let err = p.latest_block().await.unwrap_err();
        assert!(matches!(err, SourceError::Transient(_)));
        assert!(err.is_retryable());
    }

    /// A wedged endpoint is skipped once it crosses the failure threshold: the
    /// pool stops leading with it and it stops accruing calls on every request.
    #[tokio::test]
    async fn a_wedged_endpoint_stops_being_tried_first() {
        let bad = MockSource::always(Outcome::Transient);
        let good = MockSource::always(Outcome::Ok(7));
        let p = pool(vec![bad.clone(), good.clone()]);

        // Two calls push the bad endpoint over the wedge threshold (2).
        p.latest_block().await.unwrap();
        p.latest_block().await.unwrap();
        let bad_after_wedge = bad.call_count();

        // Subsequent calls should go straight to the good endpoint and leave the
        // wedged one alone.
        p.latest_block().await.unwrap();
        p.latest_block().await.unwrap();
        assert_eq!(
            bad.call_count(),
            bad_after_wedge,
            "a wedged endpoint must not be hit again while a healthy one exists"
        );
        assert!(good.call_count() >= 4);
    }

    /// A transient failure bumps the failing provider's count and leaves the
    /// one that answered at zero.
    #[tokio::test]
    async fn a_transient_failure_is_recorded_against_the_failing_provider() {
        let flaky = MockSource::always(Outcome::Transient);
        let good = MockSource::always(Outcome::Ok(5));
        let p = pool(vec![flaky, good]);

        p.latest_block().await.unwrap();
        assert_eq!(p.failures_for_test(0), 1, "the failing provider accrues a strike");
        assert_eq!(p.failures_for_test(1), 0, "the provider that answered stays clean");
    }

    /// A recovered endpoint's count returns to zero on its next success, which
    /// is what lets a provider rejoin the healthy set after a rough patch.
    #[tokio::test]
    async fn a_success_resets_the_failure_count() {
        // A single provider that fails once, then answers. Forcing it to be
        // retried (there is no backup) is what exercises the reset.
        let flaky = MockSource::new(vec![Outcome::Transient, Outcome::Ok(5)]);
        let p = pool(vec![flaky]);

        // First call: the only provider is transiently down, so the pool reports
        // Transient and the strike is recorded.
        assert!(p.latest_block().await.is_err());
        assert_eq!(p.failures_for_test(0), 1);

        // Second call: it answers, and the strike is cleared.
        assert_eq!(p.latest_block().await.unwrap(), 5);
        assert_eq!(p.failures_for_test(0), 0, "a success must clear the failure count");
    }

    // -----------------------------------------------------------------------
    // Network test, ignored by default so an offline machine still passes:
    //   cargo test -p chainscope-eth-source pool -- --ignored --nocapture
    // -----------------------------------------------------------------------

    /// The acceptance criterion against the real thing: put a dead endpoint
    /// first and a live free endpoint second, and the pool still answers by
    /// failing over. Uses the same keyless free endpoints the EthSource tests
    /// probe for; no key, no paid tier.
    #[tokio::test]
    #[ignore = "requires network"]
    async fn failover_over_a_dead_endpoint_to_a_live_one() {
        let dead: url::Url = "http://127.0.0.1:1/".parse().unwrap(); // nothing listens here
        let live: url::Url = "https://eth.drpc.org".parse().unwrap();

        // No watched addresses needed to ask for the head.
        let p = PooledSource::from_endpoints(&[dead, live], &[]);

        let tip = p.latest_block().await.expect("pool should answer via the live endpoint");
        assert!(tip > 0, "a real chain head is non-zero");

        // The dead endpoint (index 0) took the strike; the live one carried it.
        assert!(p.failures_for_test(0) >= 1, "the dead endpoint should have failed");
        println!("failover OK: live chain head via the second endpoint = {tip}");
    }
}
