//! A tiny hot-stats cache.
//!
//! Only the small, shared, recompute-heavy responses go here — the leaderboard
//! and the status heartbeat. Many clients poll those same few keys, so caching
//! them for a few seconds keeps that load off Postgres and is how the p99 target
//! holds. Keyset-paginated endpoints are already O(limit) fast and their key
//! space is effectively unbounded, so they are never cached.
//!
//! Two properties matter:
//!   * **TTL** — an entry serves for `ttl`, then the next reader recomputes it.
//!   * **single-flight** — under a miss, exactly one caller recomputes while the
//!     rest wait on it, so a burst of traffic on a cold key does not stampede the
//!     database with identical queries.
//!
//! A `ttl` of zero disables caching (always recompute), which the correctness
//! tests use so they observe live data.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::ApiError;

/// The cached bytes for one key, guarded by an async mutex so a recompute is
/// single-flight.
#[derive(Default)]
struct Slot {
    cached: Option<(Instant, Arc<Vec<u8>>)>,
}

pub struct Cache {
    ttl: Duration,
    slots: Mutex<HashMap<String, Arc<tokio::sync::Mutex<Slot>>>>,
}

impl Cache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// Return `key`'s cached bytes if fresh, else compute them once (blocking
    /// concurrent callers for the same key) and cache the result.
    pub async fn get_or_compute<F, Fut>(
        &self,
        key: &str,
        compute: F,
    ) -> Result<Arc<Vec<u8>>, ApiError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>, ApiError>>,
    {
        // Per-key async mutex, created once under the map lock.
        let slot = {
            let mut map = self.slots.lock().unwrap();
            map.entry(key.to_owned()).or_default().clone()
        };

        // Only one recompute per key proceeds past here at a time.
        let mut guard = slot.lock().await;

        if self.ttl > Duration::ZERO {
            if let Some((at, bytes)) = &guard.cached {
                if at.elapsed() < self.ttl {
                    return Ok(bytes.clone());
                }
            }
        }

        let bytes = Arc::new(compute().await?);
        if self.ttl > Duration::ZERO {
            guard.cached = Some((Instant::now(), bytes.clone()));
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn count(cache: &Cache, key: &str, calls: &AtomicUsize) -> Arc<Vec<u8>> {
        cache
            .get_or_compute(key, || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(b"value".to_vec())
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_fresh_entry_is_served_without_recomputing() {
        let cache = Cache::new(Duration::from_secs(60));
        let calls = AtomicUsize::new(0);
        count(&cache, "k", &calls).await;
        count(&cache, "k", &calls).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "second read hit the cache");
    }

    #[tokio::test]
    async fn a_zero_ttl_disables_caching() {
        let cache = Cache::new(Duration::ZERO);
        let calls = AtomicUsize::new(0);
        count(&cache, "k", &calls).await;
        count(&cache, "k", &calls).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2, "ttl 0 always recomputes");
    }

    #[tokio::test]
    async fn an_expired_entry_recomputes() {
        let cache = Cache::new(Duration::from_millis(20));
        let calls = AtomicUsize::new(0);
        count(&cache, "k", &calls).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        count(&cache, "k", &calls).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2, "stale entry recomputed after TTL");
    }

    #[tokio::test]
    async fn concurrent_misses_recompute_once() {
        let cache = Arc::new(Cache::new(Duration::from_secs(60)));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let c = cache.clone();
            let n = calls.clone();
            handles.push(tokio::spawn(async move {
                c.get_or_compute("k", || async {
                    // Hold the compute long enough that every task piles up behind it.
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    n.fetch_add(1, Ordering::SeqCst);
                    Ok(b"v".to_vec())
                })
                .await
                .unwrap()
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "single-flight: one recompute for the burst");
    }
}
