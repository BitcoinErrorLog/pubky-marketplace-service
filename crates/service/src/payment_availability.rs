use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use tokio::sync::{Mutex, RwLock};

use crate::clock::Clock;

#[derive(Clone, Debug)]
struct CacheEntry {
    value: bool,
    fetched_at: Option<DateTime<Utc>>,
    last_ok_at: Option<DateTime<Utc>>,
    refresh: Arc<Mutex<()>>,
}

impl Default for CacheEntry {
    fn default() -> Self {
        Self {
            value: false,
            fetched_at: None,
            last_ok_at: None,
            refresh: Arc::new(Mutex::new(())),
        }
    }
}

/// Server-side cache for Paykit rail and seller claim availability.
#[derive(Clone, Debug, Default)]
pub struct PaymentAvailabilityCache {
    rail: Arc<RwLock<CacheEntry>>,
    sellers: Arc<RwLock<HashMap<String, CacheEntry>>>,
}

impl PaymentAvailabilityCache {
    /// Reads rail availability, refreshing at most once per TTL.
    pub async fn rail_health<F, Fut, E>(
        &self,
        clock: &dyn Clock,
        ttl_seconds: i64,
        stale_seconds: i64,
        fetch: F,
    ) -> bool
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool, E>>,
        E: std::fmt::Debug,
    {
        self.read_or_refresh(&self.rail, clock, ttl_seconds, stale_seconds, fetch)
            .await
    }

    /// Reads one seller's claim status, refreshing at most once per TTL.
    pub async fn seller_claimed<F, Fut, E>(
        &self,
        seller: &str,
        clock: &dyn Clock,
        ttl_seconds: i64,
        stale_seconds: i64,
        fetch: F,
    ) -> bool
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool, E>>,
        E: std::fmt::Debug,
    {
        let entry = {
            let sellers = self.sellers.read().await;
            sellers.get(seller).cloned()
        };
        let entry = match entry {
            Some(entry) => entry,
            None => {
                let mut sellers = self.sellers.write().await;
                sellers
                    .entry(seller.to_string())
                    .or_insert_with(CacheEntry::default)
                    .clone()
            }
        };
        if is_fresh(&entry, clock.now(), ttl_seconds) {
            return entry.value;
        }

        let _guard = entry.refresh.lock().await;
        let entry = {
            let sellers = self.sellers.read().await;
            sellers.get(seller).cloned().unwrap_or_default()
        };
        let now = clock.now();
        if is_fresh(&entry, now, ttl_seconds) {
            return entry.value;
        }
        let result = fetch().await;
        let updated = refreshed(entry, clock.now(), stale_seconds, result);
        let value = updated.value;
        self.sellers
            .write()
            .await
            .insert(seller.to_string(), updated);
        value
    }

    /// Returns the last successful rail value and its age for the operator
    /// health surface. A rail that has never succeeded is reported as absent.
    pub async fn rail_snapshot(&self, now: DateTime<Utc>) -> (Option<bool>, Option<i64>) {
        let entry = self.rail.read().await;
        (
            entry.last_ok_at.map(|_| entry.value),
            entry.last_ok_at.map(|at| (now - at).num_seconds()),
        )
    }

    /// Removes seller entries that are no longer within the serving window.
    pub async fn evict_stale(&self, now: DateTime<Utc>, stale_seconds: i64) {
        self.sellers.write().await.retain(|_, entry| {
            Arc::strong_count(&entry.refresh) > 1
                || entry.fetched_at.is_some_and(|fetched_at| {
                    now - fetched_at <= Duration::seconds(stale_seconds)
                        && Arc::strong_count(&entry.refresh) == 1
                })
        });
    }

    async fn read_or_refresh<F, Fut, E>(
        &self,
        cache: &RwLock<CacheEntry>,
        clock: &dyn Clock,
        ttl_seconds: i64,
        stale_seconds: i64,
        fetch: F,
    ) -> bool
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool, E>>,
        E: std::fmt::Debug,
    {
        let entry = cache.read().await.clone();
        if is_fresh(&entry, clock.now(), ttl_seconds) {
            return entry.value;
        }

        let _guard = entry.refresh.lock().await;
        let entry = cache.read().await.clone();
        let now = clock.now();
        if is_fresh(&entry, now, ttl_seconds) {
            return entry.value;
        }
        let result = fetch().await;
        let updated = refreshed(entry, clock.now(), stale_seconds, result);
        let value = updated.value;
        *cache.write().await = updated;
        value
    }
}

fn is_fresh(entry: &CacheEntry, now: DateTime<Utc>, ttl_seconds: i64) -> bool {
    entry
        .fetched_at
        .is_some_and(|at| now - at < Duration::seconds(ttl_seconds))
}

fn refreshed(
    mut entry: CacheEntry,
    now: DateTime<Utc>,
    stale_seconds: i64,
    result: Result<bool, impl std::fmt::Debug>,
) -> CacheEntry {
    entry.fetched_at = Some(now);
    match result {
        Ok(value) => {
            entry.value = value;
            entry.last_ok_at = Some(now);
        }
        Err(error) => {
            let age = entry
                .last_ok_at
                .map(|at| (now - at).num_seconds())
                .unwrap_or(stale_seconds);
            tracing::warn!(
                error = ?error,
                last_ok_age_seconds = age,
                value = entry.value,
                "paykit availability refresh failed; serving cached value"
            );
            entry.value = entry
                .last_ok_at
                .filter(|at| now - *at <= Duration::seconds(stale_seconds))
                .map(|_| entry.value)
                .unwrap_or(false);
        }
    }
    entry
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use chrono::{TimeZone, Utc};
    use tokio::sync::oneshot;

    use crate::clock::{AdjustableClock, Clock};

    use super::PaymentAvailabilityCache;

    fn now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    #[tokio::test]
    async fn failed_refresh_serves_last_value_until_stale() {
        let cache = PaymentAvailabilityCache::default();
        let clock = AdjustableClock::new(now());
        assert!(
            cache
                .rail_health(&clock, 15, 60, || async { Ok::<_, &str>(true) })
                .await
        );
        clock.advance_seconds(16);
        assert!(
            cache
                .rail_health(&clock, 15, 60, || async { Err::<bool, _>("down") })
                .await
        );
        clock.advance_seconds(61);
        assert!(
            !cache
                .rail_health(&clock, 15, 60, || async { Err::<bool, _>("down") })
                .await
        );
    }

    #[tokio::test]
    async fn concurrent_cold_reads_refresh_once() {
        let cache = PaymentAvailabilityCache::default();
        let clock = Arc::new(AdjustableClock::new(now()));
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let reads = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let calls = calls.clone();
                let clock = clock.clone();
                async move {
                    cache
                        .rail_health(clock.as_ref(), 15, 60, || async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, &str>(true)
                        })
                        .await
                }
            })
            .collect::<Vec<_>>();
        assert!(futures_join(reads).await.into_iter().all(|value| value));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn seller_entries_can_be_evicted_when_stale() {
        let cache = PaymentAvailabilityCache::default();
        let clock = AdjustableClock::new(now());
        assert!(
            cache
                .seller_claimed("seller", &clock, 15, 60, || async { Ok::<_, &str>(true) })
                .await
        );
        clock.advance_seconds(61);
        cache.evict_stale(clock.now(), 60).await;
        assert_eq!(cache.sellers.read().await.len(), 0);
    }

    #[tokio::test]
    async fn queued_waiter_reads_clock_after_single_flight() {
        let cache = PaymentAvailabilityCache::default();
        let clock = Arc::new(AdjustableClock::new(now()));
        let calls = Arc::new(AtomicUsize::new(0));
        let (started, started_wait) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let holder = {
            let cache = cache.clone();
            let clock = clock.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                cache
                    .rail_health(clock.as_ref(), 15, 60, || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.send(()).expect("waiter is starting");
                        wait.await.expect("holder release");
                        Ok::<_, &str>(true)
                    })
                    .await
            })
        };
        started_wait.await.expect("holder started");
        clock.advance_seconds(16);
        let waiter = {
            let cache = cache.clone();
            let clock = clock.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                cache
                    .rail_health(clock.as_ref(), 15, 60, || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, &str>(false)
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        release.send(()).expect("holder is waiting");
        assert!(holder.await.unwrap());
        let waiter_value = waiter.await.unwrap();
        assert!(
            waiter_value,
            "fetch calls: {}",
            calls.load(Ordering::SeqCst)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn eviction_keeps_seller_with_live_refresh() {
        let cache = PaymentAvailabilityCache::default();
        let clock = Arc::new(AdjustableClock::new(now()));
        let calls = Arc::new(AtomicUsize::new(0));
        let (started, started_wait) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let first = {
            let cache = cache.clone();
            let clock = clock.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                cache
                    .seller_claimed("seller", clock.as_ref(), 15, 60, || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.send(()).expect("refresh is running");
                        wait.await.expect("refresh release");
                        Ok::<_, &str>(true)
                    })
                    .await
            })
        };
        started_wait.await.expect("refresh started");
        cache.evict_stale(clock.now(), 60).await;
        assert_eq!(cache.sellers.read().await.len(), 1);

        let second = {
            let cache = cache.clone();
            let clock = clock.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                cache
                    .seller_claimed("seller", clock.as_ref(), 15, 60, || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, &str>(false)
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        release.send(()).expect("refresh is waiting");
        assert!(first.await.unwrap());
        assert!(second.await.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn slow_seller_refresh_does_not_block_rail_refresh() {
        let cache = PaymentAvailabilityCache::default();
        let clock = Arc::new(AdjustableClock::new(now()));
        let (release, wait) = oneshot::channel();
        let seller = {
            let cache = cache.clone();
            let clock = clock.clone();
            tokio::spawn(async move {
                cache
                    .seller_claimed("seller-a", clock.as_ref(), 15, 60, || async move {
                        wait.await.expect("seller release");
                        Ok::<_, &str>(true)
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        let rail = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            cache.rail_health(clock.as_ref(), 15, 60, || async { Ok::<_, &str>(true) }),
        )
        .await
        .expect("rail refresh is independent");
        assert!(rail);
        release.send(()).expect("seller is waiting");
        assert!(seller.await.unwrap());
    }

    async fn futures_join<F>(futures: Vec<F>) -> Vec<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut tasks = tokio::task::JoinSet::new();
        for future in futures {
            tasks.spawn(future);
        }
        let mut outputs = Vec::new();
        while let Some(output) = tasks.join_next().await {
            outputs.push(output.expect("cache test task completes"));
        }
        outputs
    }
}
