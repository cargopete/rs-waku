//! Lightweight atomic counters for node observability, exposed via the REST
//! `/metrics` endpoint as Prometheus counters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct Inner {
    relay_messages: AtomicU64,
    store_queries: AtomicU64,
    lightpush_requests: AtomicU64,
    filter_pushes: AtomicU64,
    rate_limited: AtomicU64,
}

/// Cheaply-cloneable handle to the node's metric counters.
#[derive(Clone, Default)]
pub struct Metrics {
    inner: Arc<Inner>,
}

/// A point-in-time read of all counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct MetricsSnapshot {
    pub relay_messages: u64,
    pub store_queries: u64,
    pub lightpush_requests: u64,
    pub filter_pushes: u64,
    pub rate_limited: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn relay_message(&self) {
        self.inner.relay_messages.fetch_add(1, Ordering::Relaxed);
    }
    pub fn store_query(&self) {
        self.inner.store_queries.fetch_add(1, Ordering::Relaxed);
    }
    pub fn lightpush_request(&self) {
        self.inner
            .lightpush_requests
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn filter_pushes_add(&self, n: u64) {
        self.inner.filter_pushes.fetch_add(n, Ordering::Relaxed);
    }
    pub fn rate_limited(&self) {
        self.inner.rate_limited.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            relay_messages: self.inner.relay_messages.load(Ordering::Relaxed),
            store_queries: self.inner.store_queries.load(Ordering::Relaxed),
            lightpush_requests: self.inner.lightpush_requests.load(Ordering::Relaxed),
            filter_pushes: self.inner.filter_pushes.load(Ordering::Relaxed),
            rate_limited: self.inner.rate_limited.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        let m = Metrics::new();
        m.relay_message();
        m.relay_message();
        m.store_query();
        m.filter_pushes_add(3);
        m.rate_limited();
        let s = m.snapshot();
        assert_eq!(s.relay_messages, 2);
        assert_eq!(s.store_queries, 1);
        assert_eq!(s.filter_pushes, 3);
        assert_eq!(s.rate_limited, 1);
        assert_eq!(s.lightpush_requests, 0);
    }
}
