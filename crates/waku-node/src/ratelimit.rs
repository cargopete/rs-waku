//! Per-peer token-bucket rate limiting for request/response protocols.
//!
//! Mirrors nwaku's per-protocol `N/period` request rate limits: each peer gets a
//! bucket of `capacity` tokens that refills to full over `period`; an inbound
//! request consumes one token, and requests with no token are rejected.
//!
//! Time is injected (`now`) so the logic is deterministically testable.

use std::collections::HashMap;
use std::time::Instant;

use libp2p::PeerId;

struct Bucket {
    tokens: f64,
    last: Instant,
}

/// A token-bucket rate limiter keyed by peer.
pub struct RateLimiter {
    capacity: f64,
    refill_per_sec: f64,
    buckets: HashMap<PeerId, Bucket>,
}

impl RateLimiter {
    /// Allow `capacity` requests per `period_secs` (with `capacity` burst).
    pub fn new(capacity: u32, period_secs: f64) -> Self {
        let capacity = capacity.max(1) as f64;
        Self {
            capacity,
            refill_per_sec: capacity / period_secs.max(f64::MIN_POSITIVE),
            buckets: HashMap::new(),
        }
    }

    /// Consume a token for `peer` as of `now`; returns whether the request is allowed.
    pub fn allow(&mut self, peer: PeerId, now: Instant) -> bool {
        let cap = self.capacity;
        let refill = self.refill_per_sec;
        let bucket = self.buckets.entry(peer).or_insert(Bucket {
            tokens: cap,
            last: now,
        });
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * refill).min(cap);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Per-protocol request rate limiters for the service protocols.
pub struct RateLimiters {
    pub store: RateLimiter,
    pub lightpush: RateLimiter,
}

impl Default for RateLimiters {
    fn default() -> Self {
        // Conservative defaults: ~10 requests/second per peer, per protocol.
        Self {
            store: RateLimiter::new(10, 1.0),
            lightpush: RateLimiter::new(10, 1.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn token_bucket_limits_and_refills() {
        let mut rl = RateLimiter::new(2, 1.0); // 2 per second, burst 2
        let peer = PeerId::random();
        let t0 = Instant::now();

        assert!(rl.allow(peer, t0)); // 2 -> 1
        assert!(rl.allow(peer, t0)); // 1 -> 0
        assert!(!rl.allow(peer, t0)); // empty -> denied

        // After a full second the bucket has refilled.
        assert!(rl.allow(peer, t0 + Duration::from_secs(1)));
    }

    #[test]
    fn buckets_are_per_peer() {
        let mut rl = RateLimiter::new(1, 1.0);
        let (a, b) = (PeerId::random(), PeerId::random());
        let t0 = Instant::now();
        assert!(rl.allow(a, t0));
        assert!(!rl.allow(a, t0)); // a is out
        assert!(rl.allow(b, t0)); // b has its own bucket
    }
}
