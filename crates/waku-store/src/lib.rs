//! # waku-store — 13/WAKU2-STORE v3 (`/vac/waku/store-query/3.0.0`)
//!
//! Historical message storage and query, indexed by the RFC-14 message hash.
//! v3 can return hashes only (`include_data = false`), which underpins p2p
//! reliability ("did the network see hash X?").
//!
//! Design: a [`MessageStore`] trait behind two `sqlx` backends — SQLite
//! (embedded default) and PostgreSQL (production, partitioned). A write path fed
//! by the relay validator (store-on-relay) and a v3 query server behaviour.
//! Store-Sync (Negentropy / range-based set reconciliation) is a separate
//! behaviour between store nodes — no ready Rust crate, port from `waku-org/negentropy`.
//!
//! 64/WAKU2-NETWORK: store ≥ 12 h per supported shard; SHOULD only store
//! messages with a valid RLN proof.
//!
//! **Milestone 3.**

use async_trait::async_trait;
use waku_core::{MessageHash, WakuMessage};

/// Storage backend abstraction. Implemented by SQLite and Postgres backends.
#[async_trait]
pub trait MessageStore: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persist a message received on `pubsub_topic` at `receiver_time_ns`.
    async fn put(
        &self,
        pubsub_topic: &str,
        msg: &WakuMessage,
        receiver_time_ns: i64,
    ) -> Result<(), Self::Error>;

    /// Look up a single message by its deterministic hash.
    async fn get(&self, hash: &MessageHash) -> Result<Option<WakuMessage>, Self::Error>;
}
