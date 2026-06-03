//! # waku-store — 13/WAKU2-STORE v3 (`/vac/waku/store-query/3.0.0`)
//!
//! Historical message storage and query, indexed by the RFC-14 message hash.
//! v3 can return hashes only (`include_data = false`), which underpins p2p
//! reliability ("did the network see hash X?").
//!
//! This crate provides the storage core: a [`MessageStore`] trait and a
//! `sqlx`-backed [`SqliteStore`] (SQLite now; Postgres later behind the same
//! trait). The v3 request/response protobuf wire protocol layers on top.
//!
//! Ordering/cursor: messages are ordered by `(sort_time, message_hash)` where
//! `sort_time = message.timestamp` (falling back to receiver time). The Store v3
//! cursor is a message hash; the server resolves it to continue the page.

mod sqlite;
pub mod store_query;

pub use sqlite::SqliteStore;

use async_trait::async_trait;
use thiserror::Error;
use waku_core::{MessageHash, WakuMessage};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("corrupt stored row: {0}")]
    Corrupt(String),
}

/// A stored message with its index metadata.
#[derive(Clone, Debug)]
pub struct StoredMessage {
    pub hash: MessageHash,
    pub pubsub_topic: String,
    /// `None` when the query requested hashes only (`include_data = false`).
    pub message: Option<WakuMessage>,
    pub receiver_time_ns: i64,
}

/// A 13/WAKU2-STORE v3 query: filter by content topic + time range (or by
/// explicit hashes), paginated with a cursor.
#[derive(Clone, Debug)]
pub struct StoreQuery {
    pub pubsub_topic: Option<String>,
    pub content_topics: Vec<String>,
    /// Inclusive lower bound on `sort_time` (ns).
    pub time_start_ns: Option<i64>,
    /// Exclusive upper bound on `sort_time` (ns).
    pub time_end_ns: Option<i64>,
    /// Look up these exact hashes (QueryByHash). Combined with other filters.
    pub message_hashes: Vec<MessageHash>,
    /// Whether to return full message data or just hashes.
    pub include_data: bool,
    pub page_size: u64,
    /// Ascending by `(sort_time, hash)` when true.
    pub forward: bool,
    /// Resume after this message hash.
    pub cursor: Option<MessageHash>,
}

impl Default for StoreQuery {
    fn default() -> Self {
        Self {
            pubsub_topic: None,
            content_topics: Vec::new(),
            time_start_ns: None,
            time_end_ns: None,
            message_hashes: Vec::new(),
            include_data: true,
            page_size: 20,
            forward: true,
            cursor: None,
        }
    }
}

impl StoreQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn content_topic(mut self, topic: impl Into<String>) -> Self {
        self.content_topics.push(topic.into());
        self
    }

    pub fn time_range(mut self, start_ns: i64, end_ns: i64) -> Self {
        self.time_start_ns = Some(start_ns);
        self.time_end_ns = Some(end_ns);
        self
    }

    pub fn page_size(mut self, n: u64) -> Self {
        self.page_size = n;
        self
    }

    pub fn cursor(mut self, cursor: MessageHash) -> Self {
        self.cursor = Some(cursor);
        self
    }
}

/// One page of query results.
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub messages: Vec<StoredMessage>,
    /// Present when more results remain; pass as the next query's [`StoreQuery::cursor`].
    pub next_cursor: Option<MessageHash>,
}

/// Storage backend abstraction. Implemented by SQLite (and later Postgres).
#[async_trait]
pub trait MessageStore: Send + Sync {
    /// Persist a message received on `pubsub_topic` at `receiver_time_ns`.
    /// Idempotent: storing the same message hash twice is a no-op.
    async fn put(
        &self,
        pubsub_topic: &str,
        msg: &WakuMessage,
        receiver_time_ns: i64,
    ) -> Result<(), StoreError>;

    /// Look up a single message by its deterministic hash.
    async fn get(&self, hash: &MessageHash) -> Result<Option<WakuMessage>, StoreError>;

    /// Run a v3 query, returning one page plus a continuation cursor.
    async fn query(&self, query: &StoreQuery) -> Result<QueryResult, StoreError>;

    /// Of the given hashes, return those present in the store.
    async fn exists(&self, hashes: &[MessageHash]) -> Result<Vec<MessageHash>, StoreError>;

    /// Total number of stored messages.
    async fn message_count(&self) -> Result<u64, StoreError>;
}
