//! # waku-core
//!
//! Pure, dependency-light foundation for rs-waku: the [`WakuMessage`] wire type,
//! the RFC-14 [`deterministic_hash`](hash::deterministic_hash), content-topic
//! parsing, autosharding, and network presets. No networking, no async, no ZK —
//! everything here is unit-testable in isolation and shared by every other crate.

pub mod content_topic;
pub mod error;
pub mod hash;
pub mod message;
pub mod preset;
pub mod shard;

pub use content_topic::ContentTopic;
pub use error::{CoreError, Result};
pub use hash::{deterministic_hash, hash_hex, MessageHash};
pub use message::WakuMessage;
pub use preset::{NetworkPreset, TWN};
pub use shard::{autoshard, ShardId};
