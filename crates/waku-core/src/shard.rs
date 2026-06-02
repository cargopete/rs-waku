//! WAKU2-RELAY-SHARDING — pubsub topics and autosharding.
//!
//! Pubsub topic strings are `/waku/2/rs/<cluster_id>/<shard>`. Named sharding is
//! deprecated; recent nwaku only accepts the `rs` form.
//!
//! Autosharding maps a content topic to a shard within a cluster:
//! ```text
//! bytes = sha256( application || version )
//! value = big-endian u64 of bytes[24..32]
//! shard = value mod shard_count
//! ```
//!
//! ⚠ INTEROP CAVEAT: verify the byte-slice (`[24..32]`) and endianness against
//! nwaku `waku/waku_core/topics/sharding.nim` before relying on cross-impl routing.

use sha2::{Digest, Sha256};

use crate::content_topic::ContentTopic;
use crate::error::{CoreError, Result};

/// A cluster id + shard index pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShardId {
    pub cluster_id: u16,
    pub shard: u16,
}

impl ShardId {
    pub fn new(cluster_id: u16, shard: u16) -> Self {
        Self { cluster_id, shard }
    }

    /// The gossipsub pubsub topic string for this shard.
    pub fn pubsub_topic(&self) -> String {
        format!("/waku/2/rs/{}/{}", self.cluster_id, self.shard)
    }

    /// Parse a `/waku/2/rs/<cluster>/<shard>` topic string.
    pub fn parse(topic: &str) -> Result<Self> {
        let rest = topic
            .strip_prefix("/waku/2/rs/")
            .ok_or_else(|| CoreError::InvalidPubsubTopic(topic.to_string()))?;
        let (c, s) = rest
            .split_once('/')
            .ok_or_else(|| CoreError::InvalidPubsubTopic(topic.to_string()))?;
        let cluster_id = c
            .parse()
            .map_err(|_| CoreError::InvalidPubsubTopic(topic.to_string()))?;
        let shard = s
            .parse()
            .map_err(|_| CoreError::InvalidPubsubTopic(topic.to_string()))?;
        Ok(Self { cluster_id, shard })
    }
}

/// Compute the autosharding shard index for a content topic.
pub fn autoshard(cluster_id: u16, content_topic: &ContentTopic, shard_count: u16) -> ShardId {
    let mut h = Sha256::new();
    h.update(content_topic.application.as_bytes());
    h.update(content_topic.version.as_bytes());
    let digest = h.finalize();

    let last8: [u8; 8] = digest[24..32].try_into().expect("sha256 is 32 bytes");
    let value = u64::from_be_bytes(last8);
    let shard = (value % shard_count as u64) as u16;
    ShardId::new(cluster_id, shard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pubsub_topic_roundtrip() {
        let s = ShardId::new(1, 3);
        assert_eq!(s.pubsub_topic(), "/waku/2/rs/1/3");
        assert_eq!(ShardId::parse("/waku/2/rs/1/3").unwrap(), s);
        assert!(ShardId::parse("/waku/2/1/3").is_err());
    }

    #[test]
    fn autoshard_is_stable_and_in_range() {
        let ct = ContentTopic::parse("/toychat/2/huilong/proto").unwrap();
        let a = autoshard(1, &ct, 8);
        let b = autoshard(1, &ct, 8);
        assert_eq!(a, b);
        assert!(a.shard < 8);
        assert_eq!(a.cluster_id, 1);
    }
}
