//! # waku-relay — 11/WAKU2-RELAY (`/vac/waku/relay/2.0.0`)
//!
//! Gossipsub v1.1 pub/sub with the interop-critical Waku configuration:
//!
//! - **message-id fn** = the RFC-14 [`deterministic_hash`](waku_core::deterministic_hash),
//!   NOT the gossipsub default. Getting this wrong ⇒ IHAVE/IWANT ids mismatch the
//!   mesh ⇒ nwaku treats us as a faulty peer and prunes us.
//! - **StrictNoSign**: [`ValidationMode::Anonymous`] + [`MessageAuthenticity::Anonymous`];
//!   never sign, never populate `from`/`seqno` (publisher unlinkability).
//! - **mesh/scoring params** seeded from go-libp2p-pubsub defaults. ⚠ These still
//!   need a field-by-field reconciliation against go-waku's `GossipSubParams`
//!   before we trust scoring under load — see `TODO(scoring)` below.
//!
//! Manual validation is enabled (`validate_messages`): the swarm surfaces each
//! inbound message for a verdict, which [`validation`] computes and the node
//! reports back. The RLN proof check that feeds it lives in `waku-rln`.

pub mod validation;

pub use validation::{validate, MessageFacts, RlnStatus, Validation, ValidationPolicy};

use std::time::Duration;

use libp2p::gossipsub::{self, ConfigBuilder, MessageAuthenticity, MessageId, ValidationMode};
use prost::Message as _;
use sha2::{Digest, Sha256};
use thiserror::Error;
use waku_core::{deterministic_hash, ShardId, WakuMessage};

pub use waku_core::preset::RELAY_PROTOCOL_ID as PROTOCOL_ID;

#[derive(Debug, Error)]
pub enum RelayError {
    #[error("gossipsub config invalid: {0}")]
    Config(String),
    #[error("gossipsub behaviour init failed: {0}")]
    Build(String),
}

/// The Waku gossipsub message-id: the RFC-14 deterministic hash of the
/// WakuMessage carried in `message.data`, on the pubsub topic in `message.topic`.
///
/// Malformed payloads (that fail to decode as a WakuMessage) fall back to
/// `sha256(data)` so they still receive a stable id rather than panicking.
pub fn waku_message_id(message: &gossipsub::Message) -> MessageId {
    let topic = message.topic.as_str();
    match WakuMessage::decode(message.data.as_slice()) {
        Ok(msg) => MessageId::from(deterministic_hash(topic, &msg).to_vec()),
        Err(_) => {
            let mut h = Sha256::new();
            h.update(&message.data);
            MessageId::from(h.finalize().to_vec())
        }
    }
}

/// Build the Waku gossipsub [`Config`](gossipsub::Config).
pub fn relay_config() -> Result<gossipsub::Config, RelayError> {
    ConfigBuilder::default()
        // StrictNoSign — no author, no signature, no seqno.
        .validation_mode(ValidationMode::Anonymous)
        // THE interop-critical hook.
        .message_id_fn(waku_message_id)
        // Manual validation: the node reports Accept/Reject/Ignore (see `validation`).
        .validate_messages()
        // 1 MiB transmit ceiling (libp2p/go default). Waku's 150 KiB message
        // limit is enforced separately at the validation layer.
        .max_transmit_size(1024 * 1024)
        // go-libp2p-pubsub mesh defaults: D=6, Dlo=5, Dhi=12, Dout=2.
        .mesh_n(6)
        .mesh_n_low(5)
        .mesh_n_high(12)
        .mesh_outbound_min(2)
        .heartbeat_interval(Duration::from_secs(1))
        .history_length(5)
        .history_gossip(3)
        .fanout_ttl(Duration::from_secs(60))
        // go default seenTTL = 2 minutes.
        .duplicate_cache_time(Duration::from_secs(120))
        // go-waku enables flood publishing.
        .flood_publish(true)
        // TODO(scoring): replicate go-waku `GossipSubParams` peer-scoring
        // thresholds + topic params field-by-field before trusting scoring.
        .build()
        .map_err(|e| RelayError::Config(e.to_string()))
}

/// Build a configured Waku relay [`gossipsub::Behaviour`].
///
/// Uses anonymous authenticity (StrictNoSign), so no keypair is required.
pub fn build_relay() -> Result<gossipsub::Behaviour, RelayError> {
    gossipsub::Behaviour::new(MessageAuthenticity::Anonymous, relay_config()?)
        .map_err(|e| RelayError::Build(e.to_string()))
}

/// The libp2p [`IdentTopic`](gossipsub::IdentTopic) for a shard — its hash is the
/// raw `/waku/2/rs/<cluster>/<shard>` string (Waku uses identity-hash topics).
pub fn shard_topic(shard: ShardId) -> gossipsub::IdentTopic {
    gossipsub::IdentTopic::new(shard.pubsub_topic())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_builds() {
        relay_config().expect("waku relay config must be valid");
    }

    #[test]
    fn message_id_matches_rfc14_hash() {
        let shard = ShardId::new(1, 0);
        let topic = gossipsub::IdentTopic::new(shard.pubsub_topic());
        let msg = WakuMessage::new("/toychat/2/huilong/proto", b"hello".to_vec());

        // Reconstruct the gossipsub::Message the mesh would see.
        let gmsg = gossipsub::Message {
            source: None,
            data: msg.encode_to_vec(),
            sequence_number: None,
            topic: topic.hash(),
        };

        let expected = deterministic_hash(&shard.pubsub_topic(), &msg);
        assert_eq!(waku_message_id(&gmsg).0, expected.to_vec());
    }
}
