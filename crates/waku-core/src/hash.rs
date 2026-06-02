//! RFC-14 deterministic message hash.
//!
//! This single 32-byte value is used in THREE interop-critical places:
//!   1. the gossipsub `message_id` (11/WAKU2-RELAY) — see `waku-relay`;
//!   2. the 13/WAKU2-STORE v3 index / cursor;
//!   3. p2p reliability ("did the network see hash X?").
//!
//! Spec layout:
//! ```text
//! message_hash = sha256( pubsub_topic
//!                      || payload
//!                      || content_topic
//!                      || meta          (omitted if absent)
//!                      || timestamp )   (omitted if absent; 8-byte big-endian)
//! ```
//!
//! ⚠ INTEROP CAVEAT: the exact byte layout (field order, omission of absent
//! optionals, timestamp encoding) must be verified byte-for-byte against
//! nwaku `waku/waku_core/message/digest.nim` and go-waku before we rely on it.
//! A one-byte difference silently breaks the mesh. Tests below assert structure,
//! not (yet) a real nwaku-produced vector — drop one in and un-`ignore` it.

use sha2::{Digest, Sha256};

use crate::message::WakuMessage;

/// 32-byte deterministic Waku message hash.
pub type MessageHash = [u8; 32];

/// Compute the RFC-14 deterministic hash for `msg` published on `pubsub_topic`.
pub fn deterministic_hash(pubsub_topic: &str, msg: &WakuMessage) -> MessageHash {
    let mut h = Sha256::new();
    h.update(pubsub_topic.as_bytes());
    h.update(&msg.payload);
    h.update(msg.content_topic.as_bytes());
    if let Some(meta) = &msg.meta {
        h.update(meta);
    }
    if let Some(ts) = msg.timestamp {
        // Big-endian, 8 bytes. Absent timestamp is omitted entirely.
        h.update(ts.to_be_bytes());
    }
    h.finalize().into()
}

/// Lowercase hex of a message hash, matching the 64-char ids seen in nwaku logs.
pub fn hash_hex(hash: &MessageHash) -> String {
    let mut s = String::with_capacity(64);
    for b in hash {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic_and_field_sensitive() {
        let topic = "/waku/2/rs/1/0";
        let a = WakuMessage::new("/toychat/2/huilong/proto", b"hello".to_vec());
        let b = WakuMessage::new("/toychat/2/huilong/proto", b"hello".to_vec());
        assert_eq!(deterministic_hash(topic, &a), deterministic_hash(topic, &b));

        let mut c = a.clone();
        c.payload = b"world".to_vec();
        assert_ne!(deterministic_hash(topic, &a), deterministic_hash(topic, &c));

        // Pubsub topic participates in the hash.
        assert_ne!(
            deterministic_hash(topic, &a),
            deterministic_hash("/waku/2/rs/1/1", &a)
        );
    }

    #[test]
    fn optional_fields_change_the_hash_when_present() {
        let topic = "/waku/2/rs/1/0";
        let bare = WakuMessage::new("/a/1/b/proto", b"x".to_vec());

        let mut with_meta = bare.clone();
        with_meta.meta = Some(b"m".to_vec());
        assert_ne!(
            deterministic_hash(topic, &bare),
            deterministic_hash(topic, &with_meta)
        );

        let mut with_ts = bare.clone();
        with_ts.timestamp = Some(1_700_000_000_000_000_000);
        assert_ne!(
            deterministic_hash(topic, &bare),
            deterministic_hash(topic, &with_ts)
        );
    }

    #[test]
    #[ignore = "needs a real hash vector captured from nwaku"]
    fn matches_nwaku_reference_vector() {
        // TODO: paste a (pubsub_topic, message, expected_hash_hex) triple produced
        // by nwaku and assert exact equality here. This is the gate for Milestone 1.
        let topic = "/waku/2/rs/1/0";
        let msg = WakuMessage::new("/toychat/2/huilong/proto", b"hello".to_vec());
        let expected = "0000000000000000000000000000000000000000000000000000000000000000";
        assert_eq!(hash_hex(&deterministic_hash(topic, &msg)), expected);
    }
}
