//! 14/WAKU2-MESSAGE — the WakuMessage wire format.
//!
//! Defined with `prost`'s derive so we need no `protoc` at build time. Field
//! numbers MUST match nwaku/go-waku's `waku_message.proto`; they are the
//! interop contract.
//!
//! Reference: <https://rfc.vac.dev/spec/14/> (mirrored at lip.logos.co).

/// A Waku message as it travels over 11/WAKU2-RELAY and is stored by 13/WAKU2-STORE.
///
/// Note: `pubsub_topic` is deliberately NOT part of this struct — it belongs to
/// the gossipsub layer and is supplied separately to [`crate::hash::deterministic_hash`].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WakuMessage {
    /// Application payload (opaque bytes; may itself be encrypted).
    #[prost(bytes = "vec", tag = "1")]
    pub payload: Vec<u8>,

    /// Content topic: `/<app>/<version>/<name>/<encoding>` (23/WAKU2-TOPICS).
    #[prost(string, tag = "2")]
    pub content_topic: String,

    /// Optional payload-encoding version.
    #[prost(uint32, optional, tag = "3")]
    pub version: Option<u32>,

    /// Unix timestamp in **nanoseconds**. Absent ⇒ omitted from the hash.
    #[prost(int64, optional, tag = "10")]
    pub timestamp: Option<i64>,

    /// Optional application-defined metadata. Absent ⇒ omitted from the hash.
    #[prost(bytes = "vec", optional, tag = "11")]
    pub meta: Option<Vec<u8>>,

    /// Serialized RLN rate-limit proof (17/WAKU2-RLN-RELAY). Opaque here;
    /// decoded/verified by `waku-rln`.
    #[prost(bytes = "vec", optional, tag = "21")]
    pub rate_limit_proof: Option<Vec<u8>>,

    /// If true the message is not stored by 13/WAKU2-STORE.
    #[prost(bool, optional, tag = "31")]
    pub ephemeral: Option<bool>,
}

impl WakuMessage {
    /// Construct a minimal message for a content topic and payload.
    pub fn new(content_topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: payload.into(),
            content_topic: content_topic.into(),
            version: None,
            timestamp: None,
            meta: None,
            rate_limit_proof: None,
            ephemeral: None,
        }
    }
}
