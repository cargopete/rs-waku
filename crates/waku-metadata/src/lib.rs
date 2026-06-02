//! # waku-metadata — 66/WAKU2-METADATA (`/vac/waku/metadata/1.0.0`)
//!
//! Request/response handshake exchanging `cluster_id` + supported `shards`.
//! A node MUST disconnect peers whose `cluster_id` mismatches.
//!
//! **Milestone 1.** TODO: implement as a libp2p request-response behaviour;
//! enforce cluster-id mismatch disconnect; feed shard info to the peer manager.

pub use waku_core::preset::METADATA_PROTOCOL_ID as PROTOCOL_ID;

/// Payload of a metadata request/response (mirrors nwaku's `WakuMetadataResponse`).
#[derive(Clone, Debug, Default)]
pub struct Metadata {
    pub cluster_id: Option<u32>,
    pub shards: Vec<u32>,
}
