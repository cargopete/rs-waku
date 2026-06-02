//! # waku-relay — 11/WAKU2-RELAY (`/vac/waku/relay/2.0.0`)
//!
//! Gossipsub v1.1 pub/sub. The interop-critical configuration lives here:
//!
//! - **message-id fn** = the RFC-14 [`deterministic_hash`](waku_core::deterministic_hash),
//!   NOT the gossipsub default. Install via `gossipsub::ConfigBuilder::message_id_fn`.
//!   Getting this wrong ⇒ IHAVE/IWANT ids mismatch the mesh ⇒ treated as faulty.
//! - **StrictNoSign**: `ValidationMode::Anonymous` + `MessageAuthenticity::Anonymous`;
//!   never sign, never populate `from`/`seqno` (publisher unlinkability).
//! - **scoring/mesh params** replicated field-by-field from go-libp2p-pubsub /
//!   go-waku `GossipSubParams` — the single highest-effort interop area.
//! - **manual validation**: enable `validate_messages()` so `waku-rln` can run the
//!   RLN proof check, then `report_message_validation_result()` (Accept/Reject/Ignore).
//!
//! **Milestone 1** (de-risk first). TODO: build the gossipsub Behaviour wrapper,
//! the message-id closure, the scoring params, and the RLN validator seam.

pub use waku_core::preset::RELAY_PROTOCOL_ID as PROTOCOL_ID;

/// Validation outcome for an inbound relay message (gossipsub v1.1 semantics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Validation {
    /// Forward and (optionally) store.
    Accept,
    /// Drop and penalize the sender's score.
    Reject,
    /// Drop without penalty (e.g. no RLN proof on a saturated shard).
    Ignore,
}
