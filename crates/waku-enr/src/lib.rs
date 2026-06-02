//! # waku-enr — 31/WAKU2-ENR
//!
//! Builds and parses Ethereum Node Records (EIP-778) carrying Waku-specific
//! fields: the `rs` shards bitfield and the `waku2` capability byte. Wraps the
//! `enr` crate.
//!
//! **Milestone 1.** TODO: encode/decode the `rs` field (cluster id + shard
//! bitvector) exactly as nwaku does, plus the multiaddrs field for WSS peers.

/// ENR key under which Waku encodes its relay-shard bitfield.
pub const ENR_FIELD_SHARDS: &str = "rs";
/// ENR key for the Waku capability bitfield (relay/store/filter/lightpush).
pub const ENR_FIELD_WAKU2: &str = "waku2";
