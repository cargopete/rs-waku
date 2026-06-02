//! # waku-discv5 — 33/WAKU2-DISCV5 + EIP-1459 DNS discovery
//!
//! Two discovery mechanisms:
//!   1. a Waku-isolated discv5 DHT (wraps `sigp/discv5`), with a Waku-specific
//!      protocol id so we land in the Waku network, NOT Ethereum's, and shard
//!      filtering via the ENR `rs` field;
//!   2. an EIP-1459 DNS-discovery client (the Merkle-tree TXT resolver) for
//!      bootstrap — this piece is NOT provided by `sigp/discv5`.
//!
//! **Milestone 1.** TODO: wrap discv5 with Waku protocol-id isolation + shard
//! filtering; implement the enrtree TXT resolver and feed results to the peer
//! manager. Default bootstrap: [`waku_core::preset::TWN`]`.dns_discovery_enrtree`.

/// An `enrtree://` URL to resolve via EIP-1459 DNS discovery.
#[derive(Clone, Debug)]
pub struct EnrTreeUrl(pub String);
