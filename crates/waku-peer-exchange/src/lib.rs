//! # waku-peer-exchange — 34/WAKU2-PEER-EXCHANGE (`/vac/waku/peer-exchange/2.0.0-alpha1`)
//!
//! Request/response peer discovery for resource-restricted nodes that can't run
//! discv5. A node asks a peer for a batch of ENRs.
//!
//! **Milestone 4.**

pub use waku_core::preset::PEER_EXCHANGE_PROTOCOL_ID as PROTOCOL_ID;
