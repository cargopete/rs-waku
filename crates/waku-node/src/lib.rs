//! # waku-node
//!
//! Composes the per-protocol behaviours into a single
//! `#[derive(NetworkBehaviour)]` master behaviour driven by one libp2p `Swarm`
//! on one `tokio` runtime. Owns the peer manager (relay:service connection
//! split, ip-colocation limit), node [`Config`], and lifecycle.
//!
//! Protocol behaviours communicate with service tasks (store DB, RLN prover,
//! chain syncer, REST) over channels. RLN proof generation runs on a blocking
//! pool so it never stalls the swarm.

pub mod metrics;
mod ratelimit;
pub mod runtime;

pub use metrics::{Metrics, MetricsSnapshot};
pub use runtime::{
    spawn, DiscoverySettings, Event, NodeConfig, NodeError, NodeHandle, WakuBehaviour,
};
pub use waku_discv5::WakuEnr;

use waku_core::{NetworkPreset, TWN};

/// Node configuration. Mirrors nwaku CLI semantics (`--cluster-id`, `--shard`,
/// `--rln-relay`, `--store`, `--filter`, `--lightpush`, `--peer-exchange`,
/// `--discv5-discovery`, `--dns-discovery`, …).
#[derive(Clone, Debug)]
pub struct Config {
    pub preset: NetworkPreset,
    /// Shards to subscribe to within the cluster. Empty ⇒ all of `preset`.
    pub shards: Vec<u16>,
    pub relay: bool,
    pub rln_relay: bool,
    pub store: bool,
    pub filter: bool,
    pub lightpush: bool,
    pub peer_exchange: bool,
    pub discv5: bool,
    pub dns_discovery: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            preset: TWN,
            shards: Vec::new(),
            relay: true,
            rln_relay: true,
            store: false,
            filter: false,
            lightpush: false,
            peer_exchange: false,
            discv5: true,
            dns_discovery: true,
        }
    }
}

impl Config {
    /// Shards this node will actually subscribe to (defaults to the whole cluster).
    pub fn effective_shards(&self) -> Vec<u16> {
        if self.shards.is_empty() {
            (0..self.preset.shard_count).collect()
        } else {
            self.shards.clone()
        }
    }
}
