//! # waku-discv5 — 33/WAKU2-DISCV5
//!
//! Wraps `sigp/discv5` for Waku peer discovery: builds a local ENR carrying the
//! relay-shards (`rs`) field, runs the discv5 service, and discovers peers
//! filtered to our cluster.
//!
//! Waku isolation: nwaku does not customise the discv5 protocol-id — it stays on
//! the standard `discv5` wire protocol and isolates by bootstrapping only from
//! Waku nodes and filtering ENRs by the Waku shard fields. We do the same; the
//! cluster filter is applied to every discovered ENR.
//!
//! EIP-1459 DNS discovery (the enrtree TXT resolver) is the remaining M1 piece
//! and will live alongside this.

pub mod dns;

#[cfg(feature = "dns")]
pub use dns::HickoryResolver;
pub use dns::{resolve as resolve_enrtree, EnrTreeLink, TxtResolver};

use std::net::Ipv4Addr;

use discv5::enr::{CombinedKey, Enr, EnrPublicKey, NodeId};
use discv5::{ConfigBuilder, Discv5, ListenConfig};
use libp2p::identity::{secp256k1, PublicKey};
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use thiserror::Error;
use waku_enr::RelayShards;

pub use discv5::enr::CombinedKey as Key;
pub use waku_enr::{RelayShards as Shards, ENR_KEY_RS, ENR_KEY_RSV};

/// A Waku ENR (secp256k1-keyed).
pub type WakuEnr = Enr<CombinedKey>;

/// A discovered peer resolved to libp2p-dialable form.
#[derive(Clone, Debug)]
pub struct DiscoveredPeer {
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
    pub shards: RelayShards,
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("ENR build failed: {0}")]
    EnrBuild(String),
    #[error("discv5 init failed: {0}")]
    Init(String),
    #[error("discv5 error: {0}")]
    Discv5(String),
}

/// Configuration for the discv5 service.
pub struct DiscoveryConfig {
    pub listen_ip: Ipv4Addr,
    pub udp_port: u16,
    /// Advertised TCP port (libp2p) in the ENR, if any.
    pub tcp_port: Option<u16>,
    /// Externally reachable IP to advertise; defaults to `listen_ip`.
    pub external_ip: Option<Ipv4Addr>,
    pub cluster_id: u16,
    pub shards: Vec<u16>,
    pub bootstrap: Vec<Enr<CombinedKey>>,
    /// discv5/ENR signing key (secp256k1). Generated if `None`.
    pub key: Option<CombinedKey>,
}

impl DiscoveryConfig {
    pub fn new(udp_port: u16) -> Self {
        Self {
            listen_ip: Ipv4Addr::LOCALHOST,
            udp_port,
            tcp_port: None,
            external_ip: None,
            cluster_id: waku_core::TWN.cluster_id,
            shards: (0..waku_core::TWN.shard_count).collect(),
            bootstrap: Vec::new(),
            key: None,
        }
    }

    pub fn with_cluster(mut self, cluster_id: u16, shards: Vec<u16>) -> Self {
        self.cluster_id = cluster_id;
        self.shards = shards;
        self
    }

    pub fn with_bootstrap(mut self, bootstrap: Vec<Enr<CombinedKey>>) -> Self {
        self.bootstrap = bootstrap;
        self
    }
}

/// The Waku discovery service.
pub struct Discovery {
    discv5: Discv5,
    cluster_id: u16,
}

impl Discovery {
    /// Build the local ENR and the discv5 service (not yet started).
    pub fn new(mut config: DiscoveryConfig) -> Result<Self, DiscoveryError> {
        let key = config
            .key
            .take()
            .unwrap_or_else(CombinedKey::generate_secp256k1);
        let local_enr = build_waku_enr(&key, &config)?;

        let listen = ListenConfig::Ipv4 {
            ip: config.listen_ip,
            port: config.udp_port,
        };
        let discv5_config = ConfigBuilder::new(listen).build();

        let discv5 = Discv5::new(local_enr, key, discv5_config)
            .map_err(|e| DiscoveryError::Init(e.to_string()))?;

        for enr in config.bootstrap.drain(..) {
            if let Err(e) = discv5.add_enr(enr) {
                tracing::warn!(error = %e, "skipping invalid bootstrap ENR");
            }
        }

        Ok(Self {
            discv5,
            cluster_id: config.cluster_id,
        })
    }

    /// Start the discv5 service (binds the UDP socket).
    pub async fn start(&mut self) -> Result<(), DiscoveryError> {
        self.discv5
            .start()
            .await
            .map_err(|e| DiscoveryError::Discv5(e.to_string()))
    }

    pub fn local_enr(&self) -> Enr<CombinedKey> {
        self.discv5.local_enr()
    }

    /// Run a FINDNODE query, returning only ENRs in our cluster.
    pub async fn discover(&self) -> Result<Vec<Enr<CombinedKey>>, DiscoveryError> {
        let cluster = self.cluster_id;
        let predicate = Box::new(move |enr: &Enr<CombinedKey>| {
            enr_relay_shards(enr)
                .map(|rs| rs.cluster_id == cluster)
                .unwrap_or(false)
        });
        self.discv5
            .find_node_predicate(NodeId::random(), predicate, 16)
            .await
            .map_err(|e| DiscoveryError::Discv5(e.to_string()))
    }

    /// Like [`discover`](Self::discover), but resolved to libp2p-dialable peers.
    /// ENRs without a usable TCP endpoint or non-secp256k1 keys are dropped.
    pub async fn discover_dialable(&self) -> Result<Vec<DiscoveredPeer>, DiscoveryError> {
        Ok(self
            .discover()
            .await?
            .iter()
            .filter_map(enr_to_dialable)
            .collect())
    }

    /// ENRs currently held in the routing table (e.g. via prior sessions).
    pub fn table_peers(&self) -> Vec<Enr<CombinedKey>> {
        self.discv5.table_entries_enr()
    }
}

/// Derive the libp2p [`PeerId`] from an ENR's secp256k1 key.
///
/// Waku/nwaku use a single secp256k1 key for both the libp2p host and the ENR,
/// so the peer id is recoverable directly from the record.
pub fn enr_peer_id(enr: &Enr<CombinedKey>) -> Option<PeerId> {
    let compressed = enr.public_key().encode(); // 33-byte compressed for secp256k1
    let pk = secp256k1::PublicKey::try_from_bytes(compressed.as_ref()).ok()?;
    Some(PublicKey::from(pk).to_peer_id())
}

/// Resolve an ENR to a dialable [`DiscoveredPeer`] (peer id + TCP multiaddr +
/// shards). Returns `None` if the record lacks a TCP endpoint, a secp256k1 key,
/// or relay-shard info.
pub fn enr_to_dialable(enr: &Enr<CombinedKey>) -> Option<DiscoveredPeer> {
    let peer_id = enr_peer_id(enr)?;
    let shards = enr_relay_shards(enr)?;
    let (ip, tcp) = (enr.ip4()?, enr.tcp4()?);
    let addr = Multiaddr::empty()
        .with(Protocol::Ip4(ip))
        .with(Protocol::Tcp(tcp))
        .with(Protocol::P2p(peer_id));
    Some(DiscoveredPeer {
        peer_id,
        addrs: vec![addr],
        shards,
    })
}

/// Build a Waku ENR: ip/udp(/tcp) plus the relay-shards `rs` field.
fn build_waku_enr(
    key: &CombinedKey,
    config: &DiscoveryConfig,
) -> Result<Enr<CombinedKey>, DiscoveryError> {
    let shards = RelayShards::new(config.cluster_id, config.shards.iter().copied());
    let rs_bytes = shards
        .to_indices_list()
        .map_err(|e| DiscoveryError::EnrBuild(e.to_string()))?;

    let mut builder = Enr::builder();
    builder.ip4(config.external_ip.unwrap_or(config.listen_ip));
    builder.udp4(config.udp_port);
    if let Some(tcp) = config.tcp_port {
        builder.tcp4(tcp);
    }
    // The `rs` value is a raw byte string; passing `&[u8]` RLP-encodes it as such.
    builder.add_value(ENR_KEY_RS, &rs_bytes.as_slice());

    builder
        .build(key)
        .map_err(|e| DiscoveryError::EnrBuild(e.to_string()))
}

/// Read the relay-shards descriptor from an ENR (`rs` indices list, else `rsv`).
pub fn enr_relay_shards(enr: &Enr<CombinedKey>) -> Option<RelayShards> {
    if let Some(Ok(bytes)) = enr.get_decodable::<bytes::Bytes>(ENR_KEY_RS) {
        return RelayShards::from_indices_list(bytes.as_ref()).ok();
    }
    if let Some(Ok(bytes)) = enr.get_decodable::<bytes::Bytes>(ENR_KEY_RSV) {
        return RelayShards::from_bit_vector(bytes.as_ref()).ok();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_enr_carries_relay_shards() {
        let cfg = DiscoveryConfig::new(0).with_cluster(1, vec![0, 1, 2, 7]);
        let disco = Discovery::new(cfg).expect("build discovery");
        let shards = enr_relay_shards(&disco.local_enr()).expect("rs field present");
        assert_eq!(shards.cluster_id, 1);
        assert_eq!(shards.shards, vec![0, 1, 2, 7]);
    }

    #[test]
    fn enr_peer_id_matches_the_libp2p_identity() {
        // The same secp256k1 secret in libp2p form and ENR form must yield the
        // same peer id — this is the discv5 → libp2p dialing bridge.
        let lp = libp2p::identity::Keypair::generate_secp256k1();
        let expected = lp.public().to_peer_id();

        let mut secret = lp.try_into_secp256k1().unwrap().secret().to_bytes();
        let key = CombinedKey::secp256k1_from_bytes(&mut secret).unwrap();

        let mut cfg = DiscoveryConfig::new(0).with_cluster(1, vec![0]);
        cfg.key = Some(key);
        cfg.tcp_port = Some(40404);
        let disco = Discovery::new(cfg).expect("build discovery");
        let enr = disco.local_enr();

        assert_eq!(enr_peer_id(&enr), Some(expected));
        let dialable = enr_to_dialable(&enr).expect("dialable");
        assert_eq!(dialable.peer_id, expected);
        assert!(dialable.addrs[0].to_string().contains("/tcp/40404"));
    }
}
