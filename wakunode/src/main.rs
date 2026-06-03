//! `wakunode` — the rs-waku node binary.
//!
//! The CLI deliberately mirrors nwaku's flag semantics so operators and the
//! interop suite can drive it interchangeably. Milestone 1: it stands up the
//! libp2p swarm, subscribes to the configured shards, dials any static peers,
//! and relays. RLN/store/filter/lightpush land in later milestones.

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use libp2p::identity::Keypair;
use libp2p::Multiaddr;
use waku_core::{ShardId, TWN};
use waku_node::{spawn, DiscoverySettings, Event, NodeConfig, WakuEnr};
use waku_store::{MessageStore, SqliteStore};

/// rs-waku node (Logos Messaging) — native Rust.
#[derive(Parser, Debug)]
#[command(name = "wakunode", version, about)]
struct Cli {
    /// Cluster id (TWN mainnet = 1).
    #[arg(long, default_value_t = 1)]
    cluster_id: u16,

    /// Shard to subscribe to; repeatable. Empty ⇒ all shards in the cluster.
    #[arg(long = "shard")]
    shards: Vec<u16>,

    /// TCP port to listen on (0 = ephemeral; a fixed port is required for discv5).
    #[arg(long = "tcp-port", default_value_t = 60000)]
    tcp_port: u16,

    /// Static peer multiaddr to dial; repeatable.
    #[arg(long = "staticnode")]
    staticnodes: Vec<Multiaddr>,

    /// Enable 11/WAKU2-RELAY.
    #[arg(long, default_value_t = true)]
    relay: bool,

    /// Enable 17/WAKU2-RLN-RELAY (not yet implemented).
    #[arg(long = "rln-relay", default_value_t = false)]
    rln_relay: bool,

    /// Enable 33/WAKU2-DISCV5 discovery.
    #[arg(long = "discv5-discovery", default_value_t = false)]
    discv5: bool,

    /// UDP port for discv5.
    #[arg(long = "discv5-udp-port", default_value_t = 9000)]
    discv5_udp_port: u16,

    /// Externally reachable IPv4 to advertise in our ENR.
    #[arg(long = "ext-ip", default_value = "127.0.0.1")]
    ext_ip: Ipv4Addr,

    /// discv5 bootstrap node ENR (`enr:...`); repeatable.
    #[arg(long = "discv5-bootstrap-node")]
    bootstrap_enrs: Vec<String>,

    /// Enable EIP-1459 DNS discovery.
    #[arg(long = "dns-discovery", default_value_t = false)]
    dns_discovery: bool,

    /// enrtree:// URL for DNS discovery; repeatable. Defaults to the TWN tree.
    #[arg(long = "dns-discovery-url")]
    dns_discovery_urls: Vec<String>,

    /// Enable 13/WAKU2-STORE (in-memory SQLite), persisting accepted messages.
    #[arg(long, default_value_t = false)]
    store: bool,

    /// Serve the nwaku-compatible REST API on this port (e.g. 8645).
    #[arg(long = "rest-port")]
    rest_port: Option<u16>,

    /// Maximum total established connections (DoS guard).
    #[arg(long = "max-connections", default_value_t = 300)]
    max_connections: u32,

    /// Max concurrent connections from a single IP (0 = unlimited).
    #[arg(long = "ip-colocation-limit", default_value_t = 20)]
    ip_colocation_limit: usize,

    /// Persist the message store to this SQLite file (default: in-memory).
    #[arg(long = "store-path")]
    store_path: Option<String>,

    /// Load/persist the node's secp256k1 identity at this path (stable peer-id/ENR).
    #[arg(long = "node-key-file")]
    node_key_file: Option<PathBuf>,
}

/// Load a secp256k1 identity from `path`, or generate and persist one.
fn load_or_create_key(path: &Path) -> std::io::Result<Keypair> {
    if path.exists() {
        let hex_str = std::fs::read_to_string(path)?;
        let mut bytes = hex::decode(hex_str.trim())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let secret = libp2p::identity::secp256k1::SecretKey::try_from_bytes(&mut bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(libp2p::identity::secp256k1::Keypair::from(secret).into())
    } else {
        let keypair = Keypair::generate_secp256k1();
        let secret = keypair
            .clone()
            .try_into_secp256k1()
            .expect("generated secp256k1")
            .secret()
            .to_bytes();
        std::fs::write(path, hex::encode(secret))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(keypair)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    if cli.rln_relay {
        tracing::warn!("--rln-relay requested but RLN is Milestone 2; running without it");
    }
    if cli.cluster_id != TWN.cluster_id {
        tracing::warn!(
            cli.cluster_id,
            "only the TWN preset (cluster 1) is wired so far"
        );
    }

    let shards: Vec<u16> = if cli.shards.is_empty() {
        (0..TWN.shard_count).collect()
    } else {
        cli.shards.clone()
    };

    let listen: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", cli.tcp_port).parse()?;
    let mut config = NodeConfig::new()
        .with_listen_addr(listen)
        .with_cluster(cli.cluster_id, shards.clone());
    config.max_connections = cli.max_connections;
    config.ip_colocation_limit = cli.ip_colocation_limit;
    if let Some(path) = &cli.node_key_file {
        config.keypair = load_or_create_key(path)?;
        tracing::info!(path = %path.display(), "loaded persistent node identity");
    }

    // Assemble discovery settings if any discovery mechanism was requested.
    let want_discovery = cli.discv5 || cli.dns_discovery || !cli.bootstrap_enrs.is_empty();
    if want_discovery {
        if cli.tcp_port == 0 {
            tracing::warn!("discv5 advertises --tcp-port; using 0 makes us undialable");
        }
        let bootstrap = cli
            .bootstrap_enrs
            .iter()
            .filter_map(|s| match WakuEnr::from_str(s) {
                Ok(enr) => Some(enr),
                Err(e) => {
                    tracing::warn!(enr = %s, error = %e, "ignoring invalid bootstrap ENR");
                    None
                }
            })
            .collect();
        let dns_bootstrap = if cli.dns_discovery && cli.dns_discovery_urls.is_empty() {
            vec![TWN.dns_discovery_enrtree.to_string()]
        } else {
            cli.dns_discovery_urls.clone()
        };
        config.discovery = Some(DiscoverySettings {
            udp_port: cli.discv5_udp_port,
            advertised_ip: cli.ext_ip,
            advertised_tcp_port: cli.tcp_port,
            bootstrap,
            dns_bootstrap,
        });
    }

    // Optional message store (needed for store-on-relay and the REST store API).
    let store: Option<Arc<dyn MessageStore>> = if let Some(path) = &cli.store_path {
        Some(Arc::new(
            SqliteStore::connect(&format!("sqlite:{path}")).await?,
        ))
    } else if cli.store || cli.rest_port.is_some() {
        Some(Arc::new(SqliteStore::in_memory().await?))
    } else {
        None
    };
    config.store = store.clone();

    let (node, mut events) = spawn(config).await?;
    tracing::info!(peer_id = %node.peer_id(), "rs-waku node started");

    // The relay message cache feeds `GET /relay/v1/auto/messages/{ct}`.
    let mut rest_cache: Option<waku_rest::MessageCache> = None;
    if let Some(port) = cli.rest_port {
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let state = waku_rest::AppState::new(node.clone(), store.clone());
        rest_cache = Some(state.cache());
        tokio::spawn(async move {
            if let Err(e) = waku_rest::serve(addr, state).await {
                tracing::error!(error = %e, "REST API server stopped");
            }
        });
        tracing::info!(%addr, "REST API enabled");
    }
    if let Some(enr) = node.discv5_enr() {
        tracing::info!(enr = %enr.to_base64(), "local ENR (share me as a bootstrap node)");
    }

    for shard in &shards {
        let s = ShardId::new(cli.cluster_id, *shard);
        node.subscribe(s).await?;
        tracing::info!(topic = %s.pubsub_topic(), "subscribed");
    }

    for addr in &cli.staticnodes {
        match node.dial(addr.clone()).await {
            Ok(()) => tracing::info!(%addr, "dialing static node"),
            Err(e) => tracing::warn!(%addr, error = %e, "failed to dial static node"),
        }
    }

    // A SIGTERM future (unix); a never-completing future elsewhere.
    #[cfg(unix)]
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();

    // Drive the event stream until a shutdown signal.
    loop {
        let terminate = async {
            #[cfg(unix)]
            if let Some(sig) = sigterm.as_mut() {
                sig.recv().await;
            } else {
                std::future::pending::<()>().await;
            }
            #[cfg(not(unix))]
            std::future::pending::<()>().await;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received Ctrl-C, shutting down");
                node.shutdown().await;
                break;
            }
            _ = terminate => {
                tracing::info!("received SIGTERM, shutting down");
                node.shutdown().await;
                break;
            }
            event = events.recv() => match event {
                Some(Event::Message { shard, id, message, .. }) => {
                    tracing::info!(
                        topic = %shard.pubsub_topic(),
                        id = %waku_core::hash_hex(&id),
                        content_topic = %message.content_topic,
                        bytes = message.payload.len(),
                        "relayed message",
                    );
                    if let Some(cache) = &rest_cache {
                        cache.record(message);
                    }
                }
                Some(Event::FilterMessage { message, .. }) => tracing::info!(
                    content_topic = %message.content_topic,
                    bytes = message.payload.len(),
                    "filter-push message",
                ),
                Some(Event::PeerConnected(p)) => tracing::info!(peer = %p, "peer connected"),
                Some(Event::PeerDisconnected(p)) => tracing::debug!(peer = %p, "peer disconnected"),
                Some(Event::MetadataMismatch { peer, theirs }) => tracing::warn!(
                    %peer, ?theirs, "disconnected peer: cluster mismatch",
                ),
                Some(Event::Listening(addr)) => tracing::info!(%addr, "listening"),
                None => break,
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_identity_persists_across_loads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodekey");

        // First load generates + writes the key.
        let k1 = load_or_create_key(&path).unwrap();
        assert!(path.exists());
        // Second load reads the same key back → same peer id.
        let k2 = load_or_create_key(&path).unwrap();
        assert_eq!(k1.public().to_peer_id(), k2.public().to_peer_id());
    }
}
