//! `wakunode` — the rs-waku node binary.
//!
//! The CLI deliberately mirrors nwaku's flag semantics so operators and the
//! interop suite can drive it interchangeably. Milestone 1: it stands up the
//! libp2p swarm, subscribes to the configured shards, dials any static peers,
//! and relays. RLN/store/filter/lightpush land in later milestones.

use std::net::Ipv4Addr;
use std::str::FromStr;

use clap::Parser;
use libp2p::Multiaddr;
use waku_core::{ShardId, TWN};
use waku_node::{spawn, DiscoverySettings, Event, NodeConfig, WakuEnr};

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

    let (node, mut events) = spawn(config).await?;
    tracing::info!(peer_id = %node.peer_id(), "rs-waku node started");
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

    // Drive the event stream until Ctrl-C.
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down");
                break;
            }
            event = events.recv() => match event {
                Some(Event::Message { shard, id, message, .. }) => tracing::info!(
                    topic = %shard.pubsub_topic(),
                    id = %waku_core::hash_hex(&id),
                    content_topic = %message.content_topic,
                    bytes = message.payload.len(),
                    "relayed message",
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
