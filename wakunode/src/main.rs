//! `wakunode` — the rs-waku node binary.
//!
//! The CLI deliberately mirrors nwaku's flag semantics so operators and the
//! interop suite can drive it interchangeably. Milestone 1: it stands up the
//! libp2p swarm, subscribes to the configured shards, dials any static peers,
//! and relays. RLN/store/filter/lightpush land in later milestones.

use clap::Parser;
use libp2p::Multiaddr;
use waku_core::{ShardId, TWN};
use waku_node::{spawn, Event, NodeConfig};

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

    /// TCP port to listen on (0 = ephemeral).
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
    let (node, mut events) = spawn(NodeConfig::new().with_listen_addr(listen)).await?;
    tracing::info!(peer_id = %node.peer_id(), "rs-waku node started");

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
                Some(Event::Listening(addr)) => tracing::info!(%addr, "listening"),
                None => break,
            }
        }
    }
    Ok(())
}
