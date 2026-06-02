//! `wakunode` — the rs-waku node binary.
//!
//! The CLI deliberately mirrors nwaku's flag semantics so operators and the
//! interop suite can drive it interchangeably. Today it parses config and
//! reports the plan; the swarm lands in Milestone 1.

use clap::Parser;
use waku_core::{ShardId, TWN};
use waku_node::Config;

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

    /// Enable 11/WAKU2-RELAY.
    #[arg(long, default_value_t = true)]
    relay: bool,

    /// Enable 17/WAKU2-RLN-RELAY.
    #[arg(long = "rln-relay", default_value_t = false)]
    rln_relay: bool,

    /// Enable 13/WAKU2-STORE.
    #[arg(long, default_value_t = false)]
    store: bool,

    /// Enable 12/WAKU2-FILTER (full-node side).
    #[arg(long, default_value_t = false)]
    filter: bool,

    /// Enable 19/WAKU2-LIGHTPUSH (full-node side).
    #[arg(long, default_value_t = false)]
    lightpush: bool,

    /// Enable 34/WAKU2-PEER-EXCHANGE.
    #[arg(long = "peer-exchange", default_value_t = false)]
    peer_exchange: bool,

    /// Enable 33/WAKU2-DISCV5 discovery.
    #[arg(long = "discv5-discovery", default_value_t = true)]
    discv5: bool,

    /// Enable EIP-1459 DNS discovery bootstrap.
    #[arg(long = "dns-discovery", default_value_t = true)]
    dns_discovery: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    // For now only TWN is wired; other clusters become presets later.
    let config = Config {
        preset: TWN,
        shards: cli.shards,
        relay: cli.relay,
        rln_relay: cli.rln_relay,
        store: cli.store,
        filter: cli.filter,
        lightpush: cli.lightpush,
        peer_exchange: cli.peer_exchange,
        discv5: cli.discv5,
        dns_discovery: cli.dns_discovery,
    };

    tracing::info!(
        preset = config.preset.name,
        cluster_id = cli.cluster_id,
        "starting rs-waku"
    );
    let topics: Vec<String> = config
        .effective_shards()
        .into_iter()
        .map(|s| ShardId::new(config.preset.cluster_id, s).pubsub_topic())
        .collect();
    tracing::info!(?topics, "subscriptions planned");
    tracing::warn!("node runtime not yet implemented — Milestone 1 (relay + metadata + discv5)");
}
