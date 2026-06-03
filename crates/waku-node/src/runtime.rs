//! Swarm composition + the async driver.
//!
//! One libp2p `Swarm` is owned by a single task ([`run`]); everything else talks
//! to it over channels. [`spawn`] returns a [`NodeHandle`] (commands) plus an
//! event stream. This is the seam through which `waku-store`, `waku-rln`, the
//! REST API, etc. will later attach.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::ratelimit::RateLimiters;

use futures::StreamExt;
use libp2p::connection_limits::{self, ConnectionLimits};
use libp2p::gossipsub::MessageAcceptance;
use libp2p::multiaddr::Protocol;
use libp2p::request_response::{self, OutboundRequestId};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{gossipsub, identify, identity::Keypair, noise, tcp, yamux, Multiaddr, PeerId, Swarm};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use waku_core::{deterministic_hash, MessageHash, ShardId, WakuMessage, TWN};
use waku_discv5::{Discovery, DiscoveryConfig, Key as CombinedKey, WakuEnr};
use waku_filter::{
    self, subscribe_type, FilterPushResponse, FilterSubscribeRequest, FilterSubscribeResponse,
    MessagePush,
};
use waku_lightpush::{LightpushRequest, LightpushResponse};
use waku_metadata::WakuMetadata;
use waku_peer_exchange::{self, PeerExchangeRpc};
use waku_relay::{validate, MessageFacts, RlnStatus, Validation, ValidationPolicy};
use waku_store::store_query::{self, StoreQueryRequest, StoreQueryResponse};
use waku_store::MessageStore;

/// Outbound requests awaiting their response, keyed by request id.
#[derive(Default)]
struct Pending {
    store_queries: HashMap<OutboundRequestId, oneshot::Sender<Result<StoreQueryResponse, String>>>,
    lightpush: HashMap<OutboundRequestId, oneshot::Sender<Result<LightpushResponse, String>>>,
    peer_exchange: HashMap<OutboundRequestId, oneshot::Sender<Result<PeerExchangeRpc, String>>>,
    filter: HashMap<OutboundRequestId, oneshot::Sender<Result<FilterSubscribeResponse, String>>>,
}

/// A light client's content filter (12/WAKU2-FILTER): which pubsub topic and
/// content topics it wants pushed (empty content topics = all on that topic).
#[derive(Clone, Debug)]
struct FilterSub {
    pubsub_topic: Option<String>,
    content_topics: Vec<String>,
}

impl FilterSub {
    fn matches(&self, topic: &str, content_topic: &str) -> bool {
        let topic_ok = self.pubsub_topic.as_deref().is_none_or(|t| t == topic);
        let ct_ok = self.content_topics.is_empty()
            || self.content_topics.iter().any(|c| c == content_topic);
        topic_ok && ct_ok
    }
}

/// Per-peer filter subscriptions this (full) node serves.
type FilterRegistry = HashMap<PeerId, Vec<FilterSub>>;

/// Apply a filter-subscribe request to the registry and build the ack.
fn apply_filter_request(
    filters: &mut FilterRegistry,
    peer: PeerId,
    req: &FilterSubscribeRequest,
) -> FilterSubscribeResponse {
    match req.filter_subscribe_type {
        subscribe_type::SUBSCRIBE => {
            filters.entry(peer).or_default().push(FilterSub {
                pubsub_topic: req.pubsub_topic.clone(),
                content_topics: req.content_topics.clone(),
            });
        }
        subscribe_type::UNSUBSCRIBE => {
            if let Some(subs) = filters.get_mut(&peer) {
                subs.retain(|s| {
                    s.pubsub_topic != req.pubsub_topic || s.content_topics != req.content_topics
                });
            }
        }
        subscribe_type::UNSUBSCRIBE_ALL => {
            filters.remove(&peer);
        }
        subscribe_type::PING => {}
        _ => return FilterSubscribeResponse::error(req.request_id.clone(), 400, "unknown type"),
    }
    FilterSubscribeResponse::ok(req.request_id.clone())
}

/// Push a message to every subscriber whose filter matches it (filter-push).
fn push_filter_matches(
    swarm: &mut Swarm<WakuBehaviour>,
    filters: &FilterRegistry,
    topic: &str,
    msg: &WakuMessage,
) {
    let targets: Vec<PeerId> = filters
        .iter()
        .filter(|(_, subs)| subs.iter().any(|s| s.matches(topic, &msg.content_topic)))
        .map(|(peer, _)| *peer)
        .collect();
    for peer in targets {
        let push = MessagePush {
            waku_message: Some(msg.clone()),
            pubsub_topic: Some(topic.to_string()),
        };
        swarm.behaviour_mut().filter_push.send_request(&peer, push);
    }
}

/// Cap on the ENRs we keep for serving peer-exchange.
const PEER_BOOK_CAP: usize = 200;

/// ENRs we've learned (bootstrap + discovered), shared with peer-exchange.
type PeerBook = Arc<Mutex<Vec<WakuEnr>>>;

/// Remember an ENR for peer-exchange (dedup by node id, bounded).
fn remember_enr(book: &Mutex<Vec<WakuEnr>>, enr: WakuEnr) {
    let mut book = book.lock().expect("peer book lock");
    if book.iter().any(|e| e.node_id() == enr.node_id()) {
        return;
    }
    if book.len() >= PEER_BOOK_CAP {
        book.remove(0);
    }
    book.push(enr);
}

/// Current Unix time in nanoseconds (for message timestamp validation).
fn now_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// How often the discovery task runs a fresh discv5 query.
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(2);

/// The composed Waku network behaviour. Milestone 1 carries relay + identify +
/// metadata; discv5, store, filter, … slot in as further fields.
#[derive(NetworkBehaviour)]
pub struct WakuBehaviour {
    pub relay: gossipsub::Behaviour,
    pub identify: identify::Behaviour,
    pub metadata: waku_metadata::Behaviour,
    pub store_query: store_query::Behaviour,
    pub lightpush: waku_lightpush::Behaviour,
    pub peer_exchange: waku_peer_exchange::Behaviour,
    pub filter_subscribe: waku_filter::SubscribeBehaviour,
    pub filter_push: waku_filter::PushBehaviour,
    pub connection_limits: connection_limits::Behaviour,
}

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("transport/build error: {0}")]
    Build(String),
    #[error("listen failed: {0}")]
    Listen(String),
    #[error("the node task has stopped")]
    NodeStopped,
    #[error("command failed: {0}")]
    Command(String),
}

/// discv5 discovery settings for a node.
pub struct DiscoverySettings {
    /// UDP port for the discv5 service.
    pub udp_port: u16,
    /// Externally reachable IPv4 advertised in our ENR.
    pub advertised_ip: Ipv4Addr,
    /// libp2p TCP port advertised in our ENR (so peers can dial us).
    pub advertised_tcp_port: u16,
    /// Bootstrap ENRs to seed the DHT and dial immediately.
    pub bootstrap: Vec<WakuEnr>,
    /// EIP-1459 `enrtree://` URLs resolved to extra bootstrap ENRs at startup.
    pub dns_bootstrap: Vec<String>,
}

/// Configuration for the libp2p swarm underlying a node.
pub struct NodeConfig {
    pub keypair: Keypair,
    pub listen_addrs: Vec<Multiaddr>,
    pub idle_timeout: Duration,
    /// Cluster id advertised in the 66/WAKU2-METADATA handshake.
    pub cluster_id: u16,
    /// Shards advertised in the metadata handshake.
    pub shards: Vec<u16>,
    /// If set, run discv5 discovery and auto-dial discovered cluster peers.
    pub discovery: Option<DiscoverySettings>,
    /// If set, persist accepted (non-ephemeral) relay messages to this store.
    pub store: Option<Arc<dyn MessageStore>>,
    /// Maximum total established connections (DoS guard).
    pub max_connections: u32,
    /// Max concurrent connections from a single IP (0 = unlimited).
    pub ip_colocation_limit: usize,
}

/// Extract the first IP from a multiaddr (for ip-colocation accounting).
fn multiaddr_ip(addr: &Multiaddr) -> Option<IpAddr> {
    addr.iter().find_map(|p| match p {
        Protocol::Ip4(ip) => Some(IpAddr::V4(ip)),
        Protocol::Ip6(ip) => Some(IpAddr::V6(ip)),
        _ => None,
    })
}

impl NodeConfig {
    /// Fresh secp256k1 identity (shared with discv5/ENR), no listen addresses,
    /// TWN cluster/shards. secp256k1 matches nwaku's host-key choice and lets a
    /// peer's libp2p id be recovered straight from its ENR.
    pub fn new() -> Self {
        Self {
            keypair: Keypair::generate_secp256k1(),
            listen_addrs: Vec::new(),
            idle_timeout: Duration::from_secs(60),
            cluster_id: TWN.cluster_id,
            shards: (0..TWN.shard_count).collect(),
            discovery: None,
            store: None,
            max_connections: 300,
            ip_colocation_limit: 20,
        }
    }

    pub fn with_listen_addr(mut self, addr: Multiaddr) -> Self {
        self.listen_addrs.push(addr);
        self
    }

    pub fn with_cluster(mut self, cluster_id: u16, shards: Vec<u16>) -> Self {
        self.cluster_id = cluster_id;
        self.shards = shards;
        self
    }

    fn local_metadata(&self) -> WakuMetadata {
        WakuMetadata {
            cluster_id: Some(self.cluster_id as u32),
            shards: self.shards.iter().map(|s| *s as u32).collect(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Events surfaced from the swarm to the rest of the application.
#[derive(Debug)]
pub enum Event {
    Listening(Multiaddr),
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    /// A peer reported a different cluster id; we disconnected it (66/WAKU2-METADATA).
    MetadataMismatch {
        peer: PeerId,
        theirs: Option<u32>,
    },
    /// A relay message accepted on a shard.
    Message {
        shard: ShardId,
        message: WakuMessage,
        id: MessageHash,
        propagation_source: PeerId,
    },
    /// A message delivered to us via 12/WAKU2-FILTER filter-push (as a client).
    FilterMessage {
        pubsub_topic: Option<String>,
        message: WakuMessage,
    },
}

enum Command {
    Subscribe {
        shard: ShardId,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Publish {
        shard: ShardId,
        message: Box<WakuMessage>,
        reply: oneshot::Sender<Result<MessageHash, String>>,
    },
    Dial {
        addr: Multiaddr,
        reply: oneshot::Sender<Result<(), String>>,
    },
    StoreQuery {
        peer: PeerId,
        request: Box<StoreQueryRequest>,
        reply: oneshot::Sender<Result<StoreQueryResponse, String>>,
    },
    LightPush {
        peer: PeerId,
        request: Box<LightpushRequest>,
        reply: oneshot::Sender<Result<LightpushResponse, String>>,
    },
    PeerExchange {
        peer: PeerId,
        num_peers: u64,
        reply: oneshot::Sender<Result<PeerExchangeRpc, String>>,
    },
    FilterSubscribe {
        peer: PeerId,
        request: Box<FilterSubscribeRequest>,
        reply: oneshot::Sender<Result<FilterSubscribeResponse, String>>,
    },
    Shutdown,
}

/// Handle for issuing commands to a running node.
#[derive(Clone)]
pub struct NodeHandle {
    peer_id: PeerId,
    cmd_tx: mpsc::Sender<Command>,
    discv5_enr: Option<WakuEnr>,
    store: Option<Arc<dyn MessageStore>>,
    connected: Arc<Mutex<HashSet<PeerId>>>,
}

impl NodeHandle {
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The node's message store, if store-on-relay is enabled.
    pub fn store(&self) -> Option<Arc<dyn MessageStore>> {
        self.store.clone()
    }

    /// Currently connected peers.
    pub fn connected_peers(&self) -> Vec<PeerId> {
        self.connected
            .lock()
            .expect("connected lock")
            .iter()
            .copied()
            .collect()
    }

    /// This node's discv5 ENR, if discovery is enabled. Hand it to other nodes
    /// as a bootstrap entry.
    pub fn discv5_enr(&self) -> Option<WakuEnr> {
        self.discv5_enr.clone()
    }

    pub async fn subscribe(&self, shard: ShardId) -> Result<(), NodeError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Subscribe { shard, reply })
            .await
            .map_err(|_| NodeError::NodeStopped)?;
        rx.await
            .map_err(|_| NodeError::NodeStopped)?
            .map_err(NodeError::Command)
    }

    /// Publish a message to a shard. Returns the RFC-14 message hash (== gossipsub id).
    pub async fn publish(
        &self,
        shard: ShardId,
        message: WakuMessage,
    ) -> Result<MessageHash, NodeError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Publish {
                shard,
                message: Box::new(message),
                reply,
            })
            .await
            .map_err(|_| NodeError::NodeStopped)?;
        rx.await
            .map_err(|_| NodeError::NodeStopped)?
            .map_err(NodeError::Command)
    }

    pub async fn dial(&self, addr: Multiaddr) -> Result<(), NodeError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Dial { addr, reply })
            .await
            .map_err(|_| NodeError::NodeStopped)?;
        rx.await
            .map_err(|_| NodeError::NodeStopped)?
            .map_err(NodeError::Command)
    }

    /// Query a peer's 13/WAKU2-STORE v3 service.
    pub async fn store_query(
        &self,
        peer: PeerId,
        request: StoreQueryRequest,
    ) -> Result<StoreQueryResponse, NodeError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::StoreQuery {
                peer,
                request: Box::new(request),
                reply,
            })
            .await
            .map_err(|_| NodeError::NodeStopped)?;
        rx.await
            .map_err(|_| NodeError::NodeStopped)?
            .map_err(NodeError::Command)
    }

    /// Light-push a message to `peer` (a full relay node), which injects it into
    /// gossipsub on `pubsub_topic`.
    pub async fn light_push(
        &self,
        peer: PeerId,
        pubsub_topic: impl Into<String>,
        message: WakuMessage,
    ) -> Result<LightpushResponse, NodeError> {
        let (reply, rx) = oneshot::channel();
        let request = LightpushRequest {
            request_id: String::new(),
            pubsub_topic: pubsub_topic.into(),
            message: Some(message),
        };
        self.cmd_tx
            .send(Command::LightPush {
                peer,
                request: Box::new(request),
                reply,
            })
            .await
            .map_err(|_| NodeError::NodeStopped)?;
        rx.await
            .map_err(|_| NodeError::NodeStopped)?
            .map_err(NodeError::Command)
    }

    /// Gracefully stop the node: the swarm task exits, closing connections and
    /// the event stream. Subsequent commands fail with [`NodeError::NodeStopped`].
    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(Command::Shutdown).await;
    }

    /// Ask `peer` for up to `num_peers` ENRs (34/WAKU2-PEER-EXCHANGE).
    pub async fn peer_exchange(
        &self,
        peer: PeerId,
        num_peers: u64,
    ) -> Result<Vec<WakuEnr>, NodeError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::PeerExchange {
                peer,
                num_peers,
                reply,
            })
            .await
            .map_err(|_| NodeError::NodeStopped)?;
        let rpc = rx
            .await
            .map_err(|_| NodeError::NodeStopped)?
            .map_err(NodeError::Command)?;
        Ok(rpc
            .enrs()
            .iter()
            .filter_map(|b| waku_discv5::enr_from_bytes(b))
            .collect())
    }

    /// Subscribe to a content filter on `peer` (12/WAKU2-FILTER). Matching
    /// messages arrive as [`Event::FilterMessage`]. Empty `content_topics`
    /// matches all content on the topic.
    pub async fn filter_subscribe(
        &self,
        peer: PeerId,
        pubsub_topic: impl Into<String>,
        content_topics: Vec<String>,
    ) -> Result<FilterSubscribeResponse, NodeError> {
        self.filter_request(
            peer,
            subscribe_type::SUBSCRIBE,
            Some(pubsub_topic.into()),
            content_topics,
        )
        .await
    }

    /// Remove a content filter previously installed on `peer`.
    pub async fn filter_unsubscribe(
        &self,
        peer: PeerId,
        pubsub_topic: impl Into<String>,
        content_topics: Vec<String>,
    ) -> Result<FilterSubscribeResponse, NodeError> {
        self.filter_request(
            peer,
            subscribe_type::UNSUBSCRIBE,
            Some(pubsub_topic.into()),
            content_topics,
        )
        .await
    }

    async fn filter_request(
        &self,
        peer: PeerId,
        kind: i32,
        pubsub_topic: Option<String>,
        content_topics: Vec<String>,
    ) -> Result<FilterSubscribeResponse, NodeError> {
        let (reply, rx) = oneshot::channel();
        let request = FilterSubscribeRequest {
            request_id: String::new(),
            filter_subscribe_type: kind,
            pubsub_topic,
            content_topics,
        };
        self.cmd_tx
            .send(Command::FilterSubscribe {
                peer,
                request: Box::new(request),
                reply,
            })
            .await
            .map_err(|_| NodeError::NodeStopped)?;
        rx.await
            .map_err(|_| NodeError::NodeStopped)?
            .map_err(NodeError::Command)
    }
}

fn build_swarm(config: &NodeConfig) -> Result<Swarm<WakuBehaviour>, NodeError> {
    let limits = ConnectionLimits::default()
        .with_max_established(Some(config.max_connections))
        .with_max_established_per_peer(Some(4));
    let swarm = libp2p::SwarmBuilder::with_existing_identity(config.keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| NodeError::Build(e.to_string()))?
        .with_behaviour(move |key| {
            let relay = waku_relay::build_relay()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let identify = identify::Behaviour::new(
                identify::Config::new("ipfs/id/1.0.0".into(), key.public())
                    .with_agent_version(format!("rs-waku/{}", env!("CARGO_PKG_VERSION"))),
            );
            Ok(WakuBehaviour {
                relay,
                identify,
                metadata: waku_metadata::build(),
                store_query: store_query::build(),
                lightpush: waku_lightpush::build(),
                peer_exchange: waku_peer_exchange::build(),
                filter_subscribe: waku_filter::build_subscribe(),
                filter_push: waku_filter::build_push(),
                connection_limits: connection_limits::Behaviour::new(limits),
            })
        })
        .map_err(|e| NodeError::Build(e.to_string()))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(config.idle_timeout))
        .build();
    Ok(swarm)
}

/// Build the swarm, start listening, and spawn the driver task.
pub async fn spawn(
    mut config: NodeConfig,
) -> Result<(NodeHandle, mpsc::Receiver<Event>), NodeError> {
    let mut swarm = build_swarm(&config)?;
    let peer_id = *swarm.local_peer_id();
    let local_cluster = config.cluster_id as u32;
    let local_meta = config.local_metadata();

    for addr in &config.listen_addrs {
        swarm
            .listen_on(addr.clone())
            .map_err(|e| NodeError::Listen(e.to_string()))?;
    }

    let store = config.store.take();
    let peer_book: PeerBook = Arc::new(Mutex::new(Vec::new()));
    let connected: Arc<Mutex<HashSet<PeerId>>> = Arc::new(Mutex::new(HashSet::new()));

    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let (evt_tx, evt_rx) = mpsc::channel(256);

    tokio::spawn(run(
        swarm,
        cmd_rx,
        evt_tx,
        local_cluster,
        local_meta,
        store.clone(),
        peer_book.clone(),
        connected.clone(),
        config.ip_colocation_limit,
    ));

    // Optionally start discv5 discovery, sharing the node's secp256k1 key.
    let mut discv5_enr = None;
    if let Some(settings) = config.discovery.take() {
        let key = keypair_to_combined(&config.keypair)?;

        // Resolve any enrtree:// URLs into additional bootstrap ENRs.
        let mut bootstrap = settings.bootstrap;
        if !settings.dns_bootstrap.is_empty() {
            bootstrap.extend(resolve_dns_bootstrap(&settings.dns_bootstrap).await);
        }

        let mut dcfg =
            DiscoveryConfig::new(settings.udp_port).with_cluster(config.cluster_id, config.shards);
        dcfg.key = Some(key);
        dcfg.external_ip = Some(settings.advertised_ip);
        dcfg.tcp_port = Some(settings.advertised_tcp_port);
        dcfg.bootstrap = bootstrap.clone();

        let mut discovery = Discovery::new(dcfg).map_err(|e| NodeError::Build(e.to_string()))?;
        discovery
            .start()
            .await
            .map_err(|e| NodeError::Build(e.to_string()))?;
        discv5_enr = Some(discovery.local_enr());

        tokio::spawn(discovery_loop(
            discovery,
            bootstrap,
            cmd_tx.clone(),
            peer_book.clone(),
        ));
    }

    Ok((
        NodeHandle {
            peer_id,
            cmd_tx,
            discv5_enr,
            store,
            connected,
        },
        evt_rx,
    ))
}

/// Convert the node's secp256k1 libp2p key into a discv5 [`CombinedKey`].
fn keypair_to_combined(keypair: &Keypair) -> Result<CombinedKey, NodeError> {
    let kp = keypair
        .clone()
        .try_into_secp256k1()
        .map_err(|_| NodeError::Build("node identity must be secp256k1 for discv5".into()))?;
    let mut secret = kp.secret().to_bytes();
    CombinedKey::secp256k1_from_bytes(&mut secret).map_err(|e| NodeError::Build(e.to_string()))
}

/// Resolve `enrtree://` URLs to bootstrap ENRs via DNS (best-effort).
async fn resolve_dns_bootstrap(urls: &[String]) -> Vec<WakuEnr> {
    let resolver = match waku_discv5::HickoryResolver::system() {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "could not build DNS resolver; skipping enrtree bootstrap");
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for url in urls {
        match waku_discv5::resolve_enrtree(url, &resolver).await {
            Ok(enrs) => {
                tracing::info!(url, count = enrs.len(), "resolved enrtree bootstrap");
                out.extend(enrs);
            }
            Err(e) => tracing::warn!(url, error = %e, "enrtree resolution failed"),
        }
    }
    out
}

/// Dial bootstrap peers, then periodically discover and dial new cluster peers.
async fn discovery_loop(
    discovery: Discovery,
    bootstrap: Vec<WakuEnr>,
    cmd_tx: mpsc::Sender<Command>,
    peer_book: PeerBook,
) {
    let mut known: HashSet<PeerId> = HashSet::new();

    for enr in &bootstrap {
        remember_enr(&peer_book, enr.clone());
        if let Some(peer) = waku_discv5::enr_to_dialable(enr) {
            dial_discovered(&cmd_tx, &mut known, &peer_book, peer).await;
        }
    }

    loop {
        tokio::time::sleep(DISCOVERY_INTERVAL).await;
        if cmd_tx.is_closed() {
            break;
        }
        match discovery.discover_dialable().await {
            Ok(peers) => {
                for peer in peers {
                    dial_discovered(&cmd_tx, &mut known, &peer_book, peer).await;
                }
            }
            Err(e) => tracing::debug!(error = %e, "discovery query failed"),
        }
    }
}

async fn dial_discovered(
    cmd_tx: &mpsc::Sender<Command>,
    known: &mut HashSet<PeerId>,
    peer_book: &PeerBook,
    peer: waku_discv5::DiscoveredPeer,
) {
    remember_enr(peer_book, peer.enr.clone());
    if !known.insert(peer.peer_id) {
        return;
    }
    tracing::info!(peer = %peer.peer_id, "auto-dialing discovered peer");
    for addr in peer.addrs {
        let (reply, _rx) = oneshot::channel();
        if cmd_tx.send(Command::Dial { addr, reply }).await.is_err() {
            return;
        }
    }
}

#[allow(clippy::too_many_arguments)] // the swarm driver owns all shared state
async fn run(
    mut swarm: Swarm<WakuBehaviour>,
    mut cmd_rx: mpsc::Receiver<Command>,
    evt_tx: mpsc::Sender<Event>,
    local_cluster: u32,
    local_meta: WakuMetadata,
    store: Option<Arc<dyn MessageStore>>,
    peer_book: PeerBook,
    connected: Arc<Mutex<HashSet<PeerId>>>,
    ip_limit: usize,
) {
    let mut pending = Pending::default();
    let mut filters: FilterRegistry = HashMap::new();
    let mut limits = RateLimiters::default();
    let mut ip_counts: HashMap<IpAddr, usize> = HashMap::new();
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                Some(Command::Shutdown) | None => break, // shutdown or all handles dropped
                Some(cmd) => handle_command(&mut swarm, cmd, &mut pending),
            },
            event = swarm.select_next_some() => {
                if handle_event(
                    &mut swarm,
                    event,
                    &evt_tx,
                    local_cluster,
                    &local_meta,
                    &store,
                    &mut pending,
                    &peer_book,
                    &mut filters,
                    &connected,
                    &mut limits,
                    &mut ip_counts,
                    ip_limit,
                )
                .await
                .is_err()
                {
                    break; // event consumer gone
                }
            }
        }
    }
}

fn handle_command(swarm: &mut Swarm<WakuBehaviour>, cmd: Command, pending: &mut Pending) {
    match cmd {
        Command::Subscribe { shard, reply } => {
            let topic = waku_relay::shard_topic(shard);
            let res = swarm
                .behaviour_mut()
                .relay
                .subscribe(&topic)
                .map(|_| ())
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        Command::Publish {
            shard,
            message,
            reply,
        } => {
            let topic = waku_relay::shard_topic(shard);
            let data = prost::Message::encode_to_vec(&*message);
            let hash = deterministic_hash(&shard.pubsub_topic(), &message);
            let res = match swarm.behaviour_mut().relay.publish(topic.hash(), data) {
                Ok(id) => {
                    id.0.as_slice()
                        .try_into()
                        .map_err(|_| "gossipsub returned a non-32-byte message id".to_string())
                }
                Err(e) => Err(e.to_string()),
            };
            if let Ok(id) = &res {
                debug_assert_eq!(*id, hash, "gossipsub id diverged from RFC-14 hash");
            }
            let _ = reply.send(res);
        }
        Command::Dial { addr, reply } => {
            let res = swarm.dial(addr).map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        Command::StoreQuery {
            peer,
            request,
            reply,
        } => {
            let id = swarm
                .behaviour_mut()
                .store_query
                .send_request(&peer, *request);
            pending.store_queries.insert(id, reply);
        }
        Command::LightPush {
            peer,
            request,
            reply,
        } => {
            let id = swarm
                .behaviour_mut()
                .lightpush
                .send_request(&peer, *request);
            pending.lightpush.insert(id, reply);
        }
        Command::PeerExchange {
            peer,
            num_peers,
            reply,
        } => {
            let id = swarm
                .behaviour_mut()
                .peer_exchange
                .send_request(&peer, PeerExchangeRpc::query(num_peers));
            pending.peer_exchange.insert(id, reply);
        }
        Command::FilterSubscribe {
            peer,
            request,
            reply,
        } => {
            let id = swarm
                .behaviour_mut()
                .filter_subscribe
                .send_request(&peer, *request);
            pending.filter.insert(id, reply);
        }
        Command::Shutdown => {} // handled in `run`'s select before reaching here
    }
}

#[allow(clippy::too_many_arguments)] // central event dispatch; all inputs are needed
async fn handle_event(
    swarm: &mut Swarm<WakuBehaviour>,
    event: SwarmEvent<WakuBehaviourEvent>,
    evt_tx: &mpsc::Sender<Event>,
    local_cluster: u32,
    local_meta: &WakuMetadata,
    store: &Option<Arc<dyn MessageStore>>,
    pending: &mut Pending,
    peer_book: &PeerBook,
    filters: &mut FilterRegistry,
    connected: &Mutex<HashSet<PeerId>>,
    limits: &mut RateLimiters,
    ip_counts: &mut HashMap<IpAddr, usize>,
    ip_limit: usize,
) -> Result<(), ()> {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            evt_tx
                .send(Event::Listening(address))
                .await
                .map_err(|_| ())?;
        }
        SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            // ip-colocation guard: cap concurrent connections per remote IP.
            if ip_limit > 0 {
                if let Some(ip) = multiaddr_ip(endpoint.get_remote_address()) {
                    let count = ip_counts.entry(ip).or_insert(0);
                    *count += 1;
                    if *count > ip_limit {
                        *count -= 1;
                        tracing::warn!(%peer_id, %ip, "ip-colocation limit exceeded; disconnecting");
                        let _ = swarm.disconnect_peer_id(peer_id);
                        return Ok(());
                    }
                }
            }
            connected.lock().expect("connected lock").insert(peer_id);
            // Kick off the metadata handshake immediately.
            swarm
                .behaviour_mut()
                .metadata
                .send_request(&peer_id, local_meta.clone());
            evt_tx
                .send(Event::PeerConnected(peer_id))
                .await
                .map_err(|_| ())?;
        }
        SwarmEvent::ConnectionClosed {
            peer_id, endpoint, ..
        } => {
            if let Some(ip) = multiaddr_ip(endpoint.get_remote_address()) {
                if let Some(count) = ip_counts.get_mut(&ip) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        ip_counts.remove(&ip);
                    }
                }
            }
            connected.lock().expect("connected lock").remove(&peer_id);
            evt_tx
                .send(Event::PeerDisconnected(peer_id))
                .await
                .map_err(|_| ())?;
        }
        SwarmEvent::Behaviour(WakuBehaviourEvent::Relay(gossipsub::Event::Message {
            propagation_source,
            message_id,
            message,
        })) => {
            let topic = message.topic.as_str().to_string();
            // Decide the validation verdict, emit to the app on Accept, then
            // report the verdict to gossipsub so it forwards/penalizes correctly.
            let acceptance: MessageAcceptance = match (
                ShardId::parse(&topic),
                WakuMessage::try_decode(&message.data),
            ) {
                (Ok(shard), Some(decoded)) => {
                    let facts = MessageFacts {
                        // RLN enforcement is off until we sync a membership
                        // tree to verify inbound proofs (so rln = Absent).
                        timestamp_gap_secs: decoded
                            .timestamp
                            .map(|ts| (now_unix_nanos() - ts) / 1_000_000_000),
                        rln: RlnStatus::Absent,
                        shard_saturated: false,
                    };
                    let verdict = validate(&facts, &ValidationPolicy::default());
                    if verdict == Validation::Accept {
                        // Store-on-relay: persist non-ephemeral accepted messages
                        // (off the hot path, so DB latency never stalls the swarm).
                        if let Some(store) = store {
                            if decoded.ephemeral != Some(true) {
                                let store = store.clone();
                                let pt = topic.clone();
                                let to_store = decoded.clone();
                                let rx_time = now_unix_nanos();
                                tokio::spawn(async move {
                                    if let Err(e) = store.put(&pt, &to_store, rx_time).await {
                                        tracing::warn!(error = %e, "failed to store message");
                                    }
                                });
                            }
                        }
                        // Push to any filter subscribers whose filter matches.
                        push_filter_matches(swarm, filters, &topic, &decoded);

                        let id = message_id
                            .0
                            .as_slice()
                            .try_into()
                            .unwrap_or_else(|_| deterministic_hash(&topic, &decoded));
                        evt_tx
                            .send(Event::Message {
                                shard,
                                message: decoded,
                                id,
                                propagation_source,
                            })
                            .await
                            .map_err(|_| ())?;
                    }
                    verdict.into()
                }
                _ => {
                    tracing::debug!(topic, "rejecting undecodable relay message");
                    Validation::Reject.into()
                }
            };
            swarm
                .behaviour_mut()
                .relay
                .report_message_validation_result(&message_id, &propagation_source, acceptance);
        }
        SwarmEvent::Behaviour(WakuBehaviourEvent::Metadata(request_response::Event::Message {
            peer,
            message,
            ..
        })) => {
            let event = handle_metadata(swarm, peer, message, local_cluster, local_meta);
            if let Some(ev) = event {
                evt_tx.send(ev).await.map_err(|_| ())?;
            }
        }
        SwarmEvent::Behaviour(WakuBehaviourEvent::StoreQuery(
            request_response::Event::Message { peer, message, .. },
        )) => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let response = if !limits.store.allow(peer, Instant::now()) {
                    StoreQueryResponse {
                        request_id: request.request_id.clone(),
                        status_code: Some(429),
                        status_desc: Some("rate limited".into()),
                        messages: Vec::new(),
                        pagination_cursor: None,
                    }
                } else {
                    match store {
                        Some(s) => store_query::serve(s.as_ref(), &request).await,
                        None => StoreQueryResponse {
                            request_id: request.request_id.clone(),
                            status_code: Some(503),
                            status_desc: Some("store not enabled".into()),
                            messages: Vec::new(),
                            pagination_cursor: None,
                        },
                    }
                };
                let _ = swarm
                    .behaviour_mut()
                    .store_query
                    .send_response(channel, response);
            }
            request_response::Message::Response {
                request_id,
                response,
            } => {
                if let Some(tx) = pending.store_queries.remove(&request_id) {
                    let _ = tx.send(Ok(response));
                }
            }
        },
        SwarmEvent::Behaviour(WakuBehaviourEvent::StoreQuery(
            request_response::Event::OutboundFailure {
                request_id, error, ..
            },
        )) => {
            if let Some(tx) = pending.store_queries.remove(&request_id) {
                let _ = tx.send(Err(error.to_string()));
            }
        }
        SwarmEvent::Behaviour(WakuBehaviourEvent::Lightpush(
            request_response::Event::Message { peer, message, .. },
        )) => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let response = if !limits.lightpush.allow(peer, Instant::now()) {
                    LightpushResponse::error(request.request_id.clone(), 429, "rate limited")
                } else {
                    serve_lightpush(swarm, &request)
                };
                let _ = swarm
                    .behaviour_mut()
                    .lightpush
                    .send_response(channel, response);
            }
            request_response::Message::Response {
                request_id,
                response,
            } => {
                if let Some(tx) = pending.lightpush.remove(&request_id) {
                    let _ = tx.send(Ok(response));
                }
            }
        },
        SwarmEvent::Behaviour(WakuBehaviourEvent::Lightpush(
            request_response::Event::OutboundFailure {
                request_id, error, ..
            },
        )) => {
            if let Some(tx) = pending.lightpush.remove(&request_id) {
                let _ = tx.send(Err(error.to_string()));
            }
        }
        SwarmEvent::Behaviour(WakuBehaviourEvent::PeerExchange(
            request_response::Event::Message { message, .. },
        )) => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let num = request.requested().unwrap_or(0) as usize;
                let enrs: Vec<Vec<u8>> = {
                    let book = peer_book.lock().expect("peer book lock");
                    book.iter()
                        .take(num)
                        .map(waku_discv5::enr_to_bytes)
                        .collect()
                };
                let _ = swarm
                    .behaviour_mut()
                    .peer_exchange
                    .send_response(channel, PeerExchangeRpc::response(enrs));
            }
            request_response::Message::Response {
                request_id,
                response,
            } => {
                if let Some(tx) = pending.peer_exchange.remove(&request_id) {
                    let _ = tx.send(Ok(response));
                }
            }
        },
        SwarmEvent::Behaviour(WakuBehaviourEvent::PeerExchange(
            request_response::Event::OutboundFailure {
                request_id, error, ..
            },
        )) => {
            if let Some(tx) = pending.peer_exchange.remove(&request_id) {
                let _ = tx.send(Err(error.to_string()));
            }
        }
        // filter-subscribe: we are the full node; update the registry and ack.
        SwarmEvent::Behaviour(WakuBehaviourEvent::FilterSubscribe(
            request_response::Event::Message { peer, message, .. },
        )) => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let response = apply_filter_request(filters, peer, &request);
                let _ = swarm
                    .behaviour_mut()
                    .filter_subscribe
                    .send_response(channel, response);
            }
            request_response::Message::Response {
                request_id,
                response,
            } => {
                if let Some(tx) = pending.filter.remove(&request_id) {
                    let _ = tx.send(Ok(response));
                }
            }
        },
        SwarmEvent::Behaviour(WakuBehaviourEvent::FilterSubscribe(
            request_response::Event::OutboundFailure {
                request_id, error, ..
            },
        )) => {
            if let Some(tx) = pending.filter.remove(&request_id) {
                let _ = tx.send(Err(error.to_string()));
            }
        }
        // filter-push: we are the client; surface the message and ack.
        SwarmEvent::Behaviour(WakuBehaviourEvent::FilterPush(
            request_response::Event::Message {
                message:
                    request_response::Message::Request {
                        request, channel, ..
                    },
                ..
            },
        )) => {
            if let Some(m) = request.waku_message {
                evt_tx
                    .send(Event::FilterMessage {
                        pubsub_topic: request.pubsub_topic,
                        message: m,
                    })
                    .await
                    .map_err(|_| ())?;
            }
            let _ = swarm
                .behaviour_mut()
                .filter_push
                .send_response(channel, FilterPushResponse { status_code: 200 });
        }
        _ => {}
    }
    Ok(())
}

/// Process a metadata request/response.
///
/// We always answer inbound requests (so the peer can validate *us*), and we
/// validate inbound responses to *our* request: a mismatching cluster id means
/// we disconnect the peer (66/WAKU2-METADATA). Since both peers request on
/// connect, both validate — and driving disconnection from the response side
/// avoids a race where disconnecting on a request drops our own counter-request.
fn handle_metadata(
    swarm: &mut Swarm<WakuBehaviour>,
    peer: PeerId,
    message: request_response::Message<WakuMetadata, WakuMetadata>,
    local_cluster: u32,
    local_meta: &WakuMetadata,
) -> Option<Event> {
    match message {
        request_response::Message::Request { channel, .. } => {
            let _ = swarm
                .behaviour_mut()
                .metadata
                .send_response(channel, local_meta.clone());
            None
        }
        request_response::Message::Response { response, .. } => match response.cluster_id {
            Some(theirs) if theirs != local_cluster => {
                tracing::info!(%peer, theirs, ours = local_cluster, "cluster mismatch; disconnecting");
                let _ = swarm.disconnect_peer_id(peer);
                Some(Event::MetadataMismatch {
                    peer,
                    theirs: Some(theirs),
                })
            }
            _ => None,
        },
    }
}

/// Publish a light-pushed message into gossipsub and report the outcome
/// (19/WAKU2-LIGHTPUSH v3 server side).
fn serve_lightpush(swarm: &mut Swarm<WakuBehaviour>, req: &LightpushRequest) -> LightpushResponse {
    let Some(message) = &req.message else {
        return LightpushResponse::error(req.request_id.clone(), 400, "missing message");
    };
    let topic = gossipsub::IdentTopic::new(req.pubsub_topic.clone());
    let data = prost::Message::encode_to_vec(message);
    match swarm.behaviour_mut().relay.publish(topic.hash(), data) {
        Ok(_) => {
            let count = swarm.behaviour().relay.mesh_peers(&topic.hash()).count() as u32;
            LightpushResponse::ok(req.request_id.clone(), count)
        }
        Err(e) => LightpushResponse::error(req.request_id.clone(), 503, e.to_string()),
    }
}

/// Local helper: decode WakuMessage, returning `None` on failure.
trait TryDecode: Sized {
    fn try_decode(bytes: &[u8]) -> Option<Self>;
}
impl TryDecode for WakuMessage {
    fn try_decode(bytes: &[u8]) -> Option<Self> {
        prost::Message::decode(bytes).ok()
    }
}
