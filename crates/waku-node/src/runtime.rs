//! Swarm composition + the async driver.
//!
//! One libp2p `Swarm` is owned by a single task ([`run`]); everything else talks
//! to it over channels. [`spawn`] returns a [`NodeHandle`] (commands) plus an
//! event stream. This is the seam through which `waku-store`, `waku-rln`, the
//! REST API, etc. will later attach.

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::time::Duration;

use futures::StreamExt;
use libp2p::request_response;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{gossipsub, identify, identity::Keypair, noise, tcp, yamux, Multiaddr, PeerId, Swarm};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use waku_core::{deterministic_hash, MessageHash, ShardId, WakuMessage, TWN};
use waku_discv5::{Discovery, DiscoveryConfig, Key as CombinedKey, WakuEnr};
use waku_metadata::WakuMetadata;

/// How often the discovery task runs a fresh discv5 query.
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(2);

/// The composed Waku network behaviour. Milestone 1 carries relay + identify +
/// metadata; discv5, store, filter, … slot in as further fields.
#[derive(NetworkBehaviour)]
pub struct WakuBehaviour {
    pub relay: gossipsub::Behaviour,
    pub identify: identify::Behaviour,
    pub metadata: waku_metadata::Behaviour,
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
}

/// Handle for issuing commands to a running node.
#[derive(Clone)]
pub struct NodeHandle {
    peer_id: PeerId,
    cmd_tx: mpsc::Sender<Command>,
    discv5_enr: Option<WakuEnr>,
}

impl NodeHandle {
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
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
}

fn build_swarm(config: &NodeConfig) -> Result<Swarm<WakuBehaviour>, NodeError> {
    let swarm = libp2p::SwarmBuilder::with_existing_identity(config.keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| NodeError::Build(e.to_string()))?
        .with_behaviour(|key| {
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

    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let (evt_tx, evt_rx) = mpsc::channel(256);

    tokio::spawn(run(swarm, cmd_rx, evt_tx, local_cluster, local_meta));

    // Optionally start discv5 discovery, sharing the node's secp256k1 key.
    let mut discv5_enr = None;
    if let Some(settings) = config.discovery.take() {
        let key = keypair_to_combined(&config.keypair)?;
        let mut dcfg =
            DiscoveryConfig::new(settings.udp_port).with_cluster(config.cluster_id, config.shards);
        dcfg.key = Some(key);
        dcfg.external_ip = Some(settings.advertised_ip);
        dcfg.tcp_port = Some(settings.advertised_tcp_port);
        dcfg.bootstrap = settings.bootstrap.clone();

        let mut discovery = Discovery::new(dcfg).map_err(|e| NodeError::Build(e.to_string()))?;
        discovery
            .start()
            .await
            .map_err(|e| NodeError::Build(e.to_string()))?;
        discv5_enr = Some(discovery.local_enr());

        tokio::spawn(discovery_loop(
            discovery,
            settings.bootstrap,
            cmd_tx.clone(),
        ));
    }

    Ok((
        NodeHandle {
            peer_id,
            cmd_tx,
            discv5_enr,
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

/// Dial bootstrap peers, then periodically discover and dial new cluster peers.
async fn discovery_loop(
    discovery: Discovery,
    bootstrap: Vec<WakuEnr>,
    cmd_tx: mpsc::Sender<Command>,
) {
    let mut known: HashSet<PeerId> = HashSet::new();

    for enr in &bootstrap {
        if let Some(peer) = waku_discv5::enr_to_dialable(enr) {
            dial_discovered(&cmd_tx, &mut known, peer).await;
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
                    dial_discovered(&cmd_tx, &mut known, peer).await;
                }
            }
            Err(e) => tracing::debug!(error = %e, "discovery query failed"),
        }
    }
}

async fn dial_discovered(
    cmd_tx: &mpsc::Sender<Command>,
    known: &mut HashSet<PeerId>,
    peer: waku_discv5::DiscoveredPeer,
) {
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

async fn run(
    mut swarm: Swarm<WakuBehaviour>,
    mut cmd_rx: mpsc::Receiver<Command>,
    evt_tx: mpsc::Sender<Event>,
    local_cluster: u32,
    local_meta: WakuMetadata,
) {
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                Some(cmd) => handle_command(&mut swarm, cmd),
                None => break, // all handles dropped
            },
            event = swarm.select_next_some() => {
                if handle_event(&mut swarm, event, &evt_tx, local_cluster, &local_meta)
                    .await
                    .is_err()
                {
                    break; // event consumer gone
                }
            }
        }
    }
}

fn handle_command(swarm: &mut Swarm<WakuBehaviour>, cmd: Command) {
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
    }
}

async fn handle_event(
    swarm: &mut Swarm<WakuBehaviour>,
    event: SwarmEvent<WakuBehaviourEvent>,
    evt_tx: &mpsc::Sender<Event>,
    local_cluster: u32,
    local_meta: &WakuMetadata,
) -> Result<(), ()> {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            evt_tx
                .send(Event::Listening(address))
                .await
                .map_err(|_| ())?;
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
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
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
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
            let topic = message.topic.as_str();
            if let (Ok(shard), Some(decoded)) = (
                ShardId::parse(topic),
                WakuMessage::try_decode(&message.data),
            ) {
                let id = message_id
                    .0
                    .as_slice()
                    .try_into()
                    .unwrap_or_else(|_| deterministic_hash(topic, &decoded));
                evt_tx
                    .send(Event::Message {
                        shard,
                        message: decoded,
                        id,
                        propagation_source,
                    })
                    .await
                    .map_err(|_| ())?;
            } else {
                tracing::debug!(topic, "dropping undecodable relay message");
            }
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

/// Local helper: decode WakuMessage, returning `None` on failure.
trait TryDecode: Sized {
    fn try_decode(bytes: &[u8]) -> Option<Self>;
}
impl TryDecode for WakuMessage {
    fn try_decode(bytes: &[u8]) -> Option<Self> {
        prost::Message::decode(bytes).ok()
    }
}
