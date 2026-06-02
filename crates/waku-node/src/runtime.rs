//! Swarm composition + the async driver.
//!
//! One libp2p `Swarm` is owned by a single task ([`run`]); everything else talks
//! to it over channels. [`spawn`] returns a [`NodeHandle`] (commands) plus an
//! event stream. This is the seam through which `waku-store`, `waku-rln`, the
//! REST API, etc. will later attach.

use std::time::Duration;

use futures::StreamExt;
use libp2p::request_response;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{gossipsub, identify, identity::Keypair, noise, tcp, yamux, Multiaddr, PeerId, Swarm};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use waku_core::{deterministic_hash, MessageHash, ShardId, WakuMessage, TWN};
use waku_metadata::WakuMetadata;

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

/// Configuration for the libp2p swarm underlying a node.
pub struct NodeConfig {
    pub keypair: Keypair,
    pub listen_addrs: Vec<Multiaddr>,
    pub idle_timeout: Duration,
    /// Cluster id advertised in the 66/WAKU2-METADATA handshake.
    pub cluster_id: u16,
    /// Shards advertised in the metadata handshake.
    pub shards: Vec<u16>,
}

impl NodeConfig {
    /// Fresh ed25519 identity, no listen addresses, TWN cluster/shards.
    pub fn new() -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            listen_addrs: Vec::new(),
            idle_timeout: Duration::from_secs(60),
            cluster_id: TWN.cluster_id,
            shards: (0..TWN.shard_count).collect(),
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
}

impl NodeHandle {
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
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
pub async fn spawn(config: NodeConfig) -> Result<(NodeHandle, mpsc::Receiver<Event>), NodeError> {
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

    Ok((NodeHandle { peer_id, cmd_tx }, evt_rx))
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
