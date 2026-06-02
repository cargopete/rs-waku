//! Swarm composition + the async driver.
//!
//! One libp2p `Swarm` is owned by a single task ([`run`]); everything else talks
//! to it over channels. [`spawn`] returns a [`NodeHandle`] (commands) plus an
//! event stream. This is the seam through which `waku-store`, `waku-rln`, the
//! REST API, etc. will later attach.

use std::time::Duration;

use futures::StreamExt;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{gossipsub, identify, identity::Keypair, noise, tcp, yamux, Multiaddr, PeerId, Swarm};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use waku_core::{deterministic_hash, MessageHash, ShardId, WakuMessage};

/// The composed Waku network behaviour. Milestone 1 carries relay + identify;
/// metadata, discv5, store, filter, … slot in as further fields.
#[derive(NetworkBehaviour)]
pub struct WakuBehaviour {
    pub relay: gossipsub::Behaviour,
    pub identify: identify::Behaviour,
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
}

impl NodeConfig {
    /// Fresh ed25519 identity, no listen addresses, 60 s idle timeout.
    pub fn new() -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            listen_addrs: Vec::new(),
            idle_timeout: Duration::from_secs(60),
        }
    }

    pub fn with_listen_addr(mut self, addr: Multiaddr) -> Self {
        self.listen_addrs.push(addr);
        self
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
            Ok(WakuBehaviour { relay, identify })
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

    for addr in &config.listen_addrs {
        swarm
            .listen_on(addr.clone())
            .map_err(|e| NodeError::Listen(e.to_string()))?;
    }

    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let (evt_tx, evt_rx) = mpsc::channel(256);

    tokio::spawn(run(swarm, cmd_rx, evt_tx));

    Ok((NodeHandle { peer_id, cmd_tx }, evt_rx))
}

async fn run(
    mut swarm: Swarm<WakuBehaviour>,
    mut cmd_rx: mpsc::Receiver<Command>,
    evt_tx: mpsc::Sender<Event>,
) {
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                Some(cmd) => handle_command(&mut swarm, cmd),
                None => break, // all handles dropped
            },
            event = swarm.select_next_some() => {
                if handle_swarm_event(event, &evt_tx).await.is_err() {
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
            // Compute the expected hash up front so we can return it even if the
            // local mesh is momentarily empty (we just published it locally).
            let hash = deterministic_hash(&shard.pubsub_topic(), &message);
            let res = match swarm.behaviour_mut().relay.publish(topic.hash(), data) {
                Ok(id) => {
                    id.0.as_slice()
                        .try_into()
                        .map_err(|_| "gossipsub returned a non-32-byte message id".to_string())
                }
                Err(e) => Err(e.to_string()),
            };
            // Sanity: the id gossipsub computed must equal our deterministic hash.
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

async fn handle_swarm_event(
    event: SwarmEvent<WakuBehaviourEvent>,
    evt_tx: &mpsc::Sender<Event>,
) -> Result<(), ()> {
    let emit = |e: Event| evt_tx.send(e);
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            emit(Event::Listening(address)).await.map_err(|_| ())?
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            emit(Event::PeerConnected(peer_id)).await.map_err(|_| ())?
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => emit(Event::PeerDisconnected(peer_id))
            .await
            .map_err(|_| ())?,
        SwarmEvent::Behaviour(WakuBehaviourEvent::Relay(gossipsub::Event::Message {
            propagation_source,
            message_id,
            message,
        })) => {
            let topic = message.topic.as_str();
            match (
                ShardId::parse(topic),
                WakuMessage::try_decode(&message.data),
            ) {
                (Ok(shard), Some(decoded)) => {
                    let id = message_id
                        .0
                        .as_slice()
                        .try_into()
                        .unwrap_or_else(|_| deterministic_hash(topic, &decoded));
                    emit(Event::Message {
                        shard,
                        message: decoded,
                        id,
                        propagation_source,
                    })
                    .await
                    .map_err(|_| ())?
                }
                _ => {
                    tracing::debug!(topic, "dropping undecodable relay message");
                }
            }
        }
        _ => {}
    }
    Ok(())
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
