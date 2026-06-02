//! Loopback interop: two rs-waku nodes exchange a WakuMessage over gossipsub,
//! and the received gossipsub id equals the RFC-14 deterministic hash.
//!
//! This is the local stand-in for the Milestone 1 nwaku interop gate: get the
//! plumbing (transport + StrictNoSign gossipsub + Waku message-id) provably
//! working between two of our own nodes before pointing it at nwaku.

use std::time::Duration;

use tokio::time::{sleep, timeout};
use waku_core::{deterministic_hash, ShardId, WakuMessage};
use waku_node::{spawn, Event, NodeConfig};

#[tokio::test]
async fn two_nodes_exchange_a_message() {
    let _ = tracing_subscriber::fmt::try_init();
    let shard = ShardId::new(1, 0);

    // Node A listens on an ephemeral TCP port.
    let cfg_a = NodeConfig::new().with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    let (a, mut a_events) = spawn(cfg_a).await.expect("spawn A");

    // Discover A's actual listen address.
    let addr = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Event::Listening(addr)) = a_events.recv().await {
                break addr;
            }
        }
    })
    .await
    .expect("A should start listening");

    // Keep draining A's events so its driver never blocks on backpressure.
    tokio::spawn(async move { while a_events.recv().await.is_some() {} });

    // Node B dials A.
    let (b, mut b_events) = spawn(NodeConfig::new()).await.expect("spawn B");

    a.subscribe(shard).await.expect("A subscribe");
    b.subscribe(shard).await.expect("B subscribe");
    b.dial(addr).await.expect("B dial A");

    let msg = WakuMessage::new("/toychat/2/huilong/proto", b"interop?".to_vec());

    // Publish from A, retrying until B receives — mesh + subscription gossip
    // takes a heartbeat or two to settle.
    let (got, id) = timeout(Duration::from_secs(20), async {
        loop {
            let _ = a.publish(shard, msg.clone()).await; // may be InsufficientPeers early
            match timeout(Duration::from_millis(400), b_events.recv()).await {
                Ok(Some(Event::Message { message, id, .. })) => break (message, id),
                _ => sleep(Duration::from_millis(150)).await,
            }
        }
    })
    .await
    .expect("B should receive A's message");

    assert_eq!(got.payload, msg.payload);
    assert_eq!(got.content_topic, msg.content_topic);

    // The id B saw must be the RFC-14 hash, not a libp2p default.
    let expected = deterministic_hash(&shard.pubsub_topic(), &msg);
    assert_eq!(
        id, expected,
        "received id must equal the deterministic hash"
    );
}
