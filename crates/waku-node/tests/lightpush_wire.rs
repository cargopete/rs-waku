//! 19/WAKU2-LIGHTPUSH v3: a light client hands a message to a full node, which
//! injects it into gossipsub and delivers it to a subscriber.
//!
//! Topology: B (light client) → A (full relay) → C (subscriber).

use std::time::Duration;

use tokio::time::{sleep, timeout};
use waku_core::{ShardId, WakuMessage};
use waku_node::{spawn, Event, NodeConfig};

async fn listen_addr(events: &mut tokio::sync::mpsc::Receiver<Event>) -> libp2p::Multiaddr {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Event::Listening(addr)) = events.recv().await {
                break addr;
            }
        }
    })
    .await
    .expect("should start listening")
}

#[tokio::test]
async fn lightpush_is_relayed_to_a_subscriber() {
    let _ = tracing_subscriber::fmt::try_init();
    let shard = ShardId::new(1, 0);

    // C: subscriber, listening.
    let cfg_c = NodeConfig::new().with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    let (c, mut c_events) = spawn(cfg_c).await.expect("spawn C");
    let c_addr = listen_addr(&mut c_events).await;

    // A: full relay node, listening; dials C so it can forward to it.
    let cfg_a = NodeConfig::new().with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    let (a, mut a_events) = spawn(cfg_a).await.expect("spawn A");
    let a_addr = listen_addr(&mut a_events).await;
    tokio::spawn(async move { while a_events.recv().await.is_some() {} });

    c.subscribe(shard).await.unwrap();
    a.subscribe(shard).await.unwrap();
    a.dial(c_addr).await.unwrap();

    // B: light client, dials A.
    let (b, _b_events) = spawn(NodeConfig::new()).await.expect("spawn B");
    b.dial(a_addr).await.unwrap();

    let msg = WakuMessage::new("/app/1/lp/proto", b"via lightpush".to_vec());

    // B light-pushes to A repeatedly until C receives it over the relay.
    let received = timeout(Duration::from_secs(20), async {
        loop {
            let _ = b
                .light_push(a.peer_id(), shard.pubsub_topic(), msg.clone())
                .await;
            match timeout(Duration::from_millis(400), c_events.recv()).await {
                Ok(Some(Event::Message { message, .. })) => break message,
                _ => sleep(Duration::from_millis(150)).await,
            }
        }
    })
    .await
    .expect("C should receive the light-pushed message via A");

    assert_eq!(received.payload, msg.payload);
    assert_eq!(received.content_topic, msg.content_topic);
}
