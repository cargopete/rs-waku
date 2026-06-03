//! 12/WAKU2-FILTER v2: light client B installs a content filter on full node A;
//! publisher C sends a matching message; A relays it and filter-pushes it to B.
//!
//! Topology: C (publisher) → A (full node, filter server) → push → B (filter client).

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
    .expect("listening")
}

#[tokio::test]
async fn filter_pushes_matching_messages_to_a_subscriber() {
    let _ = tracing_subscriber::fmt::try_init();
    let shard = ShardId::new(1, 0);
    let content_topic = "/app/1/filt/proto";

    // A: full node + filter server.
    let cfg_a = NodeConfig::new().with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    let (a, mut a_events) = spawn(cfg_a).await.expect("spawn A");
    let a_addr = listen_addr(&mut a_events).await;
    tokio::spawn(async move { while a_events.recv().await.is_some() {} });
    a.subscribe(shard).await.unwrap();

    // C: relay publisher.
    let (c, _c_events) = spawn(NodeConfig::new()).await.expect("spawn C");
    c.subscribe(shard).await.unwrap();
    c.dial(a_addr.clone()).await.unwrap();

    // B: filter client.
    let (b, mut b_events) = spawn(NodeConfig::new()).await.expect("spawn B");
    b.dial(a_addr).await.unwrap();

    // Install B's filter on A (retry until the connection is up).
    timeout(Duration::from_secs(10), async {
        loop {
            let ok = b
                .filter_subscribe(
                    a.peer_id(),
                    shard.pubsub_topic(),
                    vec![content_topic.into()],
                )
                .await
                .map(|r| r.status_code == 200)
                .unwrap_or(false);
            if ok {
                break;
            }
            sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("B should subscribe its filter on A");

    // C publishes a matching message until B receives the filter-push.
    let msg = WakuMessage::new(content_topic, b"filtered".to_vec());
    let got = timeout(Duration::from_secs(20), async {
        loop {
            let _ = c.publish(shard, msg.clone()).await;
            match timeout(Duration::from_millis(400), b_events.recv()).await {
                Ok(Some(Event::FilterMessage { message, .. })) => break message,
                _ => sleep(Duration::from_millis(150)).await,
            }
        }
    })
    .await
    .expect("B should receive the matching message via filter-push");

    assert_eq!(got.payload, msg.payload);
    assert_eq!(got.content_topic, msg.content_topic);
}
