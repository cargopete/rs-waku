//! 66/WAKU2-METADATA: a node MUST disconnect peers on a cluster-id mismatch,
//! and MUST stay connected when clusters agree.

use std::time::Duration;

use tokio::time::timeout;
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
async fn mismatched_clusters_disconnect() {
    let _ = tracing_subscriber::fmt::try_init();

    // Node A on cluster 1, node B on cluster 99.
    let cfg_a = NodeConfig::new()
        .with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .with_cluster(1, vec![0]);
    let (_a, mut a_events) = spawn(cfg_a).await.expect("spawn A");
    let addr = listen_addr(&mut a_events).await;
    tokio::spawn(async move { while a_events.recv().await.is_some() {} });

    let (b, mut b_events) = spawn(NodeConfig::new().with_cluster(99, vec![0]))
        .await
        .expect("spawn B");
    b.dial(addr).await.expect("dial");

    // B must observe a metadata mismatch and drop the peer.
    let mismatched = timeout(Duration::from_secs(10), async {
        loop {
            match b_events.recv().await {
                Some(Event::MetadataMismatch { theirs, .. }) => break theirs,
                Some(_) => continue,
                None => panic!("event stream closed"),
            }
        }
    })
    .await
    .expect("B should detect a cluster mismatch");

    assert_eq!(mismatched, Some(1));
}

#[tokio::test]
async fn matching_clusters_stay_connected() {
    let _ = tracing_subscriber::fmt::try_init();

    let cfg_a = NodeConfig::new()
        .with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .with_cluster(1, vec![0, 1, 2]);
    let (_a, mut a_events) = spawn(cfg_a).await.expect("spawn A");
    let addr = listen_addr(&mut a_events).await;
    tokio::spawn(async move { while a_events.recv().await.is_some() {} });

    let (b, mut b_events) = spawn(NodeConfig::new().with_cluster(1, vec![0, 1, 2]))
        .await
        .expect("spawn B");
    b.dial(addr).await.expect("dial");

    // Connect, and crucially NOT see a mismatch within a healthy window.
    let mut connected = false;
    let outcome: Result<Result<(), &str>, _> = timeout(Duration::from_secs(3), async {
        loop {
            match b_events.recv().await {
                Some(Event::PeerConnected(_)) => connected = true,
                Some(Event::MetadataMismatch { .. }) => return Err("unexpected mismatch"),
                Some(_) => continue,
                None => return Err("stream closed"),
            }
        }
    })
    .await;

    // The timeout elapsing (Err) is the success path: connected, no mismatch.
    assert!(connected, "B should have connected to A");
    assert!(outcome.is_err(), "no mismatch should have been reported");
}
