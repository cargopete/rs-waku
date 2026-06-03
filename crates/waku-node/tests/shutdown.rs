//! Graceful shutdown: `NodeHandle::shutdown` stops the swarm task, closing the
//! event stream and rejecting subsequent commands.

use std::time::Duration;

use tokio::time::timeout;
use waku_core::ShardId;
use waku_node::{spawn, NodeConfig};

#[tokio::test]
async fn shutdown_stops_the_node() {
    let (node, mut events) = spawn(NodeConfig::new()).await.expect("spawn");

    node.shutdown().await;

    // The event stream closes once the swarm task exits.
    let closed = timeout(Duration::from_secs(3), async {
        while events.recv().await.is_some() {}
    })
    .await;
    assert!(closed.is_ok(), "event stream should close after shutdown");

    // Further commands fail because the node has stopped.
    assert!(node.subscribe(ShardId::new(1, 0)).await.is_err());
}
