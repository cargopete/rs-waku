//! ip-colocation: a node caps concurrent connections from a single IP. With a
//! limit of 1, two loopback clients can't both stay connected.

use std::time::Duration;

use tokio::time::{sleep, timeout};
use waku_node::{spawn, Event, NodeConfig};

#[tokio::test]
async fn ip_colocation_limit_caps_connections_per_ip() {
    let _ = tracing_subscriber::fmt::try_init();

    // Node A: at most one connection from any single IP.
    let mut cfg_a = NodeConfig::new().with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    cfg_a.ip_colocation_limit = 1;
    let (a, mut a_events) = spawn(cfg_a).await.expect("spawn A");
    let addr = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Event::Listening(addr)) = a_events.recv().await {
                break addr;
            }
        }
    })
    .await
    .expect("A listening");
    tokio::spawn(async move { while a_events.recv().await.is_some() {} });

    // Two clients, both on 127.0.0.1, dial A.
    let (b, _b) = spawn(NodeConfig::new()).await.expect("spawn B");
    let (c, _c) = spawn(NodeConfig::new()).await.expect("spawn C");
    b.dial(addr.clone()).await.unwrap();
    c.dial(addr).await.unwrap();

    // After settling, A keeps at most one connection (same IP).
    sleep(Duration::from_secs(2)).await;
    assert_eq!(
        a.connected_peers().len(),
        1,
        "ip-colocation limit should hold A to one 127.0.0.1 connection"
    );
}
