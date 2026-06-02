//! Two local discv5 nodes establish a session: B bootstraps from A and queries
//! it; A, which started with an empty table, learns B as a valid Waku peer.

use std::net::UdpSocket;
use std::time::Duration;

use tokio::time::{sleep, timeout};
use waku_discv5::{enr_relay_shards, Discovery, DiscoveryConfig};

/// Grab a currently-free UDP port on loopback (small race, fine for a test).
fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn nodes_discover_each_other_over_discv5() {
    let _ = tracing_subscriber::fmt::try_init();
    let shards: Vec<u16> = (0..8).collect();

    // A starts with no bootstrap peers.
    let mut a =
        Discovery::new(DiscoveryConfig::new(free_udp_port()).with_cluster(1, shards.clone()))
            .expect("build A");
    a.start().await.expect("start A");
    let a_enr = a.local_enr();

    // B bootstraps from A.
    let mut b = Discovery::new(
        DiscoveryConfig::new(free_udp_port())
            .with_cluster(1, shards.clone())
            .with_bootstrap(vec![a_enr.clone()]),
    )
    .expect("build B");
    b.start().await.expect("start B");
    let b_id = b.local_enr().node_id();

    // Drive B's queries until A (initially empty) learns B via the session.
    let learned = timeout(Duration::from_secs(15), async {
        loop {
            let _ = b.discover().await;
            if let Some(enr) = a.table_peers().into_iter().find(|e| e.node_id() == b_id) {
                break enr;
            }
            sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("A should learn B over discv5");

    // A must see B as a cluster-1 Waku peer.
    let shards = enr_relay_shards(&learned).expect("B advertises relay shards");
    assert_eq!(shards.cluster_id, 1);
    assert_eq!(shards.shards, (0..8).collect::<Vec<_>>());
}
