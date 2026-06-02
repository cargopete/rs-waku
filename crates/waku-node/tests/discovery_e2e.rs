//! End-to-end Milestone 1: node B is given only node A's ENR, discovers/dials it
//! over the discv5 → libp2p bridge, completes the metadata handshake, and relays
//! a message. Discovery → connection → relay, with nothing hard-coded but the ENR.

use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::time::Duration;

use tokio::time::{sleep, timeout};
use waku_core::{ShardId, WakuMessage};
use waku_node::{spawn, DiscoverySettings, Event, NodeConfig};

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_listening(events: &mut tokio::sync::mpsc::Receiver<Event>) {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Event::Listening(_)) = events.recv().await {
                break;
            }
        }
    })
    .await
    .expect("should start listening");
}

#[tokio::test]
async fn discovers_dials_and_relays() {
    let _ = tracing_subscriber::fmt::try_init();
    let shard = ShardId::new(1, 0);
    let shards: Vec<u16> = (0..8).collect();

    // Node A: listens on TCP, runs discv5, advertises both in its ENR.
    let a_tcp = free_tcp_port();
    let mut cfg_a = NodeConfig::new()
        .with_listen_addr(format!("/ip4/127.0.0.1/tcp/{a_tcp}").parse().unwrap())
        .with_cluster(1, shards.clone());
    cfg_a.discovery = Some(DiscoverySettings {
        udp_port: free_udp_port(),
        advertised_ip: Ipv4Addr::LOCALHOST,
        advertised_tcp_port: a_tcp,
        bootstrap: vec![],
        dns_bootstrap: vec![],
    });
    let (a, mut a_events) = spawn(cfg_a).await.expect("spawn A");
    wait_listening(&mut a_events).await;
    let a_enr = a.discv5_enr().expect("A should have an ENR");
    tokio::spawn(async move { while a_events.recv().await.is_some() {} });

    // Node B: knows ONLY A's ENR. No static addresses.
    let b_tcp = free_tcp_port();
    let mut cfg_b = NodeConfig::new()
        .with_listen_addr(format!("/ip4/127.0.0.1/tcp/{b_tcp}").parse().unwrap())
        .with_cluster(1, shards.clone());
    cfg_b.discovery = Some(DiscoverySettings {
        udp_port: free_udp_port(),
        advertised_ip: Ipv4Addr::LOCALHOST,
        advertised_tcp_port: b_tcp,
        bootstrap: vec![a_enr],
        dns_bootstrap: vec![],
    });
    let (b, mut b_events) = spawn(cfg_b).await.expect("spawn B");

    a.subscribe(shard).await.expect("A subscribe");
    b.subscribe(shard).await.expect("B subscribe");

    // A publishes; B should receive it once discovery has dialed and the mesh formed.
    let msg = WakuMessage::new("/toychat/2/huilong/proto", b"found you".to_vec());
    let got = timeout(Duration::from_secs(30), async {
        loop {
            let _ = a.publish(shard, msg.clone()).await;
            match timeout(Duration::from_millis(500), b_events.recv()).await {
                Ok(Some(Event::Message { message, .. })) => break message,
                _ => sleep(Duration::from_millis(200)).await,
            }
        }
    })
    .await
    .expect("B should receive A's message via a discovered+dialed connection");

    assert_eq!(got.payload, msg.payload);
    assert_eq!(got.content_topic, msg.content_topic);
}
