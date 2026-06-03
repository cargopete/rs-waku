//! 34/WAKU2-PEER-EXCHANGE: client B asks full node A for peers; A returns the
//! ENRs it knows (here, node X — seeded into A as a bootstrap peer).

use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::time::Duration;

use tokio::time::{sleep, timeout};
use waku_node::{spawn, DiscoverySettings, Event, NodeConfig};

fn free_tcp() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn free_udp() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

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

fn discovery(tcp: u16, bootstrap: Vec<waku_node::WakuEnr>) -> DiscoverySettings {
    DiscoverySettings {
        udp_port: free_udp(),
        advertised_ip: Ipv4Addr::LOCALHOST,
        advertised_tcp_port: tcp,
        bootstrap,
        dns_bootstrap: vec![],
    }
}

#[tokio::test]
async fn peer_exchange_returns_known_enrs() {
    let _ = tracing_subscriber::fmt::try_init();

    // X: a node whose ENR we expect to be handed out.
    let x_tcp = free_tcp();
    let mut cfg_x =
        NodeConfig::new().with_listen_addr(format!("/ip4/127.0.0.1/tcp/{x_tcp}").parse().unwrap());
    cfg_x.discovery = Some(discovery(x_tcp, vec![]));
    let (x, mut x_events) = spawn(cfg_x).await.expect("spawn X");
    listen_addr(&mut x_events).await;
    let x_enr = x.discv5_enr().expect("X ENR");
    let x_id = x_enr.node_id();
    tokio::spawn(async move { while x_events.recv().await.is_some() {} });

    // A: peer-exchange server, seeded with X as a bootstrap peer.
    let a_tcp = free_tcp();
    let mut cfg_a =
        NodeConfig::new().with_listen_addr(format!("/ip4/127.0.0.1/tcp/{a_tcp}").parse().unwrap());
    cfg_a.discovery = Some(discovery(a_tcp, vec![x_enr]));
    let (a, mut a_events) = spawn(cfg_a).await.expect("spawn A");
    let a_addr = listen_addr(&mut a_events).await;
    tokio::spawn(async move { while a_events.recv().await.is_some() {} });

    // B: peer-exchange client.
    let (b, _b_events) = spawn(NodeConfig::new()).await.expect("spawn B");
    b.dial(a_addr).await.unwrap();

    let enrs = timeout(Duration::from_secs(15), async {
        loop {
            match b.peer_exchange(a.peer_id(), 10).await {
                Ok(list) if list.iter().any(|e| e.node_id() == x_id) => break list,
                _ => sleep(Duration::from_millis(300)).await,
            }
        }
    })
    .await
    .expect("B should receive X's ENR via peer exchange");

    assert!(enrs.iter().any(|e| e.node_id() == x_id));
}
