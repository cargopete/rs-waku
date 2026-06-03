//! 13/WAKU2-STORE v3 over the wire: node B queries node A's store via the
//! `/vac/waku/store-query/3.0.0` request/response protocol.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{sleep, timeout};
use waku_core::WakuMessage;
use waku_node::{spawn, Event, NodeConfig};
use waku_store::store_query::StoreQueryRequest;
use waku_store::{MessageStore, SqliteStore};

#[tokio::test]
async fn query_a_peers_store_over_the_wire() {
    let _ = tracing_subscriber::fmt::try_init();

    // Node A: a store node with three messages already stored.
    let store: Arc<dyn MessageStore> = Arc::new(SqliteStore::in_memory().await.unwrap());
    for i in 0..3i64 {
        let mut m = WakuMessage::new("/app/1/chat/proto", vec![i as u8]);
        m.timestamp = Some(100 + i);
        store.put("/waku/2/rs/1/0", &m, 0).await.unwrap();
    }

    let mut cfg_a = NodeConfig::new().with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    cfg_a.store = Some(store);
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

    // Node B: a client with no store of its own.
    let (b, _b_events) = spawn(NodeConfig::new()).await.expect("spawn B");
    b.dial(addr).await.expect("dial A");

    let request = StoreQueryRequest {
        request_id: "q1".into(),
        include_data: true,
        content_topics: vec!["/app/1/chat/proto".into()],
        pagination_forward: true,
        ..Default::default()
    };

    // Retry until the connection is up and the query succeeds.
    let resp = timeout(Duration::from_secs(15), async {
        loop {
            match b.store_query(a.peer_id(), request.clone()).await {
                Ok(r) if !r.messages.is_empty() => break r,
                _ => sleep(Duration::from_millis(300)).await,
            }
        }
    })
    .await
    .expect("B should get a store response from A");

    assert_eq!(resp.status_code, Some(200));
    assert_eq!(resp.messages.len(), 3);
    assert_eq!(resp.messages[0].message.as_ref().unwrap().payload, vec![0]);
}
