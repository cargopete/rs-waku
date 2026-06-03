//! Store-on-relay: a node with a store persists the relay messages it accepts,
//! and they're then queryable through the store.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{sleep, timeout};
use waku_core::{deterministic_hash, ShardId, WakuMessage};
use waku_node::{spawn, Event, NodeConfig};
use waku_store::{MessageStore, SqliteStore, StoreQuery};

#[tokio::test]
async fn accepted_messages_are_persisted_and_queryable() {
    let _ = tracing_subscriber::fmt::try_init();
    let shard = ShardId::new(1, 0);

    // Node A listens and stores what it accepts.
    let store: Arc<dyn MessageStore> = Arc::new(SqliteStore::in_memory().await.unwrap());
    let mut cfg_a = NodeConfig::new().with_listen_addr("/ip4/127.0.0.1/tcp/0".parse().unwrap());
    cfg_a.store = Some(store.clone());
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

    // Node B dials A and publishes.
    let (b, _b_events) = spawn(NodeConfig::new()).await.expect("spawn B");
    a.subscribe(shard).await.unwrap();
    b.subscribe(shard).await.unwrap();
    b.dial(addr).await.unwrap();

    let msg = WakuMessage::new("/app/1/store/proto", b"persist me".to_vec());
    let expected = deterministic_hash(&shard.pubsub_topic(), &msg);

    // Publish from B until A has stored it.
    let found = timeout(Duration::from_secs(20), async {
        loop {
            let _ = b.publish(shard, msg.clone()).await;
            sleep(Duration::from_millis(300)).await;
            if let Some(m) = store.get(&expected).await.unwrap() {
                break m;
            }
        }
    })
    .await
    .expect("A should store the relayed message");

    assert_eq!(found.payload, msg.payload);

    // And it's queryable by content topic.
    let res = store
        .query(&StoreQuery::new().content_topic("/app/1/store/proto"))
        .await
        .unwrap();
    assert_eq!(res.messages.len(), 1);
    assert_eq!(res.messages[0].hash, expected);
}
