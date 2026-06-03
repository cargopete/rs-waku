//! # waku-rest — nwaku-compatible REST API
//!
//! An `axum` server matching nwaku's OpenAPI (port 8645) — the surface the Waku
//! Python interop suite and operators drive. This first slice covers
//! `debug`/`health`, relay publish (autosharded), and 13/WAKU2-STORE v3 query.
//!
//! ⚠ JSON field shapes follow nwaku's REST spec (camelCase, base64 payloads);
//! verify against `waku-org/waku-rest-api` before relying on interop.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use waku_core::{autoshard, ContentTopic, MessageHash, NetworkPreset, ShardId, WakuMessage, TWN};
use waku_node::NodeHandle;
use waku_store::{MessageStore, StoreQuery};

/// Default nwaku REST port.
pub const DEFAULT_REST_PORT: u16 = 8645;

/// Max messages retained per content topic in the relay cache.
const CACHE_PER_TOPIC: usize = 100;

/// A per-content-topic cache of received relay messages, polled by
/// `GET /relay/v1/auto/messages/{contentTopic}`. Only subscribed (registered)
/// content topics are cached.
#[derive(Clone, Default)]
pub struct MessageCache {
    inner: Arc<Mutex<HashMap<String, VecDeque<WakuMessage>>>>,
}

impl MessageCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin caching messages for `content_topic`.
    pub fn register(&self, content_topic: String) {
        self.inner
            .lock()
            .expect("cache")
            .entry(content_topic)
            .or_default();
    }

    /// Record a received message if its content topic is being cached.
    pub fn record(&self, msg: WakuMessage) {
        let mut cache = self.inner.lock().expect("cache");
        if let Some(queue) = cache.get_mut(&msg.content_topic) {
            if queue.len() >= CACHE_PER_TOPIC {
                queue.pop_front();
            }
            queue.push_back(msg);
        }
    }

    /// Take and clear the cached messages for `content_topic`.
    fn drain(&self, content_topic: &str) -> Vec<WakuMessage> {
        self.inner
            .lock()
            .expect("cache")
            .get_mut(content_topic)
            .map(|q| q.drain(..).collect())
            .unwrap_or_default()
    }
}

/// Shared state for the REST handlers.
#[derive(Clone)]
pub struct AppState {
    pub node: NodeHandle,
    pub store: Option<Arc<dyn MessageStore>>,
    pub preset: NetworkPreset,
    pub version: String,
    pub cache: MessageCache,
}

impl AppState {
    pub fn new(node: NodeHandle, store: Option<Arc<dyn MessageStore>>) -> Self {
        Self {
            node,
            store,
            preset: TWN,
            version: env!("CARGO_PKG_VERSION").to_string(),
            cache: MessageCache::new(),
        }
    }

    /// The relay message cache (clone the handle to feed it from node events).
    pub fn cache(&self) -> MessageCache {
        self.cache.clone()
    }
}

/// Build the REST router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/debug/v1/version", get(version))
        .route("/debug/v1/info", get(info))
        .route("/health", get(health))
        .route("/relay/v1/auto/subscriptions", post(relay_subscribe))
        .route(
            "/relay/v1/auto/messages/{content_topic}",
            post(relay_publish).get(relay_messages),
        )
        .route("/lightpush/v1/message", post(lightpush))
        .route("/store/v3/messages", get(store_query))
        .route("/admin/v1/peers", get(admin_peers))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Serve the REST API on `addr` until the process ends.
pub async fn serve(addr: SocketAddr, state: AppState) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "REST API listening");
    axum::serve(listener, router(state)).await
}

// --- JSON wire types (nwaku REST shapes) ---

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestWakuMessage {
    /// Base64-encoded payload.
    pub payload: String,
    #[serde(default)]
    pub content_topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
    /// Base64-encoded metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
}

impl RestWakuMessage {
    fn into_message(self, content_topic: String) -> Result<WakuMessage, String> {
        let payload = B64
            .decode(&self.payload)
            .map_err(|e| format!("bad payload base64: {e}"))?;
        let meta = match self.meta {
            Some(m) => Some(
                B64.decode(&m)
                    .map_err(|e| format!("bad meta base64: {e}"))?,
            ),
            None => None,
        };
        Ok(WakuMessage {
            payload,
            content_topic,
            version: self.version,
            timestamp: self.timestamp,
            meta,
            rate_limit_proof: None,
            ephemeral: self.ephemeral,
        })
    }

    fn from_message(msg: &WakuMessage) -> Self {
        Self {
            payload: B64.encode(&msg.payload),
            content_topic: msg.content_topic.clone(),
            version: msg.version,
            timestamp: msg.timestamp,
            meta: msg.meta.as_ref().map(|m| B64.encode(m)),
            ephemeral: msg.ephemeral,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LightpushBody {
    pubsub_topic: Option<String>,
    message: RestWakuMessage,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InfoResponse {
    listen_addresses: Vec<String>,
    enr_uri: Option<String>,
    peer_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StoreResponse {
    messages: Vec<StoreMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination_cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StoreMessage {
    message_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<RestWakuMessage>,
    pubsub_topic: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoreParams {
    pubsub_topic: Option<String>,
    /// Comma-separated content topics.
    content_topics: Option<String>,
    start_time: Option<i64>,
    end_time: Option<i64>,
    page_size: Option<u64>,
    ascending: Option<bool>,
    /// Hex-encoded cursor (message hash).
    cursor: Option<String>,
    include_data: Option<bool>,
}

// --- handlers ---

async fn version(State(s): State<AppState>) -> String {
    s.version
}

async fn health(State(s): State<AppState>) -> impl IntoResponse {
    let peers = s.node.connected_peers().len();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "nodeHealth": "Ready", "connectedPeers": peers })),
    )
}

async fn info(State(s): State<AppState>) -> impl IntoResponse {
    Json(InfoResponse {
        listen_addresses: Vec::new(),
        enr_uri: s.node.discv5_enr().map(|e| e.to_base64()),
        peer_id: s.node.peer_id().to_string(),
    })
}

async fn relay_publish(
    State(s): State<AppState>,
    Path(content_topic): Path<String>,
    Json(body): Json<RestWakuMessage>,
) -> impl IntoResponse {
    let ct = match ContentTopic::parse(&content_topic) {
        Ok(ct) => ct,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let message = match body.into_message(content_topic.clone()) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let shard = autoshard(s.preset.cluster_id, &ct, s.preset.shard_count);

    match s.node.publish(shard, message).await {
        Ok(hash) => (StatusCode::OK, hex::encode(hash)).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

/// `POST /relay/v1/auto/subscriptions` — subscribe to content topics (autoshard)
/// and start caching their messages for polling.
async fn relay_subscribe(
    State(s): State<AppState>,
    Json(content_topics): Json<Vec<String>>,
) -> impl IntoResponse {
    for ct_str in &content_topics {
        let ct = match ContentTopic::parse(ct_str) {
            Ok(ct) => ct,
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        };
        let shard = autoshard(s.preset.cluster_id, &ct, s.preset.shard_count);
        if let Err(e) = s.node.subscribe(shard).await {
            return (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response();
        }
        s.cache.register(ct_str.clone());
    }
    StatusCode::OK.into_response()
}

/// `GET /relay/v1/auto/messages/{contentTopic}` — drain cached messages for a
/// subscribed content topic.
async fn relay_messages(
    State(s): State<AppState>,
    Path(content_topic): Path<String>,
) -> impl IntoResponse {
    let messages: Vec<RestWakuMessage> = s
        .cache
        .drain(&content_topic)
        .iter()
        .map(RestWakuMessage::from_message)
        .collect();
    Json(messages)
}

/// `POST /lightpush/v1/message` — the node injects the message into gossipsub.
async fn lightpush(State(s): State<AppState>, Json(req): Json<LightpushBody>) -> impl IntoResponse {
    let content_topic = req.message.content_topic.clone();
    let shard = match req.pubsub_topic.as_deref() {
        Some(topic) => match ShardId::parse(topic) {
            Ok(shard) => shard,
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        },
        None => match ContentTopic::parse(&content_topic) {
            Ok(ct) => autoshard(s.preset.cluster_id, &ct, s.preset.shard_count),
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        },
    };
    let message = match req.message.into_message(content_topic) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    match s.node.publish(shard, message).await {
        Ok(hash) => (StatusCode::OK, hex::encode(hash)).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

async fn store_query(
    State(s): State<AppState>,
    Query(params): Query<StoreParams>,
) -> impl IntoResponse {
    let Some(store) = s.store else {
        return (StatusCode::SERVICE_UNAVAILABLE, "store not enabled").into_response();
    };

    let cursor = match params.cursor.as_deref().map(parse_hash).transpose() {
        Ok(c) => c,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };

    let query = StoreQuery {
        pubsub_topic: params.pubsub_topic,
        content_topics: params
            .content_topics
            .map(|c| c.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default(),
        time_start_ns: params.start_time,
        time_end_ns: params.end_time,
        message_hashes: Vec::new(),
        include_data: params.include_data.unwrap_or(true),
        page_size: params.page_size.unwrap_or(20),
        forward: params.ascending.unwrap_or(true),
        cursor,
    };

    match store.query(&query).await {
        Ok(result) => {
            let messages = result
                .messages
                .into_iter()
                .map(|m| StoreMessage {
                    message_hash: hex::encode(m.hash),
                    message: m.message.as_ref().map(RestWakuMessage::from_message),
                    pubsub_topic: m.pubsub_topic,
                })
                .collect();
            Json(StoreResponse {
                messages,
                pagination_cursor: result.next_cursor.map(hex::encode),
            })
            .into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminPeer {
    peer_id: String,
    connected: bool,
}

/// `GET /admin/v1/peers` — currently connected peers.
async fn admin_peers(State(s): State<AppState>) -> impl IntoResponse {
    let peers: Vec<AdminPeer> = s
        .node
        .connected_peers()
        .into_iter()
        .map(|p| AdminPeer {
            peer_id: p.to_string(),
            connected: true,
        })
        .collect();
    Json(peers)
}

/// `GET /metrics` — Prometheus text exposition.
async fn metrics(State(s): State<AppState>) -> impl IntoResponse {
    use prometheus::{Encoder, Gauge, Registry, TextEncoder};

    let registry = Registry::new();
    let peers = Gauge::new("rs_waku_connected_peers", "Currently connected peers").unwrap();
    peers.set(s.node.connected_peers().len() as f64);
    let _ = registry.register(Box::new(peers));

    if let Some(store) = &s.store {
        let stored = Gauge::new("rs_waku_stored_messages", "Messages in the store").unwrap();
        stored.set(store.message_count().await.unwrap_or(0) as f64);
        let _ = registry.register(Box::new(stored));
    }

    // Per-protocol activity counters.
    let snap = s.node.metrics().snapshot();
    for (name, help, value) in [
        (
            "rs_waku_relay_messages_total",
            "Relay messages accepted",
            snap.relay_messages,
        ),
        (
            "rs_waku_store_queries_total",
            "Store queries served",
            snap.store_queries,
        ),
        (
            "rs_waku_lightpush_requests_total",
            "Lightpush requests served",
            snap.lightpush_requests,
        ),
        (
            "rs_waku_filter_pushes_total",
            "Filter pushes sent",
            snap.filter_pushes,
        ),
        (
            "rs_waku_rate_limited_total",
            "Requests rejected by rate limiting",
            snap.rate_limited,
        ),
    ] {
        let counter = prometheus::Counter::new(name, help).unwrap();
        counter.inc_by(value as f64);
        let _ = registry.register(Box::new(counter));
    }

    let mut buf = Vec::new();
    let _ = TextEncoder::new().encode(&registry.gather(), &mut buf);
    (StatusCode::OK, String::from_utf8_lossy(&buf).into_owned())
}

fn parse_hash(hex_str: &str) -> Result<MessageHash, String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("bad hex cursor: {e}"))?;
    bytes
        .as_slice()
        .try_into()
        .map_err(|_| "cursor must be 32 bytes".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt; // for `oneshot`
    use waku_node::{spawn, NodeConfig};
    use waku_store::SqliteStore;

    async fn test_state(store: Option<Arc<dyn MessageStore>>) -> AppState {
        let (node, _events) = spawn(NodeConfig::new()).await.unwrap();
        AppState::new(node, store)
    }

    async fn body_string(resp: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn version_and_health() {
        let app = router(test_state(None).await);

        let resp = app
            .clone()
            .oneshot(
                Request::get("/debug/v1/version")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp).await, env!("CARGO_PKG_VERSION"));

        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(body_string(resp).await.contains("Ready"));
    }

    #[tokio::test]
    async fn store_query_returns_stored_messages() {
        let store = Arc::new(SqliteStore::in_memory().await.unwrap());
        let mut m = WakuMessage::new("/app/1/x/proto", b"hello".to_vec());
        m.timestamp = Some(1000);
        store.put("/waku/2/rs/1/0", &m, 0).await.unwrap();
        let store: Arc<dyn MessageStore> = store;

        let app = router(test_state(Some(store)).await);
        let resp = app
            .oneshot(
                Request::get("/store/v3/messages?contentTopics=/app/1/x/proto")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let json: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
        let messages = json["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        // payload is base64("hello").
        assert_eq!(messages[0]["message"]["payload"], B64.encode(b"hello"));
        assert_eq!(messages[0]["message"]["contentTopic"], "/app/1/x/proto");
    }

    #[tokio::test]
    async fn relay_cache_polls_received_messages() {
        let state = test_state(None).await;
        let cache = state.cache();
        let app = router(state);

        // Simulate the node receiving a message on a subscribed content topic.
        cache.register("/app/1/x/proto".to_string());
        cache.record(WakuMessage::new("/app/1/x/proto", b"cached".to_vec()));

        let resp = app
            .oneshot(
                Request::get("/relay/v1/auto/messages/%2Fapp%2F1%2Fx%2Fproto")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
        let messages = json.as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["payload"], B64.encode(b"cached"));
    }

    #[tokio::test]
    async fn metrics_and_admin_peers() {
        let store = Arc::new(SqliteStore::in_memory().await.unwrap());
        store
            .put(
                "/waku/2/rs/1/0",
                &WakuMessage::new("/a/1/b/proto", b"x".to_vec()),
                0,
            )
            .await
            .unwrap();
        let store: Arc<dyn MessageStore> = store;
        let app = router(test_state(Some(store)).await);

        let resp = app
            .clone()
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp).await;
        assert!(body.contains("rs_waku_connected_peers"));
        assert!(body.contains("rs_waku_stored_messages 1"));
        assert!(body.contains("rs_waku_relay_messages_total"));
        assert!(body.contains("rs_waku_rate_limited_total"));

        let resp = app
            .oneshot(Request::get("/admin/v1/peers").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp).await, "[]"); // isolated node, no peers
    }

    #[tokio::test]
    async fn publish_with_no_peers_is_service_unavailable() {
        let app = router(test_state(None).await);
        let msg =
            serde_json::json!({ "payload": B64.encode(b"hi"), "contentTopic": "/app/1/x/proto" });
        let resp = app
            .oneshot(
                Request::post("/relay/v1/auto/messages/%2Fapp%2F1%2Fx%2Fproto")
                    .header("content-type", "application/json")
                    .body(Body::from(msg.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        // No mesh peers in an isolated node → graceful 503 (parsing/routing worked).
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
