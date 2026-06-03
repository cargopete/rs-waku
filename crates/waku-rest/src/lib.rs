//! # waku-rest — nwaku-compatible REST API
//!
//! An `axum` server matching nwaku's OpenAPI (port 8645) — the surface the Waku
//! Python interop suite and operators drive. This first slice covers
//! `debug`/`health`, relay publish (autosharded), and 13/WAKU2-STORE v3 query.
//!
//! ⚠ JSON field shapes follow nwaku's REST spec (camelCase, base64 payloads);
//! verify against `waku-org/waku-rest-api` before relying on interop.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use waku_core::{autoshard, ContentTopic, MessageHash, NetworkPreset, WakuMessage, TWN};
use waku_node::NodeHandle;
use waku_store::{MessageStore, StoreQuery};

/// Default nwaku REST port.
pub const DEFAULT_REST_PORT: u16 = 8645;

/// Shared state for the REST handlers.
#[derive(Clone)]
pub struct AppState {
    pub node: NodeHandle,
    pub store: Option<Arc<dyn MessageStore>>,
    pub preset: NetworkPreset,
    pub version: String,
}

impl AppState {
    pub fn new(node: NodeHandle, store: Option<Arc<dyn MessageStore>>) -> Self {
        Self {
            node,
            store,
            preset: TWN,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Build the REST router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/debug/v1/version", get(version))
        .route("/debug/v1/info", get(info))
        .route("/health", get(health))
        .route(
            "/relay/v1/auto/messages/{content_topic}",
            post(relay_publish),
        )
        .route("/store/v3/messages", get(store_query))
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

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({ "nodeHealth": "Ready" })),
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
