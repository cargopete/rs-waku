//! 13/WAKU2-STORE v3 request/response protocol (`/vac/waku/store-query/3.0.0`).
//!
//! A libp2p request-response protocol carrying length-prefixed protobuf (the
//! same framing as 66/WAKU2-METADATA). [`serve`] answers a request from a
//! [`MessageStore`]; clients send a [`StoreQueryRequest`] and receive a
//! [`StoreQueryResponse`].
//!
//! ⚠ INTEROP CAVEAT: the protobuf field numbers follow nwaku's store v3
//! `store_query.proto` to the best of current knowledge; verify against nwaku
//! before relying on cross-implementation queries. Our own client↔server is
//! self-consistent regardless.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::StreamProtocol;
use prost::Message as _;
use waku_core::WakuMessage;

use crate::{MessageStore, StoreQuery};

pub use waku_core::preset::STORE_QUERY_PROTOCOL_ID as PROTOCOL_ID;

/// Store responses can be large (many messages); cap a single frame at 100 MiB.
const MAX_FRAME_LEN: usize = 100 * 1024 * 1024;

/// A 13/WAKU2-STORE v3 query request.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoreQueryRequest {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(bool, tag = "2")]
    pub include_data: bool,
    #[prost(string, optional, tag = "10")]
    pub pubsub_topic: Option<String>,
    #[prost(string, repeated, tag = "11")]
    pub content_topics: Vec<String>,
    #[prost(sint64, optional, tag = "12")]
    pub time_start: Option<i64>,
    #[prost(sint64, optional, tag = "13")]
    pub time_end: Option<i64>,
    #[prost(bytes = "vec", repeated, tag = "20")]
    pub message_hashes: Vec<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "51")]
    pub pagination_cursor: Option<Vec<u8>>,
    #[prost(bool, tag = "52")]
    pub pagination_forward: bool,
    #[prost(uint64, optional, tag = "53")]
    pub pagination_limit: Option<u64>,
}

/// A message hash + (optional) message + pubsub topic, as returned in responses.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WakuMessageKeyValue {
    #[prost(bytes = "vec", optional, tag = "1")]
    pub message_hash: Option<Vec<u8>>,
    #[prost(message, optional, tag = "2")]
    pub message: Option<WakuMessage>,
    #[prost(string, optional, tag = "3")]
    pub pubsub_topic: Option<String>,
}

/// A 13/WAKU2-STORE v3 query response.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoreQueryResponse {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(uint32, optional, tag = "10")]
    pub status_code: Option<u32>,
    #[prost(string, optional, tag = "11")]
    pub status_desc: Option<String>,
    #[prost(message, repeated, tag = "20")]
    pub messages: Vec<WakuMessageKeyValue>,
    #[prost(bytes = "vec", optional, tag = "51")]
    pub pagination_cursor: Option<Vec<u8>>,
}

/// The configured store-query request-response behaviour.
pub type Behaviour = request_response::Behaviour<StoreCodec>;
/// The behaviour's event type.
pub type Event = request_response::Event<StoreQueryRequest, StoreQueryResponse>;

/// Build the store-query request-response behaviour.
pub fn build() -> Behaviour {
    let protocols = std::iter::once((StreamProtocol::new(PROTOCOL_ID), ProtocolSupport::Full));
    request_response::Behaviour::with_codec(
        StoreCodec,
        protocols,
        request_response::Config::default(),
    )
}

/// Translate a wire request into an internal [`StoreQuery`].
pub fn request_to_query(req: &StoreQueryRequest) -> StoreQuery {
    StoreQuery {
        pubsub_topic: req.pubsub_topic.clone(),
        content_topics: req.content_topics.clone(),
        time_start_ns: req.time_start,
        time_end_ns: req.time_end,
        message_hashes: req
            .message_hashes
            .iter()
            .filter_map(|h| h.as_slice().try_into().ok())
            .collect(),
        include_data: req.include_data,
        page_size: req.pagination_limit.unwrap_or(20),
        forward: req.pagination_forward,
        cursor: req
            .pagination_cursor
            .as_ref()
            .and_then(|c| c.as_slice().try_into().ok()),
    }
}

/// Answer a store query from `store`.
pub async fn serve(store: &dyn MessageStore, req: &StoreQueryRequest) -> StoreQueryResponse {
    match store.query(&request_to_query(req)).await {
        Ok(result) => StoreQueryResponse {
            request_id: req.request_id.clone(),
            status_code: Some(200),
            status_desc: Some("OK".into()),
            messages: result
                .messages
                .into_iter()
                .map(|m| WakuMessageKeyValue {
                    message_hash: Some(m.hash.to_vec()),
                    message: m.message,
                    pubsub_topic: Some(m.pubsub_topic),
                })
                .collect(),
            pagination_cursor: result.next_cursor.map(|h| h.to_vec()),
        },
        Err(e) => StoreQueryResponse {
            request_id: req.request_id.clone(),
            status_code: Some(500),
            status_desc: Some(e.to_string()),
            messages: Vec::new(),
            pagination_cursor: None,
        },
    }
}

/// Length-prefixed protobuf codec for the store-query protocol.
#[derive(Clone, Default)]
pub struct StoreCodec;

#[async_trait]
impl request_response::Codec for StoreCodec {
    type Protocol = StreamProtocol;
    type Request = StoreQueryRequest;
    type Response = StoreQueryResponse;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_lp(io).await?;
        StoreQueryRequest::decode(bytes.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_lp(io).await?;
        StoreQueryResponse::decode(bytes.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_lp(io, &req.encode_to_vec()).await
    }

    async fn write_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_lp(io, &res.encode_to_vec()).await
    }
}

async fn read_lp<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let len = read_uvarint(io).await?;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "store frame exceeds maximum length",
        ));
    }
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_lp<T>(io: &mut T, data: &[u8]) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    let mut prefix = [0u8; 10];
    let n = write_uvarint(&mut prefix, data.len() as u64);
    io.write_all(&prefix[..n]).await?;
    io.write_all(data).await?;
    Ok(())
}

async fn read_uvarint<T>(io: &mut T) -> io::Result<usize>
where
    T: AsyncRead + Unpin + Send,
{
    let mut value: u64 = 0;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        io.read_exact(&mut byte).await?;
        value |= ((byte[0] & 0x7f) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
    Ok(value as usize)
}

fn write_uvarint(buf: &mut [u8; 10], mut v: u64) -> usize {
    let mut i = 0;
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf[i] = byte;
        i += 1;
        if v == 0 {
            break;
        }
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteStore;
    use futures::io::Cursor;

    #[test]
    fn request_lp_roundtrip() {
        let req = StoreQueryRequest {
            request_id: "abc".into(),
            include_data: true,
            content_topics: vec!["/app/1/x/proto".into()],
            pagination_limit: Some(10),
            pagination_forward: true,
            ..Default::default()
        };
        let mut sink = Vec::new();
        futures::executor::block_on(write_lp(&mut sink, &req.encode_to_vec())).unwrap();
        let bytes = futures::executor::block_on(read_lp(&mut Cursor::new(&sink))).unwrap();
        assert_eq!(StoreQueryRequest::decode(bytes.as_slice()).unwrap(), req);
    }

    #[tokio::test]
    async fn serve_answers_from_the_store() {
        let store = SqliteStore::in_memory().await.unwrap();
        let topic = "/waku/2/rs/1/0";
        for i in 0..3i64 {
            let mut m = WakuMessage::new("/app/1/chat/proto", vec![i as u8]);
            m.timestamp = Some(100 + i);
            store.put(topic, &m, 0).await.unwrap();
        }

        let req = StoreQueryRequest {
            request_id: "q1".into(),
            include_data: true,
            content_topics: vec!["/app/1/chat/proto".into()],
            pagination_forward: true,
            pagination_limit: Some(2),
            ..Default::default()
        };
        let resp = serve(&store, &req).await;

        assert_eq!(resp.request_id, "q1");
        assert_eq!(resp.status_code, Some(200));
        assert_eq!(resp.messages.len(), 2);
        assert_eq!(resp.messages[0].message.as_ref().unwrap().payload, vec![0]);
        assert!(resp.pagination_cursor.is_some(), "a third message remains");
    }
}
