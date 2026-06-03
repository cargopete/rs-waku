//! # waku-filter — 12/WAKU2-FILTER v2
//!
//! Light-client content filtering via two libp2p request-response protocols:
//!
//! - **filter-subscribe** (`/vac/waku/filter-subscribe/2.0.0-beta1`): client →
//!   full node. Subscribe / unsubscribe / unsubscribe-all / ping.
//! - **filter-push** (`/vac/waku/filter-push/2.0.0-beta1`): full node → client.
//!   Pushes messages matching an active subscription.
//!
//! Both carry length-prefixed protobuf (same framing as metadata/store). The
//! subscription registry + push-on-relay logic live in `waku-node`.
//!
//! ⚠ INTEROP CAVEATS: protobuf field numbers follow nwaku's filter v2 to the
//! best of current knowledge; nwaku's filter-push may be a one-way libp2p stream
//! rather than request/response (we use a small ack). Verify before cross-impl
//! use; our own client↔server is self-consistent.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::StreamProtocol;
use prost::Message as _;
use waku_core::WakuMessage;

pub use waku_core::preset::{
    FILTER_PUSH_PROTOCOL_ID as PUSH_PROTOCOL_ID,
    FILTER_SUBSCRIBE_PROTOCOL_ID as SUBSCRIBE_PROTOCOL_ID,
};

const MAX_FRAME_LEN: usize = 1024 * 1024;

/// `filter_subscribe_type` values (12/WAKU2-FILTER v2).
pub mod subscribe_type {
    pub const PING: i32 = 0;
    pub const SUBSCRIBE: i32 = 1;
    pub const UNSUBSCRIBE: i32 = 2;
    pub const UNSUBSCRIBE_ALL: i32 = 3;
}

/// A filter-subscribe request (subscribe/unsubscribe/ping).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterSubscribeRequest {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(int32, tag = "2")]
    pub filter_subscribe_type: i32,
    #[prost(string, optional, tag = "10")]
    pub pubsub_topic: Option<String>,
    #[prost(string, repeated, tag = "11")]
    pub content_topics: Vec<String>,
}

/// A filter-subscribe response (ack).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterSubscribeResponse {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(uint32, tag = "10")]
    pub status_code: u32,
    #[prost(string, optional, tag = "11")]
    pub status_desc: Option<String>,
}

impl FilterSubscribeResponse {
    pub fn ok(request_id: String) -> Self {
        Self {
            request_id,
            status_code: 200,
            status_desc: Some("OK".into()),
        }
    }

    pub fn error(request_id: String, code: u32, desc: impl Into<String>) -> Self {
        Self {
            request_id,
            status_code: code,
            status_desc: Some(desc.into()),
        }
    }
}

/// A message pushed to a subscriber (filter-push).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessagePush {
    #[prost(message, optional, tag = "1")]
    pub waku_message: Option<WakuMessage>,
    #[prost(string, optional, tag = "2")]
    pub pubsub_topic: Option<String>,
}

/// Ack for a filter-push (our small, self-consistent acknowledgement).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterPushResponse {
    #[prost(uint32, tag = "10")]
    pub status_code: u32,
}

/// filter-subscribe behaviour (client → full node).
pub type SubscribeBehaviour = request_response::Behaviour<SubscribeCodec>;
pub type SubscribeEvent = request_response::Event<FilterSubscribeRequest, FilterSubscribeResponse>;
/// filter-push behaviour (full node → client).
pub type PushBehaviour = request_response::Behaviour<PushCodec>;
pub type PushEvent = request_response::Event<MessagePush, FilterPushResponse>;

pub fn build_subscribe() -> SubscribeBehaviour {
    let protocols = std::iter::once((
        StreamProtocol::new(SUBSCRIBE_PROTOCOL_ID),
        ProtocolSupport::Full,
    ));
    request_response::Behaviour::with_codec(
        SubscribeCodec,
        protocols,
        request_response::Config::default(),
    )
}

pub fn build_push() -> PushBehaviour {
    let protocols = std::iter::once((StreamProtocol::new(PUSH_PROTOCOL_ID), ProtocolSupport::Full));
    request_response::Behaviour::with_codec(
        PushCodec,
        protocols,
        request_response::Config::default(),
    )
}

#[derive(Clone, Default)]
pub struct SubscribeCodec;

#[async_trait]
impl request_response::Codec for SubscribeCodec {
    type Protocol = StreamProtocol;
    type Request = FilterSubscribeRequest;
    type Response = FilterSubscribeResponse;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let b = read_lp(io).await?;
        FilterSubscribeRequest::decode(b.as_slice()).map_err(invalid)
    }
    async fn read_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let b = read_lp(io).await?;
        FilterSubscribeResponse::decode(b.as_slice()).map_err(invalid)
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

#[derive(Clone, Default)]
pub struct PushCodec;

#[async_trait]
impl request_response::Codec for PushCodec {
    type Protocol = StreamProtocol;
    type Request = MessagePush;
    type Response = FilterPushResponse;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let b = read_lp(io).await?;
        MessagePush::decode(b.as_slice()).map_err(invalid)
    }
    async fn read_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let b = read_lp(io).await?;
        FilterPushResponse::decode(b.as_slice()).map_err(invalid)
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

fn invalid<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

async fn read_lp<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let len = read_uvarint(io).await?;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "filter frame exceeds maximum length",
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
    use futures::io::Cursor;

    #[test]
    fn subscribe_request_roundtrip() {
        let req = FilterSubscribeRequest {
            request_id: "f1".into(),
            filter_subscribe_type: subscribe_type::SUBSCRIBE,
            pubsub_topic: Some("/waku/2/rs/1/0".into()),
            content_topics: vec!["/app/1/x/proto".into()],
        };
        let mut sink = Vec::new();
        futures::executor::block_on(write_lp(&mut sink, &req.encode_to_vec())).unwrap();
        let b = futures::executor::block_on(read_lp(&mut Cursor::new(&sink))).unwrap();
        assert_eq!(FilterSubscribeRequest::decode(b.as_slice()).unwrap(), req);
    }

    #[test]
    fn message_push_roundtrip() {
        let push = MessagePush {
            waku_message: Some(WakuMessage::new("/app/1/x/proto", b"hi".to_vec())),
            pubsub_topic: Some("/waku/2/rs/1/0".into()),
        };
        let mut sink = Vec::new();
        futures::executor::block_on(write_lp(&mut sink, &push.encode_to_vec())).unwrap();
        let b = futures::executor::block_on(read_lp(&mut Cursor::new(&sink))).unwrap();
        assert_eq!(MessagePush::decode(b.as_slice()).unwrap(), push);
    }
}
