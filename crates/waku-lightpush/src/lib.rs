//! # waku-lightpush — 19/WAKU2-LIGHTPUSH v3 (`/vac/waku/lightpush/3.0.0`)
//!
//! Lets resource-restricted nodes publish a `WakuMessage` by handing it to a
//! full relay node, which injects it into gossipsub. A libp2p request-response
//! protocol carrying length-prefixed protobuf (same framing as metadata/store).
//! The full-node side (publish + count relay peers) lives in `waku-node`; this
//! crate is the wire protocol.
//!
//! ⚠ INTEROP CAVEAT: the protobuf field numbers follow nwaku's lightpush v3 to
//! the best of current knowledge; verify against nwaku before relying on
//! cross-implementation lightpush. Our own client↔server is self-consistent.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::StreamProtocol;
use prost::Message as _;
use waku_core::WakuMessage;

pub use waku_core::preset::LIGHTPUSH_PROTOCOL_ID as PROTOCOL_ID;

const MAX_FRAME_LEN: usize = 1024 * 1024;

/// A 19/WAKU2-LIGHTPUSH v3 publish request.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LightpushRequest {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(string, tag = "20")]
    pub pubsub_topic: String,
    #[prost(message, optional, tag = "21")]
    pub message: Option<WakuMessage>,
}

/// A 19/WAKU2-LIGHTPUSH v3 response.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LightpushResponse {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(uint32, tag = "10")]
    pub status_code: u32,
    #[prost(string, optional, tag = "11")]
    pub status_desc: Option<String>,
    /// Number of relay peers the message was forwarded to.
    #[prost(uint32, tag = "12")]
    pub relay_peer_count: u32,
}

impl LightpushResponse {
    pub fn ok(request_id: String, relay_peer_count: u32) -> Self {
        Self {
            request_id,
            status_code: 200,
            status_desc: Some("OK".into()),
            relay_peer_count,
        }
    }

    pub fn error(request_id: String, status_code: u32, desc: impl Into<String>) -> Self {
        Self {
            request_id,
            status_code,
            status_desc: Some(desc.into()),
            relay_peer_count: 0,
        }
    }
}

/// The configured lightpush request-response behaviour.
pub type Behaviour = request_response::Behaviour<LightpushCodec>;
/// The behaviour's event type.
pub type Event = request_response::Event<LightpushRequest, LightpushResponse>;

/// Build the lightpush request-response behaviour.
pub fn build() -> Behaviour {
    let protocols = std::iter::once((StreamProtocol::new(PROTOCOL_ID), ProtocolSupport::Full));
    request_response::Behaviour::with_codec(
        LightpushCodec,
        protocols,
        request_response::Config::default(),
    )
}

/// Length-prefixed protobuf codec for the lightpush protocol.
#[derive(Clone, Default)]
pub struct LightpushCodec;

#[async_trait]
impl request_response::Codec for LightpushCodec {
    type Protocol = StreamProtocol;
    type Request = LightpushRequest;
    type Response = LightpushResponse;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_lp(io).await?;
        LightpushRequest::decode(bytes.as_slice())
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
        LightpushResponse::decode(bytes.as_slice())
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
            "lightpush frame exceeds maximum length",
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
    fn request_lp_roundtrip() {
        let req = LightpushRequest {
            request_id: "lp1".into(),
            pubsub_topic: "/waku/2/rs/1/0".into(),
            message: Some(WakuMessage::new("/app/1/x/proto", b"hi".to_vec())),
        };
        let mut sink = Vec::new();
        futures::executor::block_on(write_lp(&mut sink, &req.encode_to_vec())).unwrap();
        let bytes = futures::executor::block_on(read_lp(&mut Cursor::new(&sink))).unwrap();
        assert_eq!(LightpushRequest::decode(bytes.as_slice()).unwrap(), req);
    }

    #[test]
    fn response_helpers() {
        assert_eq!(LightpushResponse::ok("a".into(), 3).status_code, 200);
        assert_eq!(LightpushResponse::ok("a".into(), 3).relay_peer_count, 3);
        assert_eq!(
            LightpushResponse::error("a".into(), 503, "no peers").status_code,
            503
        );
    }
}
