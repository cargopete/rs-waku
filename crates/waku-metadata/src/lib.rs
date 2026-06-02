//! # waku-metadata — 66/WAKU2-METADATA (`/vac/waku/metadata/1.0.0`)
//!
//! A libp2p request/response handshake exchanging `cluster_id` + supported
//! `shards`. Both request and response carry the same payload. A node MUST
//! disconnect peers whose `cluster_id` mismatches — that enforcement lives in
//! `waku-node`; this crate provides the wire protocol.
//!
//! Framing: length-prefixed protobuf (unsigned-varint length + bytes), matching
//! nwaku's `writeLp`/`readLp`.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::StreamProtocol;
use prost::Message as _;

pub use waku_core::preset::METADATA_PROTOCOL_ID as PROTOCOL_ID;

/// Cap on a metadata frame; defends the varint length against abuse.
const MAX_FRAME_LEN: usize = 64 * 1024;

/// The 66/WAKU2-METADATA payload (identical wire shape for request & response).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WakuMetadata {
    #[prost(uint32, optional, tag = "1")]
    pub cluster_id: Option<u32>,
    #[prost(uint32, repeated, tag = "2")]
    pub shards: Vec<u32>,
}

/// The configured request-response behaviour.
pub type Behaviour = request_response::Behaviour<MetadataCodec>;
/// The behaviour's event type.
pub type Event = request_response::Event<WakuMetadata, WakuMetadata>;

/// Build the metadata request-response behaviour.
pub fn build() -> Behaviour {
    let protocols = std::iter::once((StreamProtocol::new(PROTOCOL_ID), ProtocolSupport::Full));
    request_response::Behaviour::with_codec(
        MetadataCodec,
        protocols,
        request_response::Config::default(),
    )
}

/// Length-prefixed protobuf codec for the metadata protocol.
#[derive(Clone, Default)]
pub struct MetadataCodec;

#[async_trait]
impl request_response::Codec for MetadataCodec {
    type Protocol = StreamProtocol;
    type Request = WakuMetadata;
    type Response = WakuMetadata;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<WakuMetadata>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_lp(io).await
    }

    async fn read_response<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<WakuMetadata>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_lp(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        req: WakuMetadata,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_lp(io, &req).await
    }

    async fn write_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        res: WakuMetadata,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_lp(io, &res).await
    }
}

async fn read_lp<T>(io: &mut T) -> io::Result<WakuMetadata>
where
    T: AsyncRead + Unpin + Send,
{
    let len = read_uvarint(io).await?;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata frame exceeds maximum length",
        ));
    }
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    WakuMetadata::decode(buf.as_slice()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

async fn write_lp<T>(io: &mut T, msg: &WakuMetadata) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    let data = msg.encode_to_vec();
    let mut prefix = [0u8; 10];
    let n = write_uvarint(&mut prefix, data.len() as u64);
    io.write_all(&prefix[..n]).await?;
    io.write_all(&data).await?;
    Ok(())
}

/// Read an LEB128 unsigned varint from an async stream.
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

/// Encode `v` as an LEB128 unsigned varint into `buf`, returning the byte count.
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
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 16384, u32::MAX as u64] {
            let mut buf = [0u8; 10];
            let n = write_uvarint(&mut buf, v);
            let got =
                futures::executor::block_on(read_uvarint(&mut Cursor::new(&buf[..n]))).unwrap();
            assert_eq!(got as u64, v);
        }
    }

    #[test]
    fn metadata_lp_roundtrip() {
        let msg = WakuMetadata {
            cluster_id: Some(1),
            shards: vec![0, 1, 2, 7],
        };
        let mut sink = Vec::new();
        futures::executor::block_on(write_lp(&mut sink, &msg)).unwrap();
        let got = futures::executor::block_on(read_lp(&mut Cursor::new(&sink))).unwrap();
        assert_eq!(got, msg);
    }
}
