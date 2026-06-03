//! # waku-peer-exchange — 34/WAKU2-PEER-EXCHANGE (`/vac/waku/peer-exchange/2.0.0-alpha1`)
//!
//! Request/response peer discovery for resource-restricted nodes that can't run
//! discv5: a node asks a peer for a batch of ENRs. A libp2p request-response
//! protocol carrying length-prefixed protobuf (same framing as metadata/store).
//! Both directions use [`PeerExchangeRpc`] (query populated on the request,
//! response populated on the reply), matching nwaku's wire shape. The ENR bytes
//! are opaque here; `waku-discv5` converts them to/from [`enr`] records.
//!
//! ⚠ INTEROP CAVEAT: protobuf field numbers follow nwaku's `peer_exchange.proto`
//! to the best of current knowledge; verify before cross-impl use.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::StreamProtocol;
use prost::Message as _;

pub use waku_core::preset::PEER_EXCHANGE_PROTOCOL_ID as PROTOCOL_ID;

const MAX_FRAME_LEN: usize = 1024 * 1024;

/// A single peer's record (raw RLP-encoded ENR bytes).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerInfo {
    #[prost(bytes = "vec", tag = "1")]
    pub enr: Vec<u8>,
}

/// How many peers the requester wants.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerExchangeQuery {
    #[prost(uint64, tag = "1")]
    pub num_peers: u64,
}

/// The peers returned in a response.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerExchangeResponse {
    #[prost(message, repeated, tag = "1")]
    pub peer_infos: Vec<PeerInfo>,
}

/// The on-the-wire envelope: a request sets `query`, a response sets `response`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerExchangeRpc {
    #[prost(message, optional, tag = "1")]
    pub query: Option<PeerExchangeQuery>,
    #[prost(message, optional, tag = "2")]
    pub response: Option<PeerExchangeResponse>,
}

impl PeerExchangeRpc {
    /// A request for `num_peers` peers.
    pub fn query(num_peers: u64) -> Self {
        Self {
            query: Some(PeerExchangeQuery { num_peers }),
            response: None,
        }
    }

    /// A response carrying the given raw ENR byte blobs.
    pub fn response(enrs: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            query: None,
            response: Some(PeerExchangeResponse {
                peer_infos: enrs.into_iter().map(|enr| PeerInfo { enr }).collect(),
            }),
        }
    }

    /// The number of peers requested, if this is a query.
    pub fn requested(&self) -> Option<u64> {
        self.query.as_ref().map(|q| q.num_peers)
    }

    /// The returned ENR byte blobs, if this is a response.
    pub fn enrs(&self) -> Vec<Vec<u8>> {
        self.response
            .as_ref()
            .map(|r| r.peer_infos.iter().map(|p| p.enr.clone()).collect())
            .unwrap_or_default()
    }
}

/// The configured peer-exchange request-response behaviour.
pub type Behaviour = request_response::Behaviour<PeerExchangeCodec>;
/// The behaviour's event type.
pub type Event = request_response::Event<PeerExchangeRpc, PeerExchangeRpc>;

/// Build the peer-exchange request-response behaviour.
pub fn build() -> Behaviour {
    let protocols = std::iter::once((StreamProtocol::new(PROTOCOL_ID), ProtocolSupport::Full));
    request_response::Behaviour::with_codec(
        PeerExchangeCodec,
        protocols,
        request_response::Config::default(),
    )
}

/// Length-prefixed protobuf codec for the peer-exchange protocol.
#[derive(Clone, Default)]
pub struct PeerExchangeCodec;

#[async_trait]
impl request_response::Codec for PeerExchangeCodec {
    type Protocol = StreamProtocol;
    type Request = PeerExchangeRpc;
    type Response = PeerExchangeRpc;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        decode(&read_lp(io).await?)
    }

    async fn read_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        decode(&read_lp(io).await?)
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

fn decode(bytes: &[u8]) -> io::Result<PeerExchangeRpc> {
    PeerExchangeRpc::decode(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

async fn read_lp<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let len = read_uvarint(io).await?;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer-exchange frame exceeds maximum length",
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
    fn query_and_response_helpers() {
        let q = PeerExchangeRpc::query(5);
        assert_eq!(q.requested(), Some(5));
        assert!(q.enrs().is_empty());

        let r = PeerExchangeRpc::response(vec![vec![1, 2, 3], vec![4, 5]]);
        assert_eq!(r.requested(), None);
        assert_eq!(r.enrs(), vec![vec![1, 2, 3], vec![4, 5]]);
    }

    #[test]
    fn rpc_lp_roundtrip() {
        let rpc = PeerExchangeRpc::response(vec![vec![9; 32]]);
        let mut sink = Vec::new();
        futures::executor::block_on(write_lp(&mut sink, &rpc.encode_to_vec())).unwrap();
        let bytes = futures::executor::block_on(read_lp(&mut Cursor::new(&sink))).unwrap();
        assert_eq!(PeerExchangeRpc::decode(bytes.as_slice()).unwrap(), rpc);
    }
}
