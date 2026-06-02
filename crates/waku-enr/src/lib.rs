//! # waku-enr — 31/WAKU2-ENR relay-shard fields
//!
//! Encodes/decodes the Waku relay-shards carried in an ENR (EIP-778), per
//! 51/WAKU2-RELAY-SHARDING. Two on-the-wire forms share the [`RelayShards`] type:
//!
//! - **`rs`** — *indices list* (compact; for ≤ 64 shards):
//!   `cluster_id(2, BE) ‖ count(1) ‖ shard_index(2, BE) × count`
//! - **`rsv`** — *bit vector* (for > 64 shards):
//!   `cluster_id(2, BE) ‖ 128-byte bitvector` (1024 bits, bit `i` ⇒ shard `i`)
//!
//! These are pure byte-codecs (no `enr`-crate dependency); `waku-discv5` plugs
//! the bytes into the actual ENR key/values.
//!
//! ⚠ INTEROP CAVEAT: the bit-vector bit ordering (MSB-first within each byte)
//! must be verified against nwaku `waku/waku_enr/sharding.nim` before relying on
//! cross-impl `rsv` parsing. TWN uses the `rs` indices list (≤ 8 shards), which
//! is the well-exercised path.

use thiserror::Error;
use waku_core::ShardId;

/// ENR key for the relay-shards *indices list*.
pub const ENR_KEY_RS: &str = "rs";
/// ENR key for the relay-shards *bit vector*.
pub const ENR_KEY_RSV: &str = "rsv";
/// ENR key for the Waku capability bitfield (relay/store/filter/lightpush).
pub const ENR_KEY_WAKU2: &str = "waku2";

/// Total shards addressable by the bit-vector form.
const BIT_VECTOR_SHARDS: usize = 1024;
const BIT_VECTOR_BYTES: usize = BIT_VECTOR_SHARDS / 8; // 128

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnrError {
    #[error("relay-shards field too short: {0} bytes")]
    TooShort(usize),
    #[error(
        "relay-shards indices-list length mismatch: header says {expected}, got {actual} bytes"
    )]
    IndicesLengthMismatch { expected: usize, actual: usize },
    #[error("relay-shards bit-vector must be {expected} bytes, got {actual}")]
    BitVectorLength { expected: usize, actual: usize },
    #[error("shard index {0} out of range for the bit-vector form (max {max})", max = BIT_VECTOR_SHARDS - 1)]
    ShardOutOfRange(u16),
    #[error("too many shards for the indices-list form: {0} > 255")]
    TooManyShards(usize),
}

/// A cluster id plus its set of shard indices.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayShards {
    pub cluster_id: u16,
    pub shards: Vec<u16>,
}

impl RelayShards {
    pub fn new(cluster_id: u16, shards: impl IntoIterator<Item = u16>) -> Self {
        let mut shards: Vec<u16> = shards.into_iter().collect();
        shards.sort_unstable();
        shards.dedup();
        Self { cluster_id, shards }
    }

    /// As [`ShardId`]s for use with the rest of the stack.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shards
            .iter()
            .map(|s| ShardId::new(self.cluster_id, *s))
            .collect()
    }

    /// Whether this descriptor contains `shard` in `cluster`.
    pub fn contains(&self, cluster_id: u16, shard: u16) -> bool {
        self.cluster_id == cluster_id && self.shards.contains(&shard)
    }

    // --- `rs` indices-list form ---

    pub fn to_indices_list(&self) -> Result<Vec<u8>, EnrError> {
        if self.shards.len() > u8::MAX as usize {
            return Err(EnrError::TooManyShards(self.shards.len()));
        }
        let mut out = Vec::with_capacity(3 + self.shards.len() * 2);
        out.extend_from_slice(&self.cluster_id.to_be_bytes());
        out.push(self.shards.len() as u8);
        for s in &self.shards {
            out.extend_from_slice(&s.to_be_bytes());
        }
        Ok(out)
    }

    pub fn from_indices_list(bytes: &[u8]) -> Result<Self, EnrError> {
        if bytes.len() < 3 {
            return Err(EnrError::TooShort(bytes.len()));
        }
        let cluster_id = u16::from_be_bytes([bytes[0], bytes[1]]);
        let count = bytes[2] as usize;
        let expected = 3 + count * 2;
        if bytes.len() != expected {
            return Err(EnrError::IndicesLengthMismatch {
                expected,
                actual: bytes.len(),
            });
        }
        let shards = bytes[3..]
            .chunks_exact(2)
            .map(|c| u16::from_be_bytes([c[0], c[1]]))
            .collect();
        Ok(Self { cluster_id, shards })
    }

    // --- `rsv` bit-vector form ---

    pub fn to_bit_vector(&self) -> Result<Vec<u8>, EnrError> {
        let mut out = vec![0u8; 2 + BIT_VECTOR_BYTES];
        out[0..2].copy_from_slice(&self.cluster_id.to_be_bytes());
        for &s in &self.shards {
            let idx = s as usize;
            if idx >= BIT_VECTOR_SHARDS {
                return Err(EnrError::ShardOutOfRange(s));
            }
            // MSB-first within each byte.
            out[2 + idx / 8] |= 1 << (7 - (idx % 8));
        }
        Ok(out)
    }

    pub fn from_bit_vector(bytes: &[u8]) -> Result<Self, EnrError> {
        if bytes.len() != 2 + BIT_VECTOR_BYTES {
            return Err(EnrError::BitVectorLength {
                expected: 2 + BIT_VECTOR_BYTES,
                actual: bytes.len(),
            });
        }
        let cluster_id = u16::from_be_bytes([bytes[0], bytes[1]]);
        let mut shards = Vec::new();
        for idx in 0..BIT_VECTOR_SHARDS {
            if bytes[2 + idx / 8] & (1 << (7 - (idx % 8))) != 0 {
                shards.push(idx as u16);
            }
        }
        Ok(Self { cluster_id, shards })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indices_list_roundtrip() {
        let rs = RelayShards::new(1, [0, 1, 2, 7]);
        let bytes = rs.to_indices_list().unwrap();
        // cluster(2) + count(1) + 4*2
        assert_eq!(bytes.len(), 3 + 8);
        assert_eq!(bytes[0..2], 1u16.to_be_bytes());
        assert_eq!(bytes[2], 4);
        assert_eq!(RelayShards::from_indices_list(&bytes).unwrap(), rs);
    }

    #[test]
    fn bit_vector_roundtrip() {
        let rs = RelayShards::new(1, [0, 7, 8, 1023]);
        let bytes = rs.to_bit_vector().unwrap();
        assert_eq!(bytes.len(), 130);
        assert_eq!(RelayShards::from_bit_vector(&bytes).unwrap(), rs);
    }

    #[test]
    fn both_forms_describe_the_same_set() {
        let rs = RelayShards::new(1, [0, 1, 2, 3, 4, 5, 6, 7]);
        let a = RelayShards::from_indices_list(&rs.to_indices_list().unwrap()).unwrap();
        let b = RelayShards::from_bit_vector(&rs.to_bit_vector().unwrap()).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn new_sorts_and_dedups() {
        let rs = RelayShards::new(1, [7, 1, 7, 0, 1]);
        assert_eq!(rs.shards, vec![0, 1, 7]);
    }

    #[test]
    fn shard_ids_carry_cluster() {
        let rs = RelayShards::new(1, [0, 3]);
        let ids = rs.shard_ids();
        assert_eq!(ids[0], ShardId::new(1, 0));
        assert_eq!(ids[1], ShardId::new(1, 3));
        assert!(rs.contains(1, 3));
        assert!(!rs.contains(2, 3));
    }

    #[test]
    fn rejects_malformed() {
        assert_eq!(
            RelayShards::from_indices_list(&[0, 1]),
            Err(EnrError::TooShort(2))
        );
        // header claims 4 shards but bytes only hold 1
        assert!(matches!(
            RelayShards::from_indices_list(&[0, 1, 4, 0, 0]),
            Err(EnrError::IndicesLengthMismatch { .. })
        ));
        assert!(matches!(
            RelayShards::from_bit_vector(&[0, 1, 2, 3]),
            Err(EnrError::BitVectorLength { .. })
        ));
    }
}
