//! EIP-1459 DNS discovery — the `enrtree` resolver.
//!
//! A node list is published as a Merkle tree of DNS TXT records rooted at a
//! domain and signed by a secp256k1 key embedded in the `enrtree://` URL:
//!
//! - root: `enrtree-root:v1 e=<hash> l=<hash> seq=<n> sig=<b64url(sig65)>`
//!   (signed content is everything before ` sig=`);
//! - branch: `enrtree-branch:<hash>,<hash>,…`;
//! - leaf: `enr:<base64url>` or a link `enrtree://<b32pubkey>@<domain>`.
//!
//! Each non-root record lives at `<subdomain>.<domain>`, where `subdomain` is
//! `base32(keccak256(content)[..16])` — so the tree is self-verifying.
//!
//! The tree walk is generic over a [`TxtResolver`] so it is fully testable
//! offline; [`HickoryResolver`] is the live DNS implementation (feature `dns`).

use std::collections::HashSet;
use std::str::FromStr;

use async_trait::async_trait;
use data_encoding::{BASE32_NOPAD, BASE64URL_NOPAD};
use sha3::{Digest, Keccak256};
use thiserror::Error;

use crate::WakuEnr;

/// Safety cap on the number of records fetched while walking a tree.
const MAX_RECORDS: usize = 5000;

#[derive(Debug, Error)]
pub enum DnsError {
    #[error("invalid enrtree url: {0}")]
    Url(String),
    #[error("invalid root record: {0}")]
    Root(String),
    #[error("root signature verification failed")]
    Signature,
    #[error("dns resolution failed: {0}")]
    Resolve(String),
}

/// Resolves TXT records for a name. Returns one entry per TXT RR, with each
/// record's character-strings concatenated (no separator).
#[async_trait]
pub trait TxtResolver: Send + Sync {
    async fn txt(&self, name: &str) -> Result<Vec<String>, DnsError>;
}

/// A parsed `enrtree://<base32-pubkey>@<domain>` link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrTreeLink {
    /// Compressed secp256k1 public key (33 bytes) that signs the tree root.
    pub pubkey: Vec<u8>,
    pub domain: String,
}

/// Parse an `enrtree://` URL.
pub fn parse_enrtree(url: &str) -> Result<EnrTreeLink, DnsError> {
    let rest = url
        .strip_prefix("enrtree://")
        .ok_or_else(|| DnsError::Url(url.to_string()))?;
    let (b32key, domain) = rest
        .split_once('@')
        .ok_or_else(|| DnsError::Url(url.to_string()))?;
    let pubkey = BASE32_NOPAD
        .decode(b32key.to_uppercase().as_bytes())
        .map_err(|e| DnsError::Url(format!("bad base32 pubkey: {e}")))?;
    Ok(EnrTreeLink {
        pubkey,
        domain: domain.to_string(),
    })
}

/// The EIP-1459 subdomain for a record: `base32(keccak256(content)[..16])`.
pub fn subdomain_hash(content: &str) -> String {
    let digest = Keccak256::digest(content.as_bytes());
    BASE32_NOPAD.encode(&digest[..16])
}

struct RootRecord {
    e_root: String,
}

/// Verify the root record's signature against `pubkey` and extract the `e` hash.
fn verify_root(record: &str, pubkey: &[u8]) -> Result<RootRecord, DnsError> {
    let sig_idx = record
        .find(" sig=")
        .ok_or_else(|| DnsError::Root("missing sig".into()))?;
    let signed = &record[..sig_idx];
    let sig_b64 = record[sig_idx + 5..].trim();

    let sig = BASE64URL_NOPAD
        .decode(sig_b64.as_bytes())
        .map_err(|e| DnsError::Root(format!("bad sig base64: {e}")))?;
    if sig.len() < 64 {
        return Err(DnsError::Root("sig too short".into()));
    }

    verify_secp256k1(signed.as_bytes(), &sig[..64], pubkey)?;

    // Extract the `e=` (ENR subtree) hash from the signed content.
    let e_root = signed
        .split_whitespace()
        .find_map(|t| t.strip_prefix("e="))
        .ok_or_else(|| DnsError::Root("missing e=".into()))?
        .to_string();
    Ok(RootRecord { e_root })
}

/// Verify a secp256k1 signature `(r‖s)` over `keccak256(msg)` against a
/// compressed public key.
fn verify_secp256k1(msg: &[u8], sig_rs: &[u8], pubkey: &[u8]) -> Result<(), DnsError> {
    use k256::ecdsa::signature::hazmat::PrehashVerifier;
    use k256::ecdsa::{Signature, VerifyingKey};

    let prehash = Keccak256::digest(msg);
    let vk = VerifyingKey::from_sec1_bytes(pubkey).map_err(|_| DnsError::Signature)?;
    let sig = Signature::from_slice(sig_rs).map_err(|_| DnsError::Signature)?;
    vk.verify_prehash(&prehash, &sig)
        .map_err(|_| DnsError::Signature)
}

/// Resolve all ENRs published under an `enrtree://` URL.
///
/// Walks the `e` (ENR) subtree; `enrtree://` links to other trees are not
/// followed. Records whose content does not hash to their subdomain are skipped.
pub async fn resolve<R: TxtResolver + ?Sized>(
    url: &str,
    resolver: &R,
) -> Result<Vec<WakuEnr>, DnsError> {
    let link = parse_enrtree(url)?;

    let root_records = resolver.txt(&link.domain).await?;
    let root_txt = root_records
        .iter()
        .find(|r| r.starts_with("enrtree-root:v1"))
        .ok_or_else(|| DnsError::Root("no enrtree-root record".into()))?;
    let root = verify_root(root_txt, &link.pubkey)?;

    let mut enrs = Vec::new();
    let mut visited = HashSet::new();
    let mut stack = vec![root.e_root];
    let mut budget = MAX_RECORDS;

    while let Some(hash) = stack.pop() {
        if hash.is_empty() || !visited.insert(hash.clone()) {
            continue;
        }
        if budget == 0 {
            tracing::warn!("enrtree walk hit record cap; list may be truncated");
            break;
        }
        budget -= 1;

        let name = format!("{}.{}", hash, link.domain);
        let records = match resolver.txt(&name).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(name, error = %e, "failed to resolve tree node");
                continue;
            }
        };
        let Some(content) = records.into_iter().next() else {
            continue;
        };

        // Self-verify: the content must hash to the subdomain we asked for.
        if subdomain_hash(&content) != hash {
            tracing::debug!(hash, "tree node content/hash mismatch; skipping");
            continue;
        }

        if let Some(branch) = content.strip_prefix("enrtree-branch:") {
            for child in branch.split(',').map(str::trim).filter(|h| !h.is_empty()) {
                stack.push(child.to_string());
            }
        } else if content.starts_with("enr:") {
            match WakuEnr::from_str(&content) {
                Ok(enr) => enrs.push(enr),
                Err(e) => tracing::debug!(error = %e, "skipping unparseable ENR"),
            }
        }
        // `enrtree://` links are intentionally not followed.
    }

    Ok(enrs)
}

/// Live DNS resolver backed by `hickory-resolver`.
#[cfg(feature = "dns")]
pub struct HickoryResolver {
    resolver: hickory_resolver::TokioResolver,
}

#[cfg(feature = "dns")]
impl HickoryResolver {
    /// Build a resolver from the system configuration (e.g. `/etc/resolv.conf`).
    pub fn system() -> Result<Self, DnsError> {
        let resolver = hickory_resolver::Resolver::builder_tokio()
            .map_err(|e| DnsError::Resolve(e.to_string()))?
            .build()
            .map_err(|e| DnsError::Resolve(e.to_string()))?;
        Ok(Self { resolver })
    }
}

#[cfg(feature = "dns")]
#[async_trait]
impl TxtResolver for HickoryResolver {
    async fn txt(&self, name: &str) -> Result<Vec<String>, DnsError> {
        use hickory_resolver::proto::rr::{rdata::TXT, RecordType};

        let lookup = self
            .resolver
            .lookup(name, RecordType::TXT)
            .await
            .map_err(|e| DnsError::Resolve(e.to_string()))?;

        let mut out = Vec::new();
        for record in lookup.answers() {
            if let Some(txt) = record.try_borrow::<TXT>() {
                // Reassemble the character-strings with no separator.
                let mut bytes = Vec::new();
                for chunk in txt.data().txt_data.iter() {
                    bytes.extend_from_slice(chunk);
                }
                out.push(String::from_utf8_lossy(&bytes).into_owned());
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// In-memory resolver for offline tests.
    struct MapResolver(HashMap<String, Vec<String>>);

    #[async_trait]
    impl TxtResolver for MapResolver {
        async fn txt(&self, name: &str) -> Result<Vec<String>, DnsError> {
            Ok(self.0.get(name).cloned().unwrap_or_default())
        }
    }

    #[test]
    fn parses_enrtree_url() {
        // 33 zero bytes base32-encoded → a valid (if useless) key blob.
        let key32 = BASE32_NOPAD.encode(&[0u8; 33]);
        let url = format!("enrtree://{key32}@nodes.example.org");
        let link = parse_enrtree(&url).unwrap();
        assert_eq!(link.domain, "nodes.example.org");
        assert_eq!(link.pubkey.len(), 33);
        assert!(parse_enrtree("https://nope").is_err());
    }

    #[test]
    fn subdomain_hash_is_base32_keccak_prefix() {
        // 16 bytes → 26 base32 chars, uppercase, no padding.
        let h = subdomain_hash("enrtree-branch:");
        assert_eq!(h.len(), 26);
        assert!(h
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()));
    }

    // Build a tiny signed tree in memory and walk it end-to-end.
    #[tokio::test]
    async fn resolves_a_signed_tree() {
        use k256::ecdsa::{signature::hazmat::PrehashSigner, Signature, SigningKey};

        let domain = "nodes.test";

        // A real, parseable ENR as the single leaf.
        let cfg = crate::DiscoveryConfig::new(0).with_cluster(1, vec![0, 1]);
        // `to_base64()` already includes the `enr:` prefix.
        let enr_text = crate::Discovery::new(cfg).unwrap().local_enr().to_base64();
        let enr_hash = subdomain_hash(&enr_text);

        // One branch pointing at the leaf.
        let branch = format!("enrtree-branch:{enr_hash}");
        let branch_hash = subdomain_hash(&branch);

        // Tree signing key → URL pubkey.
        let signing = SigningKey::from_slice(&[7u8; 32]).unwrap();
        let pubkey = signing.verifying_key().to_sec1_bytes(); // compressed
        let url = format!("enrtree://{}@{domain}", BASE32_NOPAD.encode(&pubkey));

        // Sign the root content (everything before " sig=").
        let signed = format!("enrtree-root:v1 e={branch_hash} l={branch_hash} seq=1");
        let prehash = Keccak256::digest(signed.as_bytes());
        let sig: Signature = signing.sign_prehash(&prehash).unwrap();
        let mut sig_bytes = sig.to_bytes().to_vec(); // 64 bytes r‖s
        sig_bytes.push(0); // append a recovery byte to mimic the 65-byte form
        let root = format!("{signed} sig={}", BASE64URL_NOPAD.encode(&sig_bytes));

        let mut map = HashMap::new();
        map.insert(domain.to_string(), vec![root]);
        map.insert(format!("{branch_hash}.{domain}"), vec![branch]);
        map.insert(format!("{enr_hash}.{domain}"), vec![enr_text]);
        let resolver = MapResolver(map);

        let enrs = resolve(&url, &resolver).await.unwrap();
        assert_eq!(enrs.len(), 1, "should resolve the single leaf ENR");
        assert!(enrs[0].tcp4().is_none() || enrs[0].ip4().is_some());
    }

    #[cfg(feature = "dns")]
    #[tokio::test]
    #[ignore = "hits live DNS (Status prod enrtree)"]
    async fn resolves_status_prod_tree() {
        let resolver = HickoryResolver::system().expect("system resolver");
        let enrs = resolve(waku_core::TWN.dns_discovery_enrtree, &resolver)
            .await
            .expect("resolve prod tree");
        assert!(!enrs.is_empty(), "prod enrtree should yield ENRs");
        // Every record should carry relay shards for some cluster.
        assert!(enrs.iter().any(|e| crate::enr_relay_shards(e).is_some()));
    }

    #[tokio::test]
    async fn rejects_a_bad_signature() {
        let domain = "nodes.test";
        let url = format!("enrtree://{}@{domain}", BASE32_NOPAD.encode(&[2u8; 33]));
        let root = "enrtree-root:v1 e=AAAA l=AAAA seq=1 sig=AAAA".to_string();
        let mut map = HashMap::new();
        map.insert(domain.to_string(), vec![root]);
        let resolver = MapResolver(map);
        assert!(resolve(&url, &resolver).await.is_err());
    }
}
