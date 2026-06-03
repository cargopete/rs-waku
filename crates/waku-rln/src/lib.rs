//! # waku-rln — 17/WAKU2-RLN-RELAY (RLN-V2 = RFC 58)
//!
//! Rate-Limiting Nullifier proofs gating relay publication, built directly on
//! zerokit's `rln` crate (Groth16 / BN254 / Poseidon, depth-20 Merkle tree,
//! bundled `arkzkey` + iden3 witness graph — no `protoc`, no FFI, no network).
//! Using zerokit directly keeps proofs byte-compatible with nwaku and js-rln.
//!
//! Covered: identities, membership registration into the Merkle tree, proof
//! generation/verification (incl. cross-node verify with a shared membership),
//! proof byte (de)serialization, and per-epoch nullifier tracking with
//! double-signal detection + Shamir secret recovery. Still to come for full M2:
//! the WAKU-RLN-KEYSTORE format, the on-chain group manager (`alloy`), and
//! wrapping the proof bytes in nwaku's `RateLimitProof` protobuf.
//!
//! ⚠ INTEROP CAVEATS (matter for nwaku byte-compatibility, not self-roundtrip):
//! - `x = hash_to_field_le(signal)` — endianness and the exact signal bytes must
//!   match nwaku before cross-impl proofs verify.
//! - `external_nullifier = Poseidon(epoch, rln_identifier)` — the epoch→field
//!   derivation and the `rln_identifier` domain-separator constant must match
//!   nwaku's (`waku/waku_rln_relay`).
//! - Pin the `rln` crate to the exact zerokit version nwaku vendors.

use std::collections::HashMap;

use rln::prelude::{
    bytes_le_to_rln_proof, compute_id_secret, fr_to_bytes_le, hash_to_field_le, keygen,
    poseidon_hash, rln_proof_to_bytes_le, Fr, IdSecret, RLNProof, RLNWitnessInput,
    DEFAULT_TREE_DEPTH, RLN,
};
use thiserror::Error;

pub use rln::prelude::Fr as Field;

#[derive(Debug, Error)]
pub enum RlnError {
    #[error("rln error: {0}")]
    Rln(String),
}

/// An RLN membership identity: the secret, its Poseidon commitment, and the
/// per-epoch message limit baked into the rate commitment.
#[derive(Clone)]
pub struct RlnIdentity {
    secret: IdSecret,
    commitment: Fr,
    pub user_message_limit: u64,
}

impl RlnIdentity {
    /// Generate a fresh random identity with the given per-epoch message limit.
    pub fn generate(user_message_limit: u64) -> Self {
        let (secret, commitment) = keygen();
        Self {
            secret,
            commitment,
            user_message_limit,
        }
    }

    /// `identity_commitment = Poseidon(identity_secret_hash)`.
    pub fn commitment(&self) -> Fr {
        self.commitment
    }

    /// The Merkle-tree leaf: `rate_commitment = Poseidon(id_commitment, user_message_limit)`.
    pub fn rate_commitment(&self) -> Fr {
        poseidon_hash(&[self.commitment, Fr::from(self.user_message_limit)])
    }
}

/// A generated RLN proof: the Groth16 proof plus its public values (root, x, y,
/// nullifier, external_nullifier). Serializable for attachment to a message.
pub struct RlnProof {
    inner: RLNProof,
}

impl RlnProof {
    /// `external_nullifier = Poseidon(epoch, rln_identifier)` — identifies the epoch.
    pub fn external_nullifier(&self) -> Fr {
        *self.inner.proof_values.external_nullifier()
    }

    /// The internal nullifier — equal for the same (identity, epoch, message_id).
    pub fn nullifier(&self) -> Fr {
        *self.inner.proof_values.nullifier()
    }

    /// The Shamir share `(x, y)` on the secret-sharing line for this message.
    pub fn share(&self) -> (Fr, Fr) {
        (*self.inner.proof_values.x(), *self.inner.proof_values.y())
    }

    /// The Merkle root the proof was generated against.
    pub fn root(&self) -> Fr {
        *self.inner.proof_values.root()
    }

    /// Serialize to bytes (zerokit little-endian proof encoding) for attaching to
    /// a message's `rate_limit_proof`.
    ///
    /// ⚠ This is zerokit's proof encoding, not yet wrapped in nwaku's
    /// `RateLimitProof` protobuf — that framing is a separate interop step.
    pub fn to_bytes(&self) -> Result<Vec<u8>, RlnError> {
        rln_proof_to_bytes_le(&self.inner).map_err(|e| RlnError::Rln(e.to_string()))
    }

    /// Reconstruct a proof from [`to_bytes`](Self::to_bytes) output.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RlnError> {
        let (inner, _) = bytes_le_to_rln_proof(bytes).map_err(|e| RlnError::Rln(e.to_string()))?;
        Ok(Self { inner })
    }
}

/// Outcome of observing a proof's nullifier within an epoch.
#[derive(Debug, Clone, PartialEq)]
pub enum NullifierOutcome {
    /// First sighting of this `(epoch, nullifier)` — the message is original.
    Ok,
    /// The exact same share was seen before — a replay of the same message.
    Duplicate,
    /// The nullifier reappeared with a *different* share: the publisher exceeded
    /// their rate limit (double-signaling), so their identity secret is recovered.
    DoubleSignaling { recovered_secret: Fr },
}

/// Map of internal-nullifier bytes → the Shamir share `(x, y)` first seen for it.
type EpochNullifiers = HashMap<Vec<u8>, (Fr, Fr)>;

/// Tracks seen nullifiers per epoch to detect double-signaling (RFC 32/58).
///
/// Keyed by `external_nullifier` (the epoch) then by the internal nullifier; a
/// repeat nullifier with a different Shamir share reveals the identity secret.
#[derive(Default)]
pub struct NullifierLog {
    seen: HashMap<Vec<u8>, EpochNullifiers>,
}

impl NullifierLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a proof's nullifier and classify it.
    pub fn observe(&mut self, proof: &RlnProof) -> Result<NullifierOutcome, RlnError> {
        let epoch_key = fr_to_bytes_le(&proof.external_nullifier());
        let nullifier_key = fr_to_bytes_le(&proof.nullifier());
        let (x, y) = proof.share();

        let bucket = self.seen.entry(epoch_key).or_default();
        match bucket.get(&nullifier_key) {
            None => {
                bucket.insert(nullifier_key, (x, y));
                Ok(NullifierOutcome::Ok)
            }
            Some(&(px, py)) if px == x && py == y => Ok(NullifierOutcome::Duplicate),
            Some(&prev_share) => {
                let secret = compute_id_secret(prev_share, (x, y))
                    .map_err(|e| RlnError::Rln(e.to_string()))?;
                Ok(NullifierOutcome::DoubleSignaling {
                    recovered_secret: *secret,
                })
            }
        }
    }

    /// Forget tracking for an epoch that has aged out of the validity window.
    pub fn forget_epoch(&mut self, external_nullifier: &Fr) {
        self.seen.remove(&fr_to_bytes_le(external_nullifier));
    }

    /// Number of epochs currently tracked.
    pub fn tracked_epochs(&self) -> usize {
        self.seen.len()
    }
}

/// An RLN-Relay context: the zkey + witness graph + the membership Merkle tree.
pub struct RlnRelay {
    rln: RLN,
    rln_identifier: Fr,
}

impl RlnRelay {
    /// Build an RLN-Relay over a fresh empty depth-20 in-memory membership tree.
    pub fn new() -> Result<Self, RlnError> {
        let rln = RLN::new(DEFAULT_TREE_DEPTH, "").map_err(|e| RlnError::Rln(e.to_string()))?;
        // Domain separator. ⚠ Must equal nwaku's RLN_IDENTIFIER for interop.
        let rln_identifier = hash_to_field_le(b"rln-relay-v2");
        Ok(Self {
            rln,
            rln_identifier,
        })
    }

    /// Register a member's rate commitment at the next free leaf; returns its index.
    pub fn register(&mut self, identity: &RlnIdentity) -> Result<usize, RlnError> {
        let index = self.rln.leaves_set();
        self.rln
            .set_next_leaf(identity.rate_commitment())
            .map_err(|e| RlnError::Rln(e.to_string()))?;
        Ok(index)
    }

    /// The current Merkle root over registered members.
    pub fn root(&self) -> Fr {
        self.rln.get_root()
    }

    /// `external_nullifier = Poseidon(epoch, rln_identifier)` for a given epoch.
    pub fn external_nullifier(&self, epoch: u64) -> Fr {
        poseidon_hash(&[Fr::from(epoch), self.rln_identifier])
    }

    /// Generate an RLN proof that `signal` was published by the member at
    /// `index`, in `epoch`, using slot `message_id` (`0 ≤ message_id < limit`).
    pub fn prove(
        &self,
        identity: &RlnIdentity,
        index: usize,
        signal: &[u8],
        epoch: u64,
        message_id: u64,
    ) -> Result<RlnProof, RlnError> {
        let (path_elements, identity_path_index) = self
            .rln
            .get_merkle_proof(index)
            .map_err(|e| RlnError::Rln(e.to_string()))?;

        let x = hash_to_field_le(signal);
        let external_nullifier = poseidon_hash(&[Fr::from(epoch), self.rln_identifier]);

        let witness = RLNWitnessInput::new(
            identity.secret.clone(),
            Fr::from(identity.user_message_limit),
            Fr::from(message_id),
            path_elements,
            identity_path_index,
            x,
            external_nullifier,
        )
        .map_err(|e| RlnError::Rln(e.to_string()))?;

        let (proof, proof_values) = self
            .rln
            .generate_rln_proof(&witness)
            .map_err(|e| RlnError::Rln(e.to_string()))?;

        Ok(RlnProof {
            inner: RLNProof {
                proof,
                proof_values,
            },
        })
    }

    /// Verify a proof against the current membership root and its committed signal.
    pub fn verify(&self, proof: &RlnProof) -> Result<bool, RlnError> {
        let roots = [self.root()];
        Ok(self
            .rln
            .verify_with_roots(
                &proof.inner.proof,
                &proof.inner.proof_values,
                proof.inner.proof_values.x(),
                &roots,
            )
            .unwrap_or(false))
    }
}

/// Verdict for an inbound message's RLN proof.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundVerdict {
    /// The proof is valid for this message and epoch; `double_signaling` is set
    /// if its nullifier was already used with a different message this epoch.
    Valid { double_signaling: bool },
    /// The proof is missing, malformed, doesn't bind to the message, fails
    /// cryptographic verification, or is outside the accepted epoch window.
    Invalid,
}

/// Inbound-side RLN: verifies that a proof attached to a received message is
/// valid for that message, was generated for the current epoch, and is not a
/// double-signal. Owns the membership tree (for verification) and the per-epoch
/// [`NullifierLog`].
///
/// ⚠ Epoch handling accepts the current and immediately previous epoch (a coarse
/// stand-in for nwaku's `max_epoch_gap` seconds window — reconcile before
/// cross-impl interop).
pub struct RlnValidator {
    relay: RlnRelay,
    log: NullifierLog,
    epoch_size_secs: u64,
}

impl RlnValidator {
    pub fn new(relay: RlnRelay, epoch_size_secs: u64) -> Self {
        Self {
            relay,
            log: NullifierLog::new(),
            epoch_size_secs: epoch_size_secs.max(1),
        }
    }

    /// Register a member into the membership tree used for verification.
    pub fn register(&mut self, identity: &RlnIdentity) -> Result<usize, RlnError> {
        self.relay.register(identity)
    }

    /// The epoch index for a wall-clock time in seconds.
    pub fn epoch_at(&self, now_secs: u64) -> u64 {
        now_secs / self.epoch_size_secs
    }

    /// Verify an inbound proof that should commit to `signal`, as of `now_secs`.
    pub fn verify_inbound(
        &mut self,
        proof: &RlnProof,
        signal: &[u8],
        now_secs: u64,
    ) -> InboundVerdict {
        // 1. The proof must bind to *this* message's signal.
        if proof.share().0 != hash_to_field_le(signal) {
            return InboundVerdict::Invalid;
        }
        // 2. Cryptographic validity against the membership root.
        if !self.relay.verify(proof).unwrap_or(false) {
            return InboundVerdict::Invalid;
        }
        // 3. Epoch window: accept the current or immediately previous epoch.
        let current = self.epoch_at(now_secs);
        let ext = proof.external_nullifier();
        let in_window = ext == self.relay.external_nullifier(current)
            || (current > 0 && ext == self.relay.external_nullifier(current - 1));
        if !in_window {
            return InboundVerdict::Invalid;
        }
        // 4. Double-signaling check.
        let double_signaling = matches!(
            self.log.observe(proof),
            Ok(NullifierOutcome::DoubleSignaling { .. })
        );
        InboundVerdict::Valid { double_signaling }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_proof_verifies() {
        let mut relay = RlnRelay::new().expect("rln init");
        let identity = RlnIdentity::generate(100);
        let index = relay.register(&identity).expect("register");
        let proof = relay
            .prove(&identity, index, b"hello rln", 42, 1)
            .expect("prove");
        assert!(relay.verify(&proof).expect("verify"));
    }

    #[test]
    fn proof_survives_serialization_roundtrip() {
        let mut relay = RlnRelay::new().expect("rln init");
        let identity = RlnIdentity::generate(100);
        let index = relay.register(&identity).expect("register");
        let proof = relay
            .prove(&identity, index, b"hello", 1, 0)
            .expect("prove");

        let bytes = proof.to_bytes().expect("serialize");
        let restored = RlnProof::from_bytes(&bytes).expect("deserialize");
        assert!(relay.verify(&restored).expect("verify restored"));
        // Same public values survive the round-trip.
        assert_eq!(restored.nullifier(), proof.nullifier());
        assert_eq!(restored.root(), proof.root());
    }

    #[test]
    fn proof_verifies_on_a_second_node_with_the_same_membership() {
        // Node A generates and serializes a proof; node B, which independently
        // registered the same member, deserializes and verifies it. This is the
        // publish→attach→receive→verify path, minus the wire framing.
        let identity = RlnIdentity::generate(100);

        let mut node_a = RlnRelay::new().expect("a init");
        let idx = node_a.register(&identity).expect("a register");
        let wire = node_a
            .prove(&identity, idx, b"cross-node", 11, 0)
            .expect("prove")
            .to_bytes()
            .expect("serialize");

        let mut node_b = RlnRelay::new().expect("b init");
        node_b.register(&identity).expect("b register"); // same leaf ⇒ same root
        let received = RlnProof::from_bytes(&wire).expect("deserialize");
        assert!(node_b.verify(&received).expect("b verify"));
    }

    #[test]
    fn proof_against_stale_root_fails() {
        let mut relay = RlnRelay::new().expect("rln init");
        let alice = RlnIdentity::generate(100);
        let idx = relay.register(&alice).expect("register alice");
        let proof = relay.prove(&alice, idx, b"msg", 7, 0).expect("prove");
        assert!(relay.verify(&proof).expect("verify"));

        // Registering another member changes the root; the old proof's root is
        // no longer current, so verification against the new root set fails.
        let bob = RlnIdentity::generate(100);
        relay.register(&bob).expect("register bob");
        assert!(!relay.verify(&proof).expect("verify stale"));
    }

    #[test]
    fn double_signaling_recovers_the_identity_secret() {
        let mut relay = RlnRelay::new().expect("rln init");
        let identity = RlnIdentity::generate(100);
        let index = relay.register(&identity).expect("register");
        let mut log = NullifierLog::new();

        // Same epoch + same message-id slot, two DIFFERENT signals: same
        // nullifier, different share → the rate limit is broken.
        let p1 = relay.prove(&identity, index, b"message one", 5, 0).unwrap();
        let p2 = relay.prove(&identity, index, b"message two", 5, 0).unwrap();

        assert_eq!(log.observe(&p1).unwrap(), NullifierOutcome::Ok);
        match log.observe(&p2).unwrap() {
            NullifierOutcome::DoubleSignaling { recovered_secret } => {
                assert_eq!(
                    recovered_secret, *identity.secret,
                    "must recover the secret"
                );
            }
            other => panic!("expected double-signaling, got {other:?}"),
        }
    }

    #[test]
    fn distinct_message_ids_within_limit_are_ok() {
        let mut relay = RlnRelay::new().expect("rln init");
        let identity = RlnIdentity::generate(100);
        let index = relay.register(&identity).expect("register");
        let mut log = NullifierLog::new();

        // Different message-id slots in one epoch → distinct nullifiers → all Ok.
        let p0 = relay.prove(&identity, index, b"a", 9, 0).unwrap();
        let p1 = relay.prove(&identity, index, b"b", 9, 1).unwrap();
        assert_eq!(log.observe(&p0).unwrap(), NullifierOutcome::Ok);
        assert_eq!(log.observe(&p1).unwrap(), NullifierOutcome::Ok);
        assert_eq!(log.tracked_epochs(), 1);
    }

    const EPOCH_SIZE: u64 = 600;

    #[test]
    fn inbound_verifier_accepts_a_well_formed_proof() {
        let identity = RlnIdentity::generate(100);
        let mut relay = RlnRelay::new().expect("relay");
        let idx = relay.register(&identity).expect("register");

        let now = 600 * 100; // epoch 100
        let epoch = now / EPOCH_SIZE;
        let proof = relay.prove(&identity, idx, b"hi", epoch, 0).expect("prove");

        let mut validator = RlnValidator::new(relay, EPOCH_SIZE);
        assert_eq!(
            validator.verify_inbound(&proof, b"hi", now),
            InboundVerdict::Valid {
                double_signaling: false
            }
        );
    }

    #[test]
    fn inbound_verifier_rejects_wrong_signal_stale_epoch_and_double_signal() {
        let identity = RlnIdentity::generate(100);
        let mut relay = RlnRelay::new().expect("relay");
        let idx = relay.register(&identity).expect("register");
        let now = 600 * 100;
        let epoch = now / EPOCH_SIZE;

        let proof = relay
            .prove(&identity, idx, b"real", epoch, 0)
            .expect("prove");
        // A proof generated for a long-past epoch.
        let stale = relay
            .prove(&identity, idx, b"old", epoch - 50, 1)
            .expect("prove");
        // Two proofs reusing slot 0 in this epoch with different signals.
        let dup_a = relay
            .prove(&identity, idx, b"spam one", epoch, 0)
            .expect("prove");
        let dup_b = relay
            .prove(&identity, idx, b"spam two", epoch, 0)
            .expect("prove");

        let mut validator = RlnValidator::new(relay, EPOCH_SIZE);

        // Proof doesn't bind to the claimed signal.
        assert_eq!(
            validator.verify_inbound(&proof, b"forged", now),
            InboundVerdict::Invalid
        );
        // Out of the epoch window.
        assert_eq!(
            validator.verify_inbound(&stale, b"old", now),
            InboundVerdict::Invalid
        );
        // First use ok, second use in the same slot is flagged as double-signaling.
        assert_eq!(
            validator.verify_inbound(&dup_a, b"spam one", now),
            InboundVerdict::Valid {
                double_signaling: false
            }
        );
        assert_eq!(
            validator.verify_inbound(&dup_b, b"spam two", now),
            InboundVerdict::Valid {
                double_signaling: true
            }
        );
    }

    #[test]
    fn replaying_the_same_message_is_a_duplicate() {
        let mut relay = RlnRelay::new().expect("rln init");
        let identity = RlnIdentity::generate(100);
        let index = relay.register(&identity).expect("register");
        let mut log = NullifierLog::new();

        let proof = relay.prove(&identity, index, b"dup", 3, 0).unwrap();
        assert_eq!(log.observe(&proof).unwrap(), NullifierOutcome::Ok);
        assert_eq!(log.observe(&proof).unwrap(), NullifierOutcome::Duplicate);
    }
}
