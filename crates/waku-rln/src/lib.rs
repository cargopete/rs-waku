//! # waku-rln — 17/WAKU2-RLN-RELAY (RLN-V2 = RFC 58)
//!
//! Rate-Limiting Nullifier proofs gating relay publication, built directly on
//! zerokit's `rln` crate (Groth16 / BN254 / Poseidon, depth-20 Merkle tree,
//! bundled `arkzkey` + iden3 witness graph — no `protoc`, no FFI, no network).
//! Using zerokit directly keeps proofs byte-compatible with nwaku and js-rln.
//!
//! This module currently covers the proof primitives: identities, membership
//! registration into the Merkle tree, proof generation, and verification. Still
//! to come for full M2: the WAKU-RLN-KEYSTORE format, the on-chain group manager
//! (`alloy`), per-epoch nullifier tracking, and the gossipsub validator seam.
//!
//! ⚠ INTEROP CAVEATS (matter for nwaku byte-compatibility, not self-roundtrip):
//! - `x = hash_to_field_le(signal)` — endianness and the exact signal bytes must
//!   match nwaku before cross-impl proofs verify.
//! - `external_nullifier = Poseidon(epoch, rln_identifier)` — the epoch→field
//!   derivation and the `rln_identifier` domain-separator constant must match
//!   nwaku's (`waku/waku_rln_relay`).
//! - Pin the `rln` crate to the exact zerokit version nwaku vendors.

use rln::prelude::{
    hash_to_field_le, keygen, poseidon_hash, Fr, IdSecret, Proof, RLNProofValues, RLNWitnessInput,
    DEFAULT_TREE_DEPTH, RLN,
};
use thiserror::Error;

pub use rln::prelude::{Fr as Field, Proof as ZkProof};

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

/// A generated RLN proof bundled with the signal hash it commits to.
pub struct RlnProof {
    proof: Proof,
    values: RLNProofValues,
    x: Fr,
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

        let (proof, values) = self
            .rln
            .generate_rln_proof(&witness)
            .map_err(|e| RlnError::Rln(e.to_string()))?;

        Ok(RlnProof { proof, values, x })
    }

    /// Verify a proof against the current membership root and its committed signal.
    pub fn verify(&self, proof: &RlnProof) -> Result<bool, RlnError> {
        let roots = [self.root()];
        Ok(self
            .rln
            .verify_with_roots(&proof.proof, &proof.values, &proof.x, &roots)
            .unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_roundtrip_and_tamper_detection() {
        let mut relay = RlnRelay::new().expect("rln init");
        let identity = RlnIdentity::generate(100);
        let index = relay.register(&identity).expect("register");

        let proof = relay
            .prove(&identity, index, b"hello rln", 42, 1)
            .expect("prove");

        // A valid proof verifies against the membership root.
        assert!(relay.verify(&proof).expect("verify"));

        // Tampering with the committed signal must fail verification.
        let mut tampered = proof;
        tampered.x = hash_to_field_le(b"a different message");
        assert!(!relay.verify(&tampered).expect("verify tampered"));
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
}
