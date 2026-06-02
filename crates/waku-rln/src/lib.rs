//! # waku-rln — 17/WAKU2-RLN-RELAY (RLN-V2 = RFC 58)
//!
//! Rate-Limiting Nullifier proofs gating relay publication. Built on zerokit's
//! `rln` crate (Groth16 / BN254 / Poseidon, depth-20 Merkle tree, 2^16 message
//! limit bits). We use zerokit DIRECTLY — no pure-arkworks rewrite, no Go FFI —
//! so proofs stay byte-compatible with nwaku and js-rln.
//!
//! Responsibilities:
//!   - keystore (WAKU-RLN-KEYSTORE format): identity secret + rate commitment;
//!   - on-chain group manager (`alloy`): register `rate_commitment`, sync the
//!     members Merkle tree from contract events;
//!   - proof generation per published message (within `user_message_limit`);
//!   - proof verification + per-epoch nullifier tracking (double-signaling
//!     detection). Slashing is NOT enforced on TWN — enforcement is via
//!     gossipsub scoring / peer removal.
//!
//! Plugs into `waku-relay` as a gossipsub validator. Keep proof generation
//! (~0.15 s, CPU-bound) on `spawn_blocking`/rayon so it never stalls the swarm.
//!
//! ⚠ Pin the `rln` crate to the EXACT zerokit version nwaku vendors for the
//! release we target; the crates.io latest (2.0.x) may not match the plan's
//! assumed 1.0.0. Verify the contract address + chain id against
//! `networks_config.nim` and the rlnv2-contract repo.
//!
//! **Milestone 2** (de-risk proof byte-compatibility first).

/// External nullifier domain separator components (RFC 58).
#[derive(Clone, Copy, Debug)]
pub struct Epoch(pub u64);
