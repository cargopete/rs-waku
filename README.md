# rs-waku

A native-Rust [Waku](https://waku.org) ("Logos Messaging") node — aiming at
feature parity with [nwaku](https://github.com/waku-org/nwaku) / go-waku by
layering each Waku protocol as a libp2p `NetworkBehaviour` on top of
`rust-libp2p`, `sigp/discv5`, and `zerokit`.

> **Status: Milestone 1 complete — connects to TWN mainnet.** `wakunode
> --dns-discovery` resolves the live Status enrtree, discovers cluster-1 peers
> over discv5, dials them, and completes the 66/WAKU2-METADATA handshake against
> production nwaku nodes (they keep us connected). It relays `WakuMessage`s over
> gossipsub with the correct Waku message-id and StrictNoSign. Next: RLN-Relay
> (M2), needed to publish on TWN, and a formal interop pass in the simulator.

## What works today

- **`waku-core`** — `WakuMessage` (prost, no `protoc`), the RFC-14 deterministic
  message hash, content-topic parsing, autosharding, and the TWN preset. Unit-tested.
- **`waku-relay`** — gossipsub v1.1 configured the Waku way: `message_id` = the
  RFC-14 hash, `ValidationMode::Anonymous` (StrictNoSign), go-libp2p mesh defaults.
- **`waku-metadata`** — 66/WAKU2-METADATA request/response (length-prefixed
  protobuf); the node disconnects peers on a cluster-id mismatch.
- **`waku-enr` + `waku-discv5`** — the ENR relay-shards codec (`rs`/`rsv`), a
  `sigp/discv5` wrapper that discovers peers filtered to our cluster and resolves
  each ENR to a dialable libp2p address (secp256k1 ENR → libp2p peer-id bridge),
  and an EIP-1459 `enrtree` DNS resolver (verified against the live Status tree).
- **`waku-node`** — composes `relay + identify + metadata` into one
  `#[derive(NetworkBehaviour)]` swarm driven by a single task; talks to the app
  over command/event channels (`subscribe` / `publish` / `dial`); optionally runs
  discv5 and auto-dials discovered + bootstrap peers using a shared secp256k1 key.
- **`wakunode`** — a CLI (nwaku-style flags) that runs the above as a relay node
  (discv5 not yet exposed on the CLI; use `--staticnode` to connect peers).
- **Interop tests** — a two-node gossipsub loopback (received id == RFC-14 hash),
  a metadata cluster-mismatch handshake, a discv5 session, and an end-to-end
  *discover → dial → relay* test seeded with only a peer's ENR.

## Workspace

| Crate | Spec | Status |
|---|---|---|
| `waku-core` | 14/WAKU2-MESSAGE, RFC-14 hash, sharding, presets | ✅ done |
| `waku-relay` | 11/WAKU2-RELAY (gossipsub) | ✅ done (scoring TODO) |
| `waku-metadata` | 66/WAKU2-METADATA | ✅ done |
| `waku-enr` | 31/WAKU2-ENR (relay-shards codec) | ✅ done |
| `waku-discv5` | 33/WAKU2-DISCV5 + EIP-1459 DNS | ✅ done |
| `waku-node` | composition / swarm driver / config | 🟡 M1 (relay+metadata+discv5) |
| `wakunode` | node binary (nwaku-style CLI) | 🟡 M1 |
| `waku-rln` | 17/WAKU2-RLN-RELAY (RLN-V2) | 🟡 proofs done |
| `waku-store` | 13/WAKU2-STORE v3 + Store-Sync | 🟡 storage core |
| `waku-filter` | 12/WAKU2-FILTER v2 | ⬜ M4 |
| `waku-lightpush` | 19/WAKU2-LIGHTPUSH v3 | ⬜ M4 |
| `waku-peer-exchange` | 34/WAKU2-PEER-EXCHANGE | ⬜ M4 |
| `waku-rest` | nwaku-compatible REST API (port 8645) | ⬜ M5 |
| `waku-sds` | Scalable Data Sync (optional) | ⬜ M6 |

Legend: ✅ done · 🟡 partial · ⬜ stub.

## Roadmap

Each milestone is gated by an interop test against a live nwaku node (in the
[waku-simulator](https://github.com/waku-org/waku-simulator), then the Python
`waku-interop-tests` suite).

**Milestone 1 — Relay node** (transport + relay + discovery)
- [x] Transport: TCP + Noise + Yamux libp2p swarm.
- [x] 11/WAKU2-RELAY: gossipsub with Waku message-id + StrictNoSign.
- [x] Shard subscribe / publish, command-and-event node runtime.
- [x] Two-node loopback interop test.
- [x] 66/WAKU2-METADATA: cluster/shard handshake; disconnect on cluster mismatch.
- [x] 31/WAKU2-ENR: relay-shards (`rs`/`rsv`) codec.
- [x] 33/WAKU2-DISCV5: Waku discv5 (sigp/discv5), ENR shards, cluster-filtered discovery.
- [x] Wire discv5 into the node: shared secp256k1 key, ENR→libp2p peer-id bridge, auto-dial discovered/bootstrap peers (end-to-end discover→dial→relay test).
- [x] EIP-1459 DNS discovery (enrtree TXT resolver, root-sig verification); node resolves `enrtree://` bootstrap at startup. Verified against the live Status prod tree.
- [x] discv5 / dns-discovery flags on the `wakunode` CLI; `--dns-discovery` connects to TWN mainnet and passes the metadata handshake against production nwaku.
- [ ] WSS / QUIC transports for browser interop.
- [ ] **Gate:** formal in-mesh / scoring check vs nwaku in the simulator, and a
  real nwaku-produced hash vector matched against ours.

**Milestone 2 — RLN-Relay** (the routing protocol of TWN)
- [x] zerokit `rln` (2.0.x) integration: identities, membership tree, RLN-V2 proof gen/verify (Groth16/BN254/Poseidon, depth-20), tamper + stale-root tests.
- [ ] Keystore (WAKU-RLN-KEYSTORE format).
- [ ] On-chain group manager (`alloy`): register rate-commitment, event-sync the tree.
- [x] Per-epoch nullifier tracking: double-signaling detection + Shamir identity-secret recovery (tested).
- [x] Proof byte (de)serialization + cross-node verification with a shared membership (publish→attach→receive→verify, minus wire framing).
- [x] Inbound RLN verifier: binds proof↔message signal, checks the epoch window, flags double-signaling → Valid/Invalid verdict (tested). Feeds the relay validator's RLN enforcement.
- [x] Gossipsub validator seam: `validate_messages()` live; spec-faithful 64/WAKU2-NETWORK decision engine (timestamp window, no-proof/saturation, stale epoch, double-signal) → Accept/Reject/Ignore, reported back to the mesh. RLN enforcement behind a policy flag (off until inbound membership sync).
- [ ] `RateLimitProof` wire codec (nwaku protobuf layout) — attach/parse proofs on the WakuMessage; needs a real nwaku vector.
- [ ] **Gate:** bidirectional RLN proof verification with nwaku on a shared chain.

**Milestone 3 — Store**
- [x] Storage core: `MessageStore` trait + `sqlx` SQLite backend, hash-indexed; put/get/dedup, content-topic + time-range query with keyset cursor pagination, hashes-only mode, `exists`, time + capacity retention (tested).
- [ ] v3 request/response protobuf wire protocol (query server + client behaviour).
- [x] store-on-relay write path: the node persists accepted, non-ephemeral relay messages off the hot path (end-to-end test: publish → relay → store → query).
- [x] v3 request/response wire protocol (`/vac/waku/store-query/3.0.0`): LP-protobuf codec, server serves from the store, client query with request correlation (two-node over-the-wire test).
- [ ] Postgres backend; Store-Sync (Negentropy).
- [ ] Store-Sync (Negentropy / range-based set reconciliation).

**Milestone 4 — Service protocols**
- [ ] 12/WAKU2-FILTER v2 (filter-subscribe / filter-push, refresh ping).
- [ ] 19/WAKU2-LIGHTPUSH v3.
- [ ] 34/WAKU2-PEER-EXCHANGE.

**Milestone 5 — Operations**
- [ ] nwaku-compatible REST API (`axum`, port 8645): relay/store/filter/lightpush/admin/health.
- [ ] Prometheus metrics (match nwaku names for dashboard reuse).
- [ ] DoS protection: per-protocol rate limits, ip-colocation, relay:service split.
- [ ] **Gate:** pass the full Python interop suite protocol-by-protocol.

**Milestone 6 — Reliability & hardening**
- [ ] SDS (Scalable Data Sync) — port of nim-sds.
- [ ] Browser interop (WSS/QUIC), soak testing, performance.

## Interop landmines (where one byte breaks everything)

- gossipsub `message_id` **must** be the RFC-14 deterministic hash, not the default. ✅ done
- StrictNoSign (`ValidationMode::Anonymous`); never sign or populate `from`/`seqno`. ✅ done
- gossipsub scoring/mesh params replicated from go-libp2p-pubsub field-by-field. ⚠ `TODO(scoring)`
- RFC-14 hash byte layout + autosharding slice — spec-implemented, **verify against
  nwaku** (see the `#[ignore]`d `matches_nwaku_reference_vector` test, the M1 gate). ⚠
- RLN circuit artifacts come from zerokit — pin to nwaku's *vendored* version; do
  not regenerate. ⚠ crates.io `rln` is 2.0.x; the plan assumed 1.0.0 — reconcile.
- 150 KiB max message size; 20 s timestamp window. (constants in `waku-core::preset`)
- chain id / RLN contract / enrtree are **config**, not constants — the TWN preset
  currently ships Sepolia-class testnet placeholders. Verify against
  `networks_config.nim` before mainnet wiring. ⚠

## Build & run

```sh
cargo build --workspace          # fast: heavy deps (rln/alloy/sqlx) land per-milestone
cargo test  --workspace          # includes the two-node loopback interop test
cargo run -p wakunode -- --help
```

Run a relay node and connect a second one to it:

```sh
# terminal 1 — note the printed listen multiaddr
cargo run -p wakunode -- --tcp-port 60000

# terminal 2 — dial the first node
cargo run -p wakunode -- --tcp-port 60001 \
    --staticnode /ip4/127.0.0.1/tcp/60000
```

Or join **The Waku Network mainnet** via DNS discovery:

```sh
cargo run -p wakunode -- --tcp-port 60000 --discv5-udp-port 9000 --dns-discovery
# resolves the Status enrtree, discovers cluster-1 peers, dials production nwaku
# nodes, and stays connected through the metadata handshake.
```

Key flags (mirroring nwaku): `--cluster-id`, `--shard` (repeatable; empty = all
shards), `--tcp-port`, `--staticnode` (repeatable), `--discv5-discovery`,
`--discv5-udp-port`, `--ext-ip`, `--discv5-bootstrap-node` (repeatable),
`--dns-discovery`, `--dns-discovery-url` (repeatable), `--rln-relay` (M2).

## License

MIT OR Apache-2.0.
