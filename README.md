# rs-waku

A native-Rust [Waku](https://waku.org) ("Logos Messaging") node — aiming at
feature parity with [nwaku](https://github.com/waku-org/nwaku) / go-waku by
layering each Waku protocol as a libp2p `NetworkBehaviour` on top of
`rust-libp2p`, `sigp/discv5`, and `zerokit`.

> **Status: Milestone 1 in progress.** A runnable relay node exists: it stands
> up a libp2p swarm, subscribes to the TWN shards, and relays `WakuMessage`s over
> gossipsub with the correct Waku message-id and StrictNoSign. A two-node
> loopback interop test passes. Metadata, discv5 and DNS discovery are next, then
> the gate against a live nwaku node.

## What works today

- **`waku-core`** — `WakuMessage` (prost, no `protoc`), the RFC-14 deterministic
  message hash, content-topic parsing, autosharding, and the TWN preset. Unit-tested.
- **`waku-relay`** — gossipsub v1.1 configured the Waku way: `message_id` = the
  RFC-14 hash, `ValidationMode::Anonymous` (StrictNoSign), go-libp2p mesh defaults.
- **`waku-node`** — composes `relay + identify` into one `#[derive(NetworkBehaviour)]`
  swarm driven by a single task; the rest of the app talks to it over command/event
  channels (`subscribe` / `publish` / `dial`).
- **`wakunode`** — a CLI (nwaku-style flags) that runs the above as a real relay node.
- **Loopback interop test** — two nodes do a Noise handshake, form a gossipsub
  mesh, exchange a message, and the received id equals the RFC-14 hash.

## Workspace

| Crate | Spec | Status |
|---|---|---|
| `waku-core` | 14/WAKU2-MESSAGE, RFC-14 hash, sharding, presets | ✅ done |
| `waku-relay` | 11/WAKU2-RELAY (gossipsub) | ✅ done (scoring TODO) |
| `waku-node` | composition / swarm driver / config | 🟡 M1 (relay+identify) |
| `wakunode` | node binary (nwaku-style CLI) | 🟡 M1 |
| `waku-metadata` | 66/WAKU2-METADATA | ⬜ M1 |
| `waku-enr` | 31/WAKU2-ENR | ⬜ M1 |
| `waku-discv5` | 33/WAKU2-DISCV5 + EIP-1459 DNS | ⬜ M1 |
| `waku-rln` | 17/WAKU2-RLN-RELAY (RLN-V2) | ⬜ M2 |
| `waku-store` | 13/WAKU2-STORE v3 + Store-Sync | ⬜ M3 |
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
- [ ] 66/WAKU2-METADATA: cluster/shard handshake; disconnect on cluster mismatch.
- [ ] 31/WAKU2-ENR + 33/WAKU2-DISCV5: Waku-isolated discv5, shard filtering.
- [ ] EIP-1459 DNS discovery (enrtree TXT resolver) for bootstrap.
- [ ] WSS / QUIC transports for browser interop.
- [ ] **Gate:** join a live nwaku via the simulator; confirm we stay in-mesh
  (not pruned/penalized) and a real nwaku-produced hash vector matches ours.

**Milestone 2 — RLN-Relay** (the routing protocol of TWN)
- [ ] zerokit `rln` integration: proof gen/verify (Groth16/BN254/Poseidon).
- [ ] Keystore (WAKU-RLN-KEYSTORE format).
- [ ] On-chain group manager (`alloy`): register rate-commitment, event-sync the tree.
- [ ] Per-epoch nullifier tracking (double-signaling detection).
- [ ] Wire as a gossipsub validator (`validate_messages()` → Accept/Reject/Ignore).
- [ ] **Gate:** bidirectional RLN proof verification with nwaku on a shared chain.

**Milestone 3 — Store**
- [ ] 13/WAKU2-STORE v3 query server + client (`sqlx`, SQLite then Postgres).
- [ ] Retention policies (time / capacity / size); store-on-relay write path.
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

Key flags (mirroring nwaku): `--cluster-id`, `--shard` (repeatable; empty = all
shards), `--tcp-port`, `--staticnode` (repeatable), `--rln-relay` (M2).

## License

MIT OR Apache-2.0.
