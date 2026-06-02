# rs-waku

A native-Rust [Waku](https://waku.org) ("Logos Messaging") node — aiming at
feature parity with [nwaku](https://github.com/waku-org/nwaku) / go-waku by
layering each Waku protocol as a libp2p `NetworkBehaviour` on top of
`rust-libp2p`, `sigp/discv5`, and `zerokit`.

> Status: **scaffold**. `waku-core` is implemented and tested; every other crate
> is a documented stub awaiting its milestone.

## Workspace

| Crate | Spec | Milestone |
|---|---|---|
| `waku-core` | 14/WAKU2-MESSAGE, RFC-14 hash, sharding, presets | ✅ done |
| `waku-metadata` | 66/WAKU2-METADATA | 1 |
| `waku-enr` | 31/WAKU2-ENR | 1 |
| `waku-discv5` | 33/WAKU2-DISCV5 + EIP-1459 DNS | 1 |
| `waku-relay` | 11/WAKU2-RELAY (gossipsub) | 1 |
| `waku-rln` | 17/WAKU2-RLN-RELAY (RLN-V2) | 2 |
| `waku-store` | 13/WAKU2-STORE v3 + Store-Sync | 3 |
| `waku-filter` | 12/WAKU2-FILTER v2 | 4 |
| `waku-lightpush` | 19/WAKU2-LIGHTPUSH v3 | 4 |
| `waku-peer-exchange` | 34/WAKU2-PEER-EXCHANGE | 4 |
| `waku-node` | composition / peer manager / config | 1+ |
| `waku-rest` | nwaku-compatible REST API (port 8645) | 5 |
| `waku-sds` | Scalable Data Sync (optional) | 6 |
| `wakunode` | node binary (nwaku-style CLI) | — |

## Roadmap

Each milestone is gated by an interop test against a live nwaku node.

1. **Transport + Metadata + Relay + Discv5 + DNS** — join `/waku/2/rs/1/0..7`,
   stay in-mesh against nwaku. *De-risk gossipsub interop first.*
2. **RLN-Relay** — zerokit proofs, on-chain membership, nullifier tracking.
   *De-risk RLN proof byte-compatibility against nwaku, in parallel.*
3. **Store v3** (SQLite → Postgres) + Store-Sync.
4. **Filter v2** + **Light Push v3** + **Peer Exchange**.
5. **REST API** + metrics + health → full Python interop suite + simulator.
6. **SDS / reliability**, QUIC/WSS browser interop, hardening.

## Interop landmines (where one byte breaks everything)

- gossipsub `message_id` **must** be the RFC-14 deterministic hash, not the default.
- StrictNoSign (`ValidationMode::Anonymous`); never sign or populate `from`/`seqno`.
- gossipsub scoring/mesh params replicated from go-libp2p-pubsub field-by-field.
- RLN circuit artifacts come from zerokit — pin to nwaku's vendored version; do not regenerate.
- 150 KiB max message size; 20 s timestamp window.
- chain id / RLN contract / enrtree are **config**, not constants — verify the
  live values against `networks_config.nim` before mainnet wiring.

## Build

```sh
cargo build            # fast: heavy deps (libp2p/rln/alloy/sqlx) land per-milestone
cargo test --workspace
cargo run -p wakunode -- --help
```

## License

MIT OR Apache-2.0.
