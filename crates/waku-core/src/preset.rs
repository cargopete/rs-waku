//! Network presets and protocol constants.
//!
//! Source of truth is nwaku `waku/factory/networks_config.nim` and
//! 64/WAKU2-NETWORK. Treat everything chain-related as CONFIG, not constants —
//! see the RLN caveats below.

/// Absolute maximum 14/WAKU2-MESSAGE size after protobuf serialization.
///
/// 64/WAKU2-NETWORK says "150 kilobytes". nwaku enforces 150 * 1024. We follow
/// the implementation. (`--max-msg-size` overrides.)
pub const MAX_MESSAGE_SIZE: usize = 150 * 1024;

/// Recommended average message size (soft target, not enforced).
pub const RECOMMENDED_MESSAGE_SIZE: usize = 4 * 1024;

/// A relay node MUST reject messages whose timestamp deviates by more than this
/// from local time, in either direction (64/WAKU2-NETWORK).
pub const TIMESTAMP_TOLERANCE_SECS: i64 = 20;

/// max_epoch_gap for RLN proofs (seconds). Proofs outside this window are rejected.
pub const RLN_MAX_EPOCH_GAP_SECS: u64 = 20;

/// Per-shard bandwidth at/above which unproofed messages SHOULD be ignored (bits/s).
pub const SHARD_BANDWIDTH_IGNORE_THRESHOLD_BPS: u64 = 1_000_000;

// --- libp2p protocol ids ---
pub const RELAY_PROTOCOL_ID: &str = "/vac/waku/relay/2.0.0";
pub const METADATA_PROTOCOL_ID: &str = "/vac/waku/metadata/1.0.0";
pub const STORE_QUERY_PROTOCOL_ID: &str = "/vac/waku/store-query/3.0.0";
pub const FILTER_SUBSCRIBE_PROTOCOL_ID: &str = "/vac/waku/filter-subscribe/2.0.0-beta1";
pub const FILTER_PUSH_PROTOCOL_ID: &str = "/vac/waku/filter-push/2.0.0-beta1";
pub const LIGHTPUSH_PROTOCOL_ID: &str = "/vac/waku/lightpush/3.0.0";
pub const PEER_EXCHANGE_PROTOCOL_ID: &str = "/vac/waku/peer-exchange/2.0.0-alpha1";

/// A Waku network preset.
#[derive(Clone, Debug)]
pub struct NetworkPreset {
    pub name: &'static str,
    pub cluster_id: u16,
    pub shard_count: u16,
    /// RLN messages allowed per membership per epoch. Spec text says 100; the
    /// in-code nwaku cluster-1 preset has historically used 20. CONFIGURABLE.
    pub rln_user_message_limit: u64,
    /// RLN epoch length in seconds (TWN: 600 = 10 minutes).
    pub rln_epoch_size_secs: u64,
    /// EIP-1459 bootstrap enrtree.
    pub dns_discovery_enrtree: &'static str,
    /// RLN membership contract chain id.
    ///
    /// ⚠ The "Linea mainnet 59144" premise is UNCONFIRMED. The de-facto
    /// deployments are Sepolia-class testnets. Verify against
    /// `networks_config.nim` + the rlnv2-contract repo before mainnet wiring.
    pub rln_chain_id: u64,
    /// RLN membership contract address (hex). See chain-id caveat above.
    pub rln_contract_address: &'static str,
}

/// The Waku Network mainnet (TWN): cluster 1, shards 0–7.
pub const TWN: NetworkPreset = NetworkPreset {
    name: "twn",
    cluster_id: 1,
    shard_count: 8,
    rln_user_message_limit: 100,
    rln_epoch_size_secs: 600,
    dns_discovery_enrtree:
        "enrtree://AIRVQ5DDA4FFWLRBCHJWUWOO6X6S4ZTZ5B667LQ6AJU6PEYDLRD5O@sandbox.waku.nodes.status.im",
    // CAVEAT: testnet placeholders. nwaku in-code preset references this on
    // Ethereum Sepolia (11155111); nwaku-compose default uses
    // 0xB9cd878C90E49F797B4431fBF4fb333108CB90e6 on Linea Sepolia (59141).
    rln_chain_id: 11_155_111,
    rln_contract_address: "0xCB33Aa5B38d79E3D9Fa8B10afF38AA201399a7e3",
};

/// Status test fleet enrtree (handy during bring-up).
pub const TEST_FLEET_ENRTREE: &str =
    "enrtree://AOGYWMBYOUIMOENHXCHILPKY3ZRFEULMFI4DOM442QSZ73TT2A7VI@test.waku.nodes.status.im";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::ShardId;

    #[test]
    fn twn_shards_render() {
        assert_eq!(TWN.cluster_id, 1);
        let topics: Vec<String> = (0..TWN.shard_count)
            .map(|s| ShardId::new(TWN.cluster_id, s).pubsub_topic())
            .collect();
        assert_eq!(topics[0], "/waku/2/rs/1/0");
        assert_eq!(topics[7], "/waku/2/rs/1/7");
        assert_eq!(topics.len(), 8);
    }
}
