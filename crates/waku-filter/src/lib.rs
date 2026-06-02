//! # waku-filter — 12/WAKU2-FILTER v2
//!
//! Light-client content filtering. Two sub-protocols:
//!   - `/vac/waku/filter-subscribe/2.0.0-beta1` (client → full node);
//!   - `/vac/waku/filter-push/2.0.0-beta1` (full node → client).
//!
//! Subscriptions require a periodic refresh ping (~5 min) or they lapse.
//! Filter v1 is removed.
//!
//! **Milestone 4.**

pub use waku_core::preset::{
    FILTER_PUSH_PROTOCOL_ID as PUSH_PROTOCOL_ID,
    FILTER_SUBSCRIBE_PROTOCOL_ID as SUBSCRIBE_PROTOCOL_ID,
};
