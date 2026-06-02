//! # waku-lightpush — 19/WAKU2-LIGHTPUSH v3 (`/vac/waku/lightpush/3.0.0`)
//!
//! Lets resource-restricted nodes publish a WakuMessage by handing it to a full
//! relay node, which injects it into gossipsub. v3 is current; legacy lightpush
//! is being deprecated.
//!
//! **Milestone 4.**

pub use waku_core::preset::LIGHTPUSH_PROTOCOL_ID as PROTOCOL_ID;
