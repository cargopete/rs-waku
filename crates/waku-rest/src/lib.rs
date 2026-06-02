//! # waku-rest — nwaku-compatible REST API
//!
//! `axum` server matching nwaku's OpenAPI (`waku-org/waku-rest-api`) on port
//! 8645. This is the surface the Python interop suite and operators drive, so it
//! ships early (Milestone 5 at the latest). Endpoints to mirror:
//!   `/relay/v1/...`, `/store/v3/...`, `/filter/v2/...`,
//!   `/lightpush/v1|v3/...`, `/admin/v1/...`, `/debug/...`, `/health`.

/// Default nwaku REST port.
pub const DEFAULT_REST_PORT: u16 = 8645;
