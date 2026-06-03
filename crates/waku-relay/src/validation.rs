//! Relay message validation per 64/WAKU2-NETWORK.
//!
//! This is the decision engine that gossipsub's manual validation reports to:
//! given the facts about an inbound message ([`MessageFacts`]) and the node's
//! [`ValidationPolicy`], decide [`Validation::Accept`] / `Reject` / `Ignore`.
//!
//! The cryptographic proof check and the nwaku `RateLimitProof` wire decoding
//! live elsewhere (`waku-rln`); this module is the pure, spec-faithful policy.

use libp2p::gossipsub::MessageAcceptance;
use waku_core::preset;

/// Validation outcome (gossipsub v1.1 semantics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Validation {
    /// Forward and (optionally) store.
    Accept,
    /// Drop and penalize the sender's score.
    Reject,
    /// Drop without penalty (e.g. no RLN proof on a saturated shard).
    Ignore,
}

impl From<Validation> for MessageAcceptance {
    fn from(v: Validation) -> Self {
        match v {
            Validation::Accept => Self::Accept,
            Validation::Reject => Self::Reject,
            Validation::Ignore => Self::Ignore,
        }
    }
}

/// RLN status of an inbound message, as determined by `waku-rln`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RlnStatus {
    /// No rate-limit proof attached.
    Absent,
    /// Proof present and cryptographically valid; carries its epoch deviation
    /// from local time and whether its nullifier indicates double-signaling.
    Valid {
        epoch_gap_secs: i64,
        double_signaling: bool,
    },
    /// Proof present but invalid (bad zk proof, unknown Merkle root, …).
    Invalid,
}

/// Facts about an inbound relay message, gathered before the validity decision.
#[derive(Clone, Copy, Debug)]
pub struct MessageFacts {
    /// `now - message.timestamp` in seconds, or `None` when no timestamp is set.
    pub timestamp_gap_secs: Option<i64>,
    /// RLN proof status.
    pub rln: RlnStatus,
    /// Whether the shard is at/above the RLN bandwidth threshold (1 Mbps).
    pub shard_saturated: bool,
}

/// Policy knobs governing validation.
#[derive(Clone, Copy, Debug)]
pub struct ValidationPolicy {
    /// Enforce RLN. Off until we sync a membership tree to verify inbound proofs.
    pub enforce_rln: bool,
    pub timestamp_tolerance_secs: i64,
    pub max_epoch_gap_secs: i64,
}

impl Default for ValidationPolicy {
    fn default() -> Self {
        Self {
            enforce_rln: false,
            timestamp_tolerance_secs: preset::TIMESTAMP_TOLERANCE_SECS,
            max_epoch_gap_secs: preset::RLN_MAX_EPOCH_GAP_SECS as i64,
        }
    }
}

/// Decide the validation outcome for a message, per 64/WAKU2-NETWORK.
pub fn validate(facts: &MessageFacts, policy: &ValidationPolicy) -> Validation {
    // Timestamp deviation beyond tolerance → Reject (applies whether or not RLN
    // is enforced).
    if let Some(gap) = facts.timestamp_gap_secs {
        if gap.abs() > policy.timestamp_tolerance_secs {
            return Validation::Reject;
        }
    }

    if !policy.enforce_rln {
        return Validation::Accept;
    }

    match facts.rln {
        // No proof: ignored (no penalty) only when the shard is saturated;
        // otherwise accepted within the grace allowance.
        RlnStatus::Absent => {
            if facts.shard_saturated {
                Validation::Ignore
            } else {
                Validation::Accept
            }
        }
        RlnStatus::Invalid => Validation::Reject,
        RlnStatus::Valid {
            epoch_gap_secs,
            double_signaling,
        } => {
            if epoch_gap_secs.abs() > policy.max_epoch_gap_secs || double_signaling {
                Validation::Reject
            } else {
                Validation::Accept
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(rln: RlnStatus) -> MessageFacts {
        MessageFacts {
            timestamp_gap_secs: None,
            rln,
            shard_saturated: false,
        }
    }

    #[test]
    fn acceptance_mapping() {
        // MessageAcceptance has no PartialEq, so match instead.
        assert!(matches!(
            MessageAcceptance::from(Validation::Accept),
            MessageAcceptance::Accept
        ));
        assert!(matches!(
            MessageAcceptance::from(Validation::Reject),
            MessageAcceptance::Reject
        ));
        assert!(matches!(
            MessageAcceptance::from(Validation::Ignore),
            MessageAcceptance::Ignore
        ));
    }

    #[test]
    fn timestamp_outside_window_is_rejected() {
        let policy = ValidationPolicy::default();
        let mut f = facts(RlnStatus::Absent);
        f.timestamp_gap_secs = Some(25);
        assert_eq!(validate(&f, &policy), Validation::Reject);
        f.timestamp_gap_secs = Some(-25);
        assert_eq!(validate(&f, &policy), Validation::Reject);
        f.timestamp_gap_secs = Some(5);
        assert_eq!(validate(&f, &policy), Validation::Accept);
    }

    #[test]
    fn without_enforcement_everything_in_time_is_accepted() {
        let policy = ValidationPolicy::default(); // enforce_rln = false
        assert_eq!(
            validate(&facts(RlnStatus::Invalid), &policy),
            Validation::Accept
        );
    }

    #[test]
    fn rln_enforced_rules() {
        let policy = ValidationPolicy {
            enforce_rln: true,
            ..Default::default()
        };

        // No proof: accepted while the shard has headroom, ignored when saturated.
        assert_eq!(
            validate(&facts(RlnStatus::Absent), &policy),
            Validation::Accept
        );
        let mut saturated = facts(RlnStatus::Absent);
        saturated.shard_saturated = true;
        assert_eq!(validate(&saturated, &policy), Validation::Ignore);

        // Invalid proof → Reject.
        assert_eq!(
            validate(&facts(RlnStatus::Invalid), &policy),
            Validation::Reject
        );

        // Valid, fresh, single-use → Accept.
        assert_eq!(
            validate(
                &facts(RlnStatus::Valid {
                    epoch_gap_secs: 3,
                    double_signaling: false
                }),
                &policy
            ),
            Validation::Accept
        );

        // Stale epoch → Reject.
        assert_eq!(
            validate(
                &facts(RlnStatus::Valid {
                    epoch_gap_secs: 25,
                    double_signaling: false
                }),
                &policy
            ),
            Validation::Reject
        );

        // Double-signaling → Reject.
        assert_eq!(
            validate(
                &facts(RlnStatus::Valid {
                    epoch_gap_secs: 0,
                    double_signaling: true
                }),
                &policy
            ),
            Validation::Reject
        );
    }
}
