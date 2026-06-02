//! 23/WAKU2-TOPICS — content topic parsing.
//!
//! Format: `/<application>/<version>/<content-topic-name>/<encoding>`.
//! (A `<generation>` prefix exists for autosharding variants; we keep the
//! common 4-segment form and can extend later.)

use std::fmt;

use crate::error::{CoreError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentTopic {
    pub application: String,
    pub version: String,
    pub name: String,
    pub encoding: String,
}

impl ContentTopic {
    pub fn parse(s: &str) -> Result<Self> {
        // Must be absolute and have exactly four non-empty segments.
        let rest = s
            .strip_prefix('/')
            .ok_or_else(|| CoreError::InvalidContentTopic(s.to_string()))?;
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 4 || parts.iter().any(|p| p.is_empty()) {
            return Err(CoreError::InvalidContentTopic(s.to_string()));
        }
        Ok(Self {
            application: parts[0].to_string(),
            version: parts[1].to_string(),
            name: parts[2].to_string(),
            encoding: parts[3].to_string(),
        })
    }
}

impl fmt::Display for ContentTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "/{}/{}/{}/{}",
            self.application, self.version, self.name, self.encoding
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let ct = ContentTopic::parse("/toychat/2/huilong/proto").unwrap();
        assert_eq!(ct.application, "toychat");
        assert_eq!(ct.version, "2");
        assert_eq!(ct.name, "huilong");
        assert_eq!(ct.encoding, "proto");
        assert_eq!(ct.to_string(), "/toychat/2/huilong/proto");
    }

    #[test]
    fn rejects_malformed() {
        for bad in ["toychat/2/huilong/proto", "/a/b/c", "/a/b/c/d/e", "/a//c/d"] {
            assert!(ContentTopic::parse(bad).is_err(), "should reject {bad:?}");
        }
    }
}
