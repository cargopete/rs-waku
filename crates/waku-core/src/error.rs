//! Core error type.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid content topic {0:?}: expected /<app>/<version>/<name>/<encoding>")]
    InvalidContentTopic(String),

    #[error("invalid pubsub topic {0:?}: expected /waku/2/rs/<cluster>/<shard>")]
    InvalidPubsubTopic(String),

    #[error("message exceeds maximum size: {actual} > {max} bytes")]
    MessageTooLarge { actual: usize, max: usize },
}

pub type Result<T> = std::result::Result<T, CoreError>;
