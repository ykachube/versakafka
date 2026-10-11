//! kp-eventid - Event ID generation modes and headers

pub mod modes;
pub mod headers;
pub mod hmac;
pub mod config;

use thiserror::Error;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum EventIdError {
    #[error("generation error: {0}")]
    Generation(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("hmac error: {0}")]
    Hmac(String),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, EventIdError>;

/// Event ID modes as specified in the design
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventIdMode {
    /// Keep producer-supplied event-id header, generate if missing
    Preserve,
    /// Generate UUIDv7 (time-ordered)
    GenerateUuidv7,
    /// Deterministic: hash of producer identity + sequence
    Deterministic,
    /// Content hash of topic, key, value
    ContentHash,
    /// Reject duplicates via seen-ID cache
    RejectDuplicate,
}

impl Default for EventIdMode {
    fn default() -> Self {
        Self::Preserve
    }
}