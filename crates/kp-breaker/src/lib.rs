//! kp-breaker - Local adaptive circuit breaker and ARC client

pub mod local;
pub mod arc;
pub mod composition;
pub mod config;

use thiserror::Error;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum BreakerError {
    #[error("local breaker error: {0}")]
    Local(String),
    #[error("arc client error: {0}")]
    Arc(String),
    #[error("composition error: {0}")]
    Composition(String),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, BreakerError>;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl Default for BreakerState {
    fn default() -> Self {
        Self::Closed
    }
}