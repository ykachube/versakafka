//! kp-metadata - Metadata and FindCoordinator request/response rewriting

pub mod rewrite;
pub mod routing;
pub mod config;

pub use rewrite::MetadataRewriter;

use thiserror::Error;
use kp_protocol::ProtocolError;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("rewrite error: {0}")]
    Rewrite(String),
    #[error("routing error: {0}")]
    Routing(String),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("broker not found: {0}")]
    BrokerNotFound(String),
}

pub type Result<T> = std::result::Result<T, MetadataError>;