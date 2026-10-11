//! kp-protocol - Kafka protocol codec wrapper with API version negotiation

pub mod codec;
pub mod api_versions;
pub mod frame;
pub mod golden;
pub mod config;

use thiserror::Error;
use kafka_protocol::protocol::{Decodable, Encodable};

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("decode error: {0}")]
    Decode(String),
    #[error("encode error: {0}")]
    Encode(String),
    #[error("unsupported api version: {0}")]
    UnsupportedVersion(i16),
    #[error("api version negotiation failed: {0}")]
    NegotiationFailed(String),
    #[error("invalid frame: {0}")]
    InvalidFrame(String),
}

pub type Result<T> = std::result::Result<T, ProtocolError>;

/// Trait for Kafka request/response types that can be encoded/decoded
pub trait KafkaCodec: Encodable + Decodable + Send + Sync + 'static {
    const API_KEY: i16;
    const MIN_VERSION: i16;
    const MAX_VERSION: i16;
}