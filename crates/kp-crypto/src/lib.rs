//! kp-crypto - Envelope encryption with KMS-wrapped DEKs

pub mod envelope;
pub mod kms;
pub mod dek_cache;
pub mod headers;
pub mod config;
pub mod pipeline;

use thiserror::Error;
use kp_protocol::ProtocolError;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("envelope encryption error: {0}")]
    Envelope(String),
    #[error("kms error: {0}")]
    Kms(String),
    #[error("dek cache error: {0}")]
    DekCache(String),
    #[error("header error: {0}")]
    Header(String),
    #[error("pipeline error: {0}")]
    Pipeline(String),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, CryptoError>;

// Re-exports
pub use config::{CryptoConfig, KmsConfig, DekConfig, DekAlgorithm, CryptoHeaders};
pub use pipeline::{CryptoPipeline, EncryptedRecord};