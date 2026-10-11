//! kp-wal - Local Write-Ahead Log with segments, checksums, recovery

pub mod segment;
pub mod writer;
pub mod reader;
pub mod recovery;
pub mod config;

use thiserror::Error;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("segment error: {0}")]
    Segment(String),
    #[error("writer error: {0}")]
    Writer(String),
    #[error("reader error: {0}")]
    Reader(String),
    #[error("recovery error: {0}")]
    Recovery(String),
    #[error("checksum mismatch: {0}")]
    ChecksumMismatch(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, WalError>;

// Re-export from segment
pub use segment::WalRecord;