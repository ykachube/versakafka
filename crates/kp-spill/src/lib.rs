//! kp-spill - Spill to object storage (S3/MinIO/GCS/R2) and replayer

pub mod uploader;
pub mod manifest;
pub mod replayer;
pub mod channel;
pub mod config;

use thiserror::Error;
use kp_wal::WalError;
use kp_channel::ChannelError;
use object_store::Error as ObjectStoreError;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum SpillError {
    #[error("uploader error: {0}")]
    Uploader(String),
    #[error("manifest error: {0}")]
    Manifest(String),
    #[error("replayer error: {0}")]
    Replayer(String),
    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, SpillError>;