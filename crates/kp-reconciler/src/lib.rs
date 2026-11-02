//! kp-reconciler - Channel reconciliation and gap replay

pub mod checker;
pub mod replay;
pub mod config;

use thiserror::Error;
use kp_channel::ChannelError;
use kp_spill::SpillError;
use kp_eventid::EventIdError;
use kp_wal::WalError;
use object_store::Error as ObjectStoreError;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum ReconcilerError {
    #[error("checker error: {0}")]
    Checker(String),
    #[error("replay error: {0}")]
    Replay(String),
    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("spill error: {0}")]
    Spill(#[from] SpillError),
    #[error("event-id error: {0}")]
    EventId(#[from] EventIdError),
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, ReconcilerError>;

// Re-exports
pub use checker::{ReconcilerChecker, ReconciliationReport, Gap, Discrepancy};
pub use replay::GapReplayer;
pub use config::ReconcilerConfig;