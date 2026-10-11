//! kp-filters - Filter chain and hook traits

pub mod chain;
pub mod hooks;
pub mod builtin;
pub mod config;

use thiserror::Error;
use kp_protocol::ProtocolError;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum FilterError {
    #[error("chain error: {0}")]
    Chain(String),
    #[error("hook error: {0}")]
    Hook(String),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, FilterError>;