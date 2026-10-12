//! kp-filters - Filter chain and hook traits

pub mod chain;
pub mod hooks;
pub mod builtin;
pub mod config;

pub use chain::{FilterChain, FilterStage, FilterBatch, FilterRecord, FilterHooks};
pub use hooks::{PreReceiveHook, PostReceiveHook, PreSendHook, PostSendHook, SendResult, FilterContext};

use thiserror::Error;
use kp_protocol::ProtocolError;
use kp_eventid::EventIdError;
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
    #[error("wasm error: {0}")]
    Wasm(String),
    #[error("event id error: {0}")]
    EventId(#[from] EventIdError),
}

pub type Result<T> = std::result::Result<T, FilterError>;