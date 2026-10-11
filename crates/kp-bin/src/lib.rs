//! kp-bin - Main proxy binary

pub mod config;
pub mod proxy;

use thiserror::Error;
use anyhow::Error as AnyError;
use kp_proxy_core::ProxyCoreError;

#[derive(Debug, Error)]
pub enum BinError {
    #[error("config error: {0}")]
    Config(#[from] AnyError),
    #[error("proxy error: {0}")]
    Proxy(#[from] ProxyCoreError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, BinError>;