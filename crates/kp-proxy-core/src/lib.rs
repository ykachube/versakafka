//! kp-proxy-core - Core proxy functionality: connection handling, TLS, framing, backpressure

pub mod connection;
pub mod tls;
pub mod framing;
pub mod backpressure;
pub mod drain;
pub mod server;
pub mod config;

use thiserror::Error;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum ProxyCoreError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("framing error: {0}")]
    Framing(String),
    #[error("backpressure: {0}")]
    Backpressure(String),
    #[error("drain error: {0}")]
    Drain(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, ProxyCoreError>;