//! kp-proxy-core - Core proxy functionality: connection handling, TLS, framing, backpressure

pub mod connection;
pub mod tls;
pub mod framing;
pub mod backpressure;
pub mod drain;
pub mod server;
pub mod config;
pub mod produce;
pub mod breaker;

use thiserror::Error;
use anyhow::Error as AnyError;
use kp_protocol::ProtocolError;
use kp_metadata::MetadataError;
use kp_breaker::BreakerError;
use kp_channel::ChannelError;
use kp_filters::FilterError;

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
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("metadata error: {0}")]
    Metadata(#[from] MetadataError),
    #[error("breaker error: {0}")]
    Breaker(#[from] BreakerError),
    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("filter error: {0}")]
    Filter(#[from] FilterError),
}

pub type Result<T> = std::result::Result<T, ProxyCoreError>;