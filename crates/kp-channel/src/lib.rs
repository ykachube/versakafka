//! kp-channel - Channel trait, Kafka channel implementation, ack policies

pub mod trait_channel;
pub mod kafka_channel;
pub mod policies;
pub mod fanout;
pub mod config;

use thiserror::Error;
use kp_protocol::ProtocolError;
use kp_breaker::BreakerError;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("channel error: {0}")]
    Channel(String),
    #[error("kafka channel error: {0}")]
    KafkaChannel(String),
    #[error("policy error: {0}")]
    Policy(String),
    #[error("fanout error: {0}")]
    Fanout(String),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("breaker error: {0}")]
    Breaker(#[from] BreakerError),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, ChannelError>;

// Re-export from trait_channel
pub use trait_channel::{Channel, ChannelType, AckPolicy, ChannelBatch, ChannelRecord, SendResult, HealthStatus};
pub use policies::AckPolicyEngine;
pub use fanout::FanoutEngine;
pub use kafka_channel::KafkaChannel;
pub use config::{ChannelConfig, ChannelType as ConfigChannelType, KafkaChannelConfig};