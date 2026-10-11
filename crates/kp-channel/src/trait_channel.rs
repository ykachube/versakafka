//! Channel trait

use crate::{ChannelError, Result, config::ChannelConfig};
use kp_protocol::{ProtocolError, Result as ProtocolResult};
use async_trait::async_trait;
use std::sync::Arc;
use kp_breaker::BreakerState;

#[async_trait]
pub trait Channel: Send + Sync {
    fn name(&self) -> &str;
    fn channel_type(&self) -> ChannelType;
    fn ack_policy(&self) -> AckPolicy;
    
    async fn send(&self, batch: ChannelBatch) -> Result<SendResult>;
    async fn health_check(&self) -> Result<HealthStatus>;
    fn queue_depth(&self) -> usize;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelType {
    Kafka,
    S3,
    Kinesis,
    Sqs,
    Wal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AckPolicy {
    /// Primary sync, secondary async
    PrimarySyncSecondaryAsync,
    /// First channel to durably ack
    Any,
    /// All channels must ack
    All,
    /// All ack, or failed channel is durably spilled
    AllOrSpill,
}

impl Default for AckPolicy {
    fn default() -> Self {
        Self::PrimarySyncSecondaryAsync
    }
}

#[derive(Debug, Clone)]
pub struct ChannelBatch {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<ChannelRecord>,
}

#[derive(Debug, Clone)]
pub struct ChannelRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: i64,
    pub event_id: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct SendResult {
    pub success: bool,
    pub offset: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub lag: Option<i64>,
    pub breaker_state: BreakerState,
}