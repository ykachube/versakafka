//! Channel configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;
use super::AckPolicy;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub name: String,
    pub channel_type: ChannelType,
    pub ack_policy: AckPolicy,
    pub kafka: Option<KafkaChannelConfig>,
    pub breaker: Option<String>, // reference to breaker config key
    pub queue_size: usize,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelType {
    Kafka,
    S3,
    Kinesis,
    Sqs,
    Wal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaChannelConfig {
    pub bootstrap_servers: String,
    pub client_id: String,
    pub producer_config: serde_json::Value,
    pub topic_mapping: std::collections::HashMap<String, String>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            name: "primary".to_string(),
            channel_type: ChannelType::Kafka,
            ack_policy: AckPolicy::default(),
            kafka: None,
            breaker: None,
            queue_size: 1000,
            timeout: Duration::from_secs(10),
        }
    }
}