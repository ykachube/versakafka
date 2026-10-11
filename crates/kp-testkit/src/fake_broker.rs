//! Fake Kafka broker for testing

use crate::TestKitError;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

pub struct FakeBroker {
    config: super::config::FakeBrokerConfig,
    state: Arc<RwLock<BrokerState>>,
}

#[derive(Default)]
struct BrokerState {
    topics: HashMap<String, TopicState>,
    delay_ms: u64,
    error_rate: f64,
}

#[derive(Default)]
struct TopicState {
    partitions: HashMap<i32, PartitionState>,
}

#[derive(Default)]
struct PartitionState {
    messages: Vec<Vec<u8>>,
    hwm: i64,
}

impl FakeBroker {
    pub fn new(config: super::config::FakeBrokerConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(BrokerState::default())),
        }
    }

    pub async fn start(&self) -> Result<(), TestKitError> {
        tracing::info!("Fake broker started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), TestKitError> {
        tracing::info!("Fake broker stopped");
        Ok(())
    }
}