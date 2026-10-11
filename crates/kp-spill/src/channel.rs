//! Spill as a channel

use crate::{SpillError, Result as SpillResult};
use kp_channel::{Channel, ChannelType, AckPolicy, ChannelBatch, ChannelRecord, SendResult, HealthStatus, Result as ChannelResult, ChannelError};
use kp_breaker::BreakerState;
use async_trait::async_trait;
use std::sync::Arc;
use parking_lot::RwLock;

pub struct SpillChannel {
    config: super::config::SpillConfig,
    wal_path: String,
    state: Arc<RwLock<SpillState>>,
}

struct SpillState {
    healthy: bool,
    breaker_state: BreakerState,
    queue_depth: usize,
}

impl SpillChannel {
    pub fn new(config: super::config::SpillConfig, wal_path: String) -> Self {
        Self {
            config,
            wal_path,
            state: Arc::new(RwLock::new(SpillState {
                healthy: true,
                breaker_state: BreakerState::Closed,
                queue_depth: 0,
            })),
        }
    }
}

#[async_trait]
impl Channel for SpillChannel {
    fn name(&self) -> &str {
        "spill"
    }
    
    fn channel_type(&self) -> ChannelType {
        ChannelType::Wal
    }
    
    fn ack_policy(&self) -> AckPolicy {
        AckPolicy::AllOrSpill
    }
    
    async fn send(&self, batch: ChannelBatch) -> ChannelResult<SendResult> {
        // TODO: Write to WAL
        Ok(SendResult {
            success: true,
            offset: None,
            error: None,
        })
    }
    
    async fn health_check(&self) -> ChannelResult<HealthStatus> {
        let state = self.state.read();
        Ok(HealthStatus {
            healthy: state.healthy,
            lag: None,
            breaker_state: state.breaker_state,
        })
    }
    
    fn queue_depth(&self) -> usize {
        self.state.read().queue_depth
    }
}