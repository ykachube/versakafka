//! Spill as a channel

use crate::{SpillError, Result as SpillResult};
use kp_channel::{Channel, ChannelType, AckPolicy, ChannelBatch, ChannelRecord, SendResult, HealthStatus, Result as ChannelResult, ChannelError};
use kp_breaker::BreakerState;
use kp_wal::{WalWriter, WalRecord, WalConfig};
use async_trait::async_trait;
use std::sync::Arc;
use parking_lot::RwLock;
use std::path::PathBuf;
use tracing::{error, info};

pub struct SpillChannel {
    config: super::config::SpillConfig,
    wal_path: String,
    wal_writer: Arc<WalWriter>,
    state: Arc<RwLock<SpillState>>,
}

struct SpillState {
    healthy: bool,
    breaker_state: BreakerState,
    queue_depth: usize,
    total_written: u64,
    total_failed: u64,
}

impl SpillChannel {
    pub fn new(config: super::config::SpillConfig, wal_path: String) -> Self {
        let wal_config = WalConfig {
            dir: PathBuf::from(&wal_path),
            ..Default::default()
        };
        let wal_writer = Arc::new(WalWriter::new(wal_config));
        
        Self {
            config,
            wal_path,
            wal_writer,
            state: Arc::new(RwLock::new(SpillState {
                healthy: true,
                breaker_state: BreakerState::Closed,
                queue_depth: 0,
                total_written: 0,
                total_failed: 0,
            })),
        }
    }
    
    fn convert_record(&self, record: &ChannelRecord) -> WalRecord {
        WalRecord {
            timestamp: record.timestamp,
            key: record.key.clone(),
            value: record.value.clone(),
            headers: record.headers.clone(),
            event_id: record.event_id.clone(),
            offset: 0, // Will be set by WAL writer
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
        {
            let mut state = self.state.write();
            state.queue_depth += batch.records.len();
        }
        
        let mut all_success = true;
        let mut last_offset = None;
        let mut last_error = None;
        
        for record in &batch.records {
            let wal_record = self.convert_record(record);
            match self.wal_writer.write(wal_record).await {
                Ok(offset) => {
                    last_offset = Some(offset);
                    let mut state = self.state.write();
                    state.total_written += 1;
                }
                Err(e) => {
                    all_success = false;
                    last_error = Some(e.to_string());
                    let mut state = self.state.write();
                    state.total_failed += 1;
                    error!("Failed to write to spill WAL: {}", e);
                }
            }
        }
        
        {
            let mut state = self.state.write();
            state.queue_depth = state.queue_depth.saturating_sub(batch.records.len());
        }
        
        Ok(SendResult {
            success: all_success,
            offset: last_offset,
            error: last_error,
        })
    }
    
    async fn health_check(&self) -> ChannelResult<HealthStatus> {
        let state = self.state.read();
        Ok(HealthStatus {
            healthy: state.healthy && state.breaker_state != BreakerState::Open,
            lag: None,
            breaker_state: state.breaker_state,
        })
    }
    
    fn queue_depth(&self) -> usize {
        self.state.read().queue_depth
    }
}

impl SpillChannel {
    pub fn get_stats(&self) -> (u64, u64) {
        let state = self.state.read();
        (state.total_written, state.total_failed)
    }
    
    pub async fn flush(&self) -> SpillResult<()> {
        self.wal_writer.flush().await
            .map_err(|e| SpillError::Wal(e))
    }
}