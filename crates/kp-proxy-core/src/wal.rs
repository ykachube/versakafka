//! WAL integration for proxy core

use crate::{ProxyCoreError, Result};
use kp_wal::{WalWriter, WalRecord, WalConfig, recover};
use std::sync::Arc;
use std::path::PathBuf;
use tracing::{info, warn};

pub struct WalIntegration {
    writer: Arc<WalWriter>,
    config: WalConfig,
}

impl WalIntegration {
    pub fn new(config: WalConfig) -> Result<Self> {
        let writer = Arc::new(WalWriter::new(config.clone()));
        
        Ok(Self { writer, config })
    }
    
    pub async fn write(&self, record: WalRecord) -> Result<i64> {
        self.writer.write(record).await
            .map_err(|e| ProxyCoreError::Wal(e))
    }
    
    pub async fn flush(&self) -> Result<()> {
        self.writer.flush().await
            .map_err(|e| ProxyCoreError::Wal(e))
    }
    
    pub fn current_offset(&self) -> i64 {
        self.writer.current_offset()
    }
    
    pub fn current_segment_id(&self) -> u64 {
        self.writer.current_segment_id()
    }
    
    pub async fn recover(&self) -> Result<Vec<kp_wal::Segment>> {
        recover(self.config.clone(), self.config.recovery.clone())
            .map_err(|e| ProxyCoreError::Wal(e))
    }
    
    pub fn get_writer(&self) -> Arc<WalWriter> {
        self.writer.clone()
    }
}

impl Default for WalIntegration {
    fn default() -> Self {
        let config = WalConfig::default();
        Self::new(config).expect("Failed to create default WAL integration")
    }
}