//! WAL writer

use crate::{WalError, Result, config::WalConfig};
use super::segment::{Segment, WalRecord};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct WalWriter {
    config: WalConfig,
    current_segment: Arc<RwLock<Option<Segment>>>,
    segment_id: u64,
    base_offset: i64,
    sender: mpsc::Sender<WriteRequest>,
}

#[derive(Debug)]
struct WriteRequest {
    record: WalRecord,
    response: tokio::sync::oneshot::Sender<Result<i64>>,
}

impl WalWriter {
    pub fn new(config: WalConfig) -> Self {
        let (sender, mut receiver) = mpsc::channel::<WriteRequest>(1000);
        let current_segment = Arc::new(RwLock::new(None));
        let current_segment_clone = current_segment.clone();
        let config_clone = config.clone();
        
        tokio::spawn(async move {
            while let Some(req) = receiver.recv().await {
                let mut segment_guard = current_segment_clone.write();
                let segment = segment_guard.get_or_insert_with(|| {
                    Segment::create(config_clone.clone(), &config_clone.dir, 0, 0).unwrap()
                });
                
                let result = segment.append(&req.record);
                let _ = req.response.send(result);
                
                // Check if segment is full
                if segment.is_full() {
                    // Rotate segment
                    *segment_guard = None;
                }
            }
        });
        
        Self {
            config,
            current_segment,
            segment_id: 0,
            base_offset: 0,
            sender,
        }
    }

    pub async fn write(&self, record: WalRecord) -> Result<i64> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(WriteRequest { record, response: tx }).await
            .map_err(|e| WalError::Writer(e.to_string()))?;
        
        rx.await.map_err(|e| WalError::Writer(e.to_string()))?
    }
}