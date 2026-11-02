//! WAL writer with group commit

use crate::{WalError, Result, config::WalConfig};
use super::segment::{Segment, WalRecord};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::Duration;
use std::io::Write;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info};

pub struct WalWriter {
    config: WalConfig,
    current_segment: Arc<RwLock<Option<Segment>>>,
    segment_id: Arc<RwLock<u64>>,
    base_offset: Arc<RwLock<i64>>,
    sender: mpsc::Sender<WriteRequest>,
    _rotate_handle: tokio::task::JoinHandle<()>,
    _group_commit_handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug)]
struct WriteRequest {
    record: WalRecord,
    response: tokio::sync::oneshot::Sender<Result<i64>>,
}

impl WalWriter {
    pub fn new(config: WalConfig) -> Self {
        let (sender, receiver) = mpsc::channel::<WriteRequest>(10000);
        let current_segment = Arc::new(RwLock::new(None));
        let segment_id = Arc::new(RwLock::new(0u64));
        let base_offset = Arc::new(RwLock::new(0i64));
        
        let current_segment_writer = current_segment.clone();
        let segment_id_writer = segment_id.clone();
        let base_offset_writer = base_offset.clone();
        let config_writer = config.clone();
        
        // Writer task
        let writer_handle = tokio::spawn(async move {
            Self::writer_task(receiver, current_segment_writer, segment_id_writer, base_offset_writer, config_writer).await;
        });
        
        // Group commit task
        let group_commit_interval = config.group_commit_interval;
        let current_segment_gc = current_segment.clone();
        let config_gc = config.clone();
        
        let group_commit_handle = tokio::spawn(async move {
            let mut interval = interval(group_commit_interval);
            loop {
                interval.tick().await;
                if matches!(config_gc.fsync_policy, super::config::FsyncPolicy::GroupCommit) {
                    if let Some(segment) = &*current_segment_gc.read() {
                        let mut file = segment.file.write();
                        if let Err(e) = (*file).flush() {
                            error!("Group commit flush failed: {}", e);
                        }
                        if let Err(e) = (*file).sync_data() {
                            error!("Group commit sync failed: {}", e);
                        }
                    }
                }
            }
        });
        
        Self {
            config,
            current_segment,
            segment_id,
            base_offset,
            sender,
            _rotate_handle: writer_handle,
            _group_commit_handle: group_commit_handle,
        }
    }
    
    async fn writer_task(
        mut receiver: mpsc::Receiver<WriteRequest>,
        current_segment: Arc<RwLock<Option<Segment>>>,
        segment_id: Arc<RwLock<u64>>,
        base_offset: Arc<RwLock<i64>>,
        config: WalConfig,
    ) {
        while let Some(req) = receiver.recv().await {
            let mut segment_guard = current_segment.write();
            
            // Create segment if needed
            let segment = if segment_guard.is_none() {
                let sid = *segment_id.read();
                let bo = *base_offset.read();
                match Segment::create(config.clone(), &config.dir, sid, bo) {
                    Ok(s) => {
                        *segment_id.write() += 1;
                        *segment_guard = Some(s);
                        segment_guard.as_mut().unwrap()
                    }
                    Err(e) => {
                        let _ = req.response.send(Err(WalError::Writer(e.to_string())));
                        continue;
                    }
                }
            } else {
                segment_guard.as_mut().unwrap()
            };
            
            let result = segment.append(&req.record);
            
            if let Ok(offset) = result {
                *base_offset.write() = offset;
            }
            
            let _ = req.response.send(result);
            
            // Check if segment is full and rotate
            if segment.is_full() {
                info!("WAL segment full, rotating");
                *segment_guard = None;
            }
        }
    }
    
    pub async fn write(&self, record: WalRecord) -> Result<i64> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(WriteRequest { record, response: tx }).await
            .map_err(|e| WalError::Writer(e.to_string()))?;
        
        rx.await.map_err(|e| WalError::Writer(e.to_string()))?
    }
    
    pub async fn flush(&self) -> Result<()> {
        if let Some(segment) = &*self.current_segment.read() {
            let mut file = segment.file.write();
            (*file).flush()?;
            if matches!(self.config.fsync_policy, super::config::FsyncPolicy::Always) {
                (*file).sync_all()?;
            } else {
                (*file).sync_data()?;
            }
        }
        Ok(())
    }
    
    pub fn current_segment_id(&self) -> u64 {
        *self.segment_id.read()
    }
    
    pub fn current_offset(&self) -> i64 {
        *self.base_offset.read()
    }
}