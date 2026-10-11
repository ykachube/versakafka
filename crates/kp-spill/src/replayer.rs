//! Spill replayer

use crate::{SpillError, Result, config::SpillConfig};
use kp_channel::Channel;
use object_store::{ObjectStore, path::Path as ObjectPath};
use std::sync::Arc;

pub struct Replayer {
    config: SpillConfig,
    store: Arc<dyn ObjectStore>,
    position: Arc<parking_lot::RwLock<ReplayPosition>>,
}

#[derive(Default)]
struct ReplayPosition {
    manifest_path: Option<String>,
    segment_index: usize,
    record_offset: u64,
}

impl Replayer {
    pub fn new(config: SpillConfig, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config,
            store,
            position: Arc::new(parking_lot::RwLock::new(ReplayPosition::default())),
        }
    }

    pub async fn replay(&self, channel: &dyn Channel) -> Result<()> {
        // TODO: Implement replay logic
        // 1. List manifest files
        // 2. For each manifest, read segments
        // 3. For each segment, read records and send to channel
        // 4. Update position after each successful send
        
        Ok(())
    }

    pub fn get_position(&self) -> (usize, u64) {
        let pos = self.position.read();
        (pos.segment_index, pos.record_offset)
    }

    pub fn set_position(&self, segment_index: usize, record_offset: u64) {
        self.position.write().segment_index = segment_index;
        self.position.write().record_offset = record_offset;
    }
}