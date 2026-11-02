//! Spill replayer

use crate::{SpillError, Result, config::SpillConfig, manifest::ManifestManager};
use kp_channel::Channel;
use kp_wal::{WalReader, WalRecord};
use object_store::{ObjectStore, path::Path as ObjectPath};
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{info, warn};

pub struct Replayer {
    config: SpillConfig,
    store: Arc<dyn ObjectStore>,
    manifest_manager: ManifestManager,
    position: Arc<RwLock<ReplayPosition>>,
}

#[derive(Default, Clone)]
struct ReplayPosition {
    manifest_path: Option<String>,
    segment_index: usize,
    record_offset: u64,
}

impl Replayer {
    pub fn new(config: SpillConfig, store: Arc<dyn ObjectStore>) -> Self {
        let manifest_manager = ManifestManager::new(config.clone());
        Self {
            config,
            store,
            manifest_manager,
            position: Arc::new(RwLock::new(ReplayPosition::default())),
        }
    }
    
    pub async fn replay(&self, channel: &dyn Channel) -> Result<()> {
        info!("Starting spill replay");
        
        // List manifest files
        let manifests = self.list_manifests().await?;
        if manifests.is_empty() {
            info!("No manifests found to replay");
            return Ok(());
        }
        
        for manifest_path in manifests {
            info!("Replaying manifest: {}", manifest_path);
            let manifest = self.download_manifest(&manifest_path).await?;
            
            // Replay each segment
            for (segment_index, segment) in manifest.segments.iter().enumerate() {
                info!("Replaying segment: {} ({} records)", segment.path, segment.record_count);
                
                let records = self.download_segment(segment).await?;
                
                for record in records {
                    let channel_batch = self.record_to_batch(&segment.path, record);
                    let result = channel.send(channel_batch).await
                        .map_err(|e| SpillError::Channel(e))?;
                    
                    if !result.success {
                        warn!("Failed to replay record: {:?}", result.error);
                    }
                    
                    // Update position
                    self.update_position(segment_index, 0).await;
                }
            }
        }
        
        info!("Spill replay completed");
        Ok(())
    }
    
    async fn list_manifests(&self) -> Result<Vec<String>> {
        let prefix = ObjectPath::from(self.config.prefix.clone());
        let mut manifests = Vec::new();
        
        let mut stream = self.store.list(Some(&prefix));
        while let Some(result) = futures::stream::StreamExt::next(&mut stream).await {
            match result {
                Ok(meta) => {
                    if meta.location.to_string().ends_with(".json") && meta.location.to_string().contains("manifest") {
                        manifests.push(meta.location.to_string());
                    }
                }
                Err(e) => return Err(SpillError::ObjectStore(e)),
            }
        }
        
        manifests.sort();
        Ok(manifests)
    }
    
    async fn download_manifest(&self, path: &str) -> Result<crate::manifest::Manifest> {
        let object_path = ObjectPath::from(path);
        let result = self.store.get(&object_path).await
            .map_err(|e| SpillError::ObjectStore(e))?;
        
        let data = result.bytes().await
            .map_err(|e| SpillError::ObjectStore(e))?;
        
        let manifest: crate::manifest::Manifest = serde_json::from_slice(&data)?;
        Ok(manifest)
    }
    
    async fn download_segment(&self, segment: &crate::manifest::ManifestSegment) -> Result<Vec<WalRecord>> {
        let object_path = ObjectPath::from(segment.path.clone());
        let result = self.store.get(&object_path).await
            .map_err(|e| SpillError::ObjectStore(e))?;
        
        let data = result.bytes().await
            .map_err(|e| SpillError::ObjectStore(e))?;
        
        // Write to temp file and read with WalReader
        let temp_path = std::env::temp_dir().join(format!("spill_replay_{}", segment.path.replace('/', "_")));
        std::fs::write(&temp_path, &data)
            .map_err(|e| SpillError::Io(e))?;
        
        let mut reader = WalReader::open(kp_wal::config::WalConfig::default(), &temp_path)?;
        let records = reader.read_all()?;
        
        // Clean up
        let _ = std::fs::remove_file(&temp_path);
        
        Ok(records)
    }
    
    fn record_to_batch(&self, topic: &str, record: WalRecord) -> kp_channel::ChannelBatch {
        kp_channel::ChannelBatch {
            topic: topic.to_string(),
            partition: 0,
            records: vec![kp_channel::ChannelRecord {
                key: record.key,
                value: record.value,
                headers: record.headers,
                timestamp: record.timestamp,
                event_id: record.event_id,
            }],
        }
    }
    
    async fn update_position(&self, segment_index: usize, record_offset: u64) {
        let mut pos = self.position.write();
        pos.segment_index = segment_index;
        pos.record_offset = record_offset;
    }
    
    pub fn get_position(&self) -> (usize, u64) {
        let pos = self.position.read();
        (pos.segment_index, pos.record_offset)
    }
    
    pub fn set_position(&self, segment_index: usize, record_offset: u64) {
        let mut pos = self.position.write();
        pos.segment_index = segment_index;
        pos.record_offset = record_offset;
    }
}