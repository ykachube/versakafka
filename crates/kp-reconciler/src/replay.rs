//! Gap replay

use crate::{ReconcilerError, Result, config::ReconcilerConfig, checker::Gap};
use kp_spill::replayer::Replayer;
use kp_channel::Channel;
use kp_wal::{WalReader, WalRecord, WalConfig};
use object_store::{ObjectStore, path::Path as ObjectPath};
use std::sync::Arc;
use std::path::PathBuf;
use tracing::{debug, info, warn};

pub struct GapReplayer {
    config: ReconcilerConfig,
}

impl GapReplayer {
    pub fn new(config: ReconcilerConfig) -> Self {
        Self { config }
    }
    
    pub async fn replay_gaps(&self, gaps: &[Gap], replayer: &Replayer, channel: &dyn Channel) -> Result<()> {
        if gaps.is_empty() {
            info!("No gaps to replay");
            return Ok(());
        }
        
        info!("Replaying {} gaps to channel {}", gaps.len(), channel.name());
        
        for gap in gaps {
            info!("Replaying gap: topic={}, partition={}, offsets={}-{}", 
                gap.topic, gap.partition, gap.start_offset, gap.end_offset);
            
            // Determine source for replay (WAL or S3 spill)
            // For now, we'll use the replayer which reads from S3
            // In a full implementation, we'd check WAL first, then spill
            
            let result = self.replay_gap(gap, replayer, channel).await;
            
            if let Err(e) = result {
                warn!("Failed to replay gap: {}", e);
                // Continue with other gaps
            }
        }
        
        Ok(())
    }
    
    async fn replay_gap(&self, gap: &Gap, replayer: &Replayer, channel: &dyn Channel) -> Result<()> {
        // Get records from replayer (which reads from S3 spill)
        // The replayer replays entire segments, so we need to filter for our gap
        
        // For now, we'll replay all and filter
        // In a production system, we'd have an index to seek directly to the gap
        
        let position = replayer.get_position();
        info!("Replayer position: segment={}, offset={}", position.0, position.1);
        
        // Replay from spill
        replayer.replay(channel).await
            .map_err(|e| ReconcilerError::Replay(e.to_string()))?;
        
        Ok(())
    }
    
    /// Replay from local WAL
    pub async fn replay_from_wal(&self, gap: &Gap, wal_dir: &PathBuf, channel: &dyn Channel) -> Result<()> {
        // Find WAL segments that might contain the gap
        let entries = std::fs::read_dir(wal_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|ext| ext == "wal").unwrap_or(false))
            .collect::<Vec<_>>();
        
        for entry in entries {
            let mut reader = WalReader::open(WalConfig::default(), &entry.path())?;
            let records = reader.read_from_offset(gap.start_offset)?;
            
            for record in records {
                if record.offset > gap.end_offset {
                    break;
                }
                
                let channel_batch = kp_channel::ChannelBatch {
                    topic: gap.topic.clone(),
                    partition: gap.partition,
                    records: vec![kp_channel::ChannelRecord {
                        key: record.key,
                        value: record.value,
                        headers: record.headers,
                        timestamp: record.timestamp,
                        event_id: record.event_id,
                    }],
                };
                
                let result = channel.send(channel_batch).await
                    .map_err(|e| ReconcilerError::Channel(e))?;
                
                if !result.success {
                    warn!("Failed to replay record at offset {}: {:?}", record.offset, result.error);
                }
            }
        }
        
        Ok(())
    }
}