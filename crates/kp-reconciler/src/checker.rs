//! Reconciler checker

use crate::{ReconcilerError, Result, config::ReconcilerConfig};
use kp_channel::Channel;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

pub struct ReconcilerChecker {
    config: ReconcilerConfig,
}

impl ReconcilerChecker {
    pub fn new(config: ReconcilerConfig) -> Self {
        Self { config }
    }
    
    pub async fn check(&self, channels: &[Arc<dyn Channel>]) -> Result<ReconciliationReport> {
        info!("Starting reconciliation check across {} channels", channels.len());
        
        if channels.len() < 2 {
            return Ok(ReconciliationReport {
                matched: true,
                gaps: Vec::new(),
                discrepancies: Vec::new(),
            });
        }
        
        // For each channel, collect event IDs per topic/partition
        let mut channel_data: Vec<ChannelEventData> = Vec::new();
        
        for channel in channels {
            let health = channel.health_check().await
                .map_err(|e| ReconcilerError::Channel(e))?;
            
            if !health.healthy {
                warn!("Channel {} is unhealthy, skipping", channel.name());
                continue;
            }
            
            // In a real implementation, we'd query the channel for recent events
            // For now, we'll use a placeholder
            let data = ChannelEventData {
                channel_name: channel.name().to_string(),
                events: HashMap::new(),
            };
            channel_data.push(data);
        }
        
        if channel_data.len() < 2 {
            return Ok(ReconciliationReport {
                matched: true,
                gaps: Vec::new(),
                discrepancies: Vec::new(),
            });
        }
        
        // Compare channel data
        let mut gaps = Vec::new();
        let mut discrepancies = Vec::new();
        
        // This is a simplified comparison - in reality we'd:
        // 1. Get recent events from each channel (by topic/partition/offset)
        // 2. Compare event IDs for the same offsets
        // 3. Identify gaps and discrepancies
        
        let mut matched = true;
        
        // Compare first two channels as example
        if channel_data.len() >= 2 {
            let ch1 = &channel_data[0];
            let ch2 = &channel_data[1];
            
            // Check for missing events in channel 2
            for (topic_partition, events1) in &ch1.events {
                if let Some(events2) = ch2.events.get(topic_partition) {
                    // Compare event IDs at same offsets
                    for (offset, event_id1) in events1 {
                        if let Some(event_id2) = events2.get(offset) {
                            if event_id1 != event_id2 {
                                discrepancies.push(Discrepancy {
                                    topic: topic_partition.0.clone(),
                                    partition: topic_partition.1,
                                    offset: *offset,
                                    channel_a_event_id: Some(event_id1.clone()),
                                    channel_b_event_id: Some(event_id2.clone()),
                                });
                                matched = false;
                            }
                        } else {
                            // Gap in channel 2
                            gaps.push(Gap {
                                topic: topic_partition.0.clone(),
                                partition: topic_partition.1,
                                start_offset: *offset,
                                end_offset: *offset,
                            });
                            matched = false;
                        }
                    }
                }
            }
        }
        
        info!("Reconciliation complete: matched={}, gaps={}, discrepancies={}", 
            matched, gaps.len(), discrepancies.len());
        
        Ok(ReconciliationReport {
            matched,
            gaps,
            discrepancies,
        })
    }
}

#[derive(Debug, Clone)]
struct ChannelEventData {
    channel_name: String,
    events: HashMap<(String, i32), HashMap<i64, Vec<u8>>>, // (topic, partition) -> (offset -> event_id)
}

#[derive(Debug, Clone)]
pub struct ReconciliationReport {
    pub matched: bool,
    pub gaps: Vec<Gap>,
    pub discrepancies: Vec<Discrepancy>,
}

#[derive(Debug, Clone)]
pub struct Gap {
    pub topic: String,
    pub partition: i32,
    pub start_offset: i64,
    pub end_offset: i64,
}

#[derive(Debug, Clone)]
pub struct Discrepancy {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub channel_a_event_id: Option<Vec<u8>>,
    pub channel_b_event_id: Option<Vec<u8>>,
}