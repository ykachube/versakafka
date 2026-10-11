//! Filter chain

use crate::{FilterError, Result};
use super::config::{FiltersConfig, FilterStageConfig, TopicFilterConfig};
use kp_eventid::EventIdMode;
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct FilterChain {
    stages: Vec<Box<dyn FilterStage>>,
    topic_configs: Arc<RwLock<HashMap<String, TopicFilterConfig>>>,
}

pub trait FilterStage: Send + Sync {
    fn name(&self) -> &str;
    fn process_batch(&self, batch: &mut FilterBatch) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct FilterBatch {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<FilterRecord>,
    pub event_id_mode: EventIdMode,
    pub per_record: bool,
}

#[derive(Debug, Clone)]
pub struct FilterRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: i64,
    pub offset: i64,
}

impl FilterChain {
    pub fn new(config: FiltersConfig) -> Self {
        let topic_configs = Arc::new(RwLock::new(config.per_topic.clone()));
        let stages = config.chain.into_iter()
            .filter(|s| s.enabled)
            .map(|s| Box::new(FilterStageImpl { name: s.name, config: s.config }) as Box<dyn FilterStage>)
            .collect();
        
        Self { stages, topic_configs }
    }

    pub fn process(&self, mut batch: FilterBatch) -> Result<FilterBatch> {
        // Apply topic-specific config
        if let Some(topic_config) = self.topic_configs.read().get(&batch.topic) {
            if let Some(mode) = topic_config.event_id_mode {
                batch.event_id_mode = mode;
            }
            batch.per_record = topic_config.per_record_processing;
        }
        
        for stage in &self.stages {
            stage.process_batch(&mut batch)?;
        }
        
        Ok(batch)
    }
}

struct FilterStageImpl {
    name: String,
    config: serde_json::Value,
}

impl FilterStage for FilterStageImpl {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn process_batch(&self, _batch: &mut FilterBatch) -> Result<()> {
        Ok(())
    }
}