//! Filters configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use kp_eventid::EventIdMode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiltersConfig {
    pub chain: Vec<FilterStageConfig>,
    pub per_topic: HashMap<String, TopicFilterConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterStageConfig {
    pub name: String,
    pub enabled: bool,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFilterConfig {
    pub event_id_mode: Option<EventIdMode>,
    pub per_record_processing: bool,
    pub filters: Vec<String>,
}

impl Default for FiltersConfig {
    fn default() -> Self {
        Self {
            chain: vec![],
            per_topic: HashMap::new(),
        }
    }
}