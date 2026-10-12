//! Filter chain

use crate::{FilterError, Result, hooks::{FilterContext, SendResult}, builtin::{EnrichmentFilter, FilteringFilter}};
use super::config::{FiltersConfig, FilterStageConfig, TopicFilterConfig};
use kp_eventid::{EventIdGenerator, EventIdMode, EventIdConfig};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct FilterChain {
    stages: Vec<Box<dyn FilterStage>>,
    hooks: FilterHooks,
    topic_configs_map: Arc<RwLock<HashMap<String, TopicFilterConfig>>>,
    topic_configs: Arc<HashMap<String, TopicFilterConfig>>,
    context: FilterContext,
}

pub struct FilterHooks {
    pub pre_receive: Vec<Box<dyn crate::hooks::PreReceiveHook>>,
    pub post_receive: Vec<Box<dyn crate::hooks::PostReceiveHook>>,
    pub pre_send: Vec<Box<dyn crate::hooks::PreSendHook>>,
    pub post_send: Vec<Box<dyn crate::hooks::PostSendHook>>,
}

impl FilterHooks {
    pub fn new() -> Self {
        Self {
            pre_receive: vec![],
            post_receive: vec![],
            pre_send: vec![],
            post_send: vec![],
        }
    }
    
    pub fn add_pre_receive(&mut self, hook: Box<dyn crate::hooks::PreReceiveHook>) {
        self.pre_receive.push(hook);
    }
    
    pub fn add_post_receive(&mut self, hook: Box<dyn crate::hooks::PostReceiveHook>) {
        self.post_receive.push(hook);
    }
    
    pub fn add_pre_send(&mut self, hook: Box<dyn crate::hooks::PreSendHook>) {
        self.pre_send.push(hook);
    }
    
    pub fn add_post_send(&mut self, hook: Box<dyn crate::hooks::PostSendHook>) {
        self.post_send.push(hook);
    }
}

pub trait FilterStage: Send + Sync {
    fn name(&self) -> &str;
    fn process_batch(&self, batch: &mut FilterBatch, context: &FilterContext) -> Result<()>;
    fn process_record(&self, record: &mut FilterRecord, context: &FilterContext) -> Result<()> {
        // Default implementation does nothing
        let _ = record;
        let _ = context;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FilterBatch {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<FilterRecord>,
    pub event_id_mode: EventIdMode,
    pub per_record: bool,
    pub producer_id: Option<i64>,
    pub producer_epoch: Option<i16>,
    pub base_sequence: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct FilterRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: i64,
    pub offset: i64,
    pub event_id: Option<Vec<u8>>,
}

impl FilterChain {
    pub fn new(
        config: FiltersConfig,
        event_id_config: EventIdConfig,
        wasm_host: Option<Arc<dyn crate::hooks::WasmHost>>,
    ) -> Self {
        let topic_configs_map = Arc::new(RwLock::new(config.per_topic.clone()));
        let topic_configs = Arc::new(HashMap::new());
        let event_id_generator = Arc::new(EventIdGenerator::new(event_id_config));
        
        let context = FilterContext::new(
            event_id_generator.clone(),
            wasm_host,
            topic_configs.clone(),
        );
        
        let stages = config.chain.into_iter()
            .filter(|s| s.enabled)
            .map(|s| create_stage(s))
            .collect();
        
        let hooks = FilterHooks::new();
        
        Self {
            stages,
            hooks,
            topic_configs_map,
            topic_configs,
            context,
        }
    }
    
    pub fn context(&self) -> &FilterContext {
        &self.context
    }
    
    pub fn hooks(&mut self) -> &mut FilterHooks {
        &mut self.hooks
    }
    
    /// Process a batch through the filter chain
    pub fn process_batch(&self, mut batch: FilterBatch) -> Result<FilterBatch> {
        // Apply topic-specific config
        if let Some(topic_config) = self.topic_configs_map.read().get(&batch.topic) {
            if let Some(mode) = topic_config.event_id_mode {
                batch.event_id_mode = mode;
            }
            batch.per_record = topic_config.per_record_processing;
        }
        
        // Run pre-receive hooks
        for hook in &self.hooks.pre_receive {
            // Pre-receive hooks work on raw bytes, not applicable here
        }
        
        // Process through stages
        for stage in &self.stages {
            stage.process_batch(&mut batch, &self.context)?;
        }
        
        // Run post-receive hooks
        for hook in &self.hooks.post_receive {
            hook.on_post_receive(&mut batch)?;
        }
        
        // Assign event IDs
        self.assign_event_ids(&mut batch)?;
        
        // Run per-record processing if enabled
        if batch.per_record {
            self.process_per_record(&mut batch)?;
        }
        
        Ok(batch)
    }
    
    /// Process records individually (for per-record filters)
    fn process_per_record(&self, batch: &mut FilterBatch) -> Result<()> {
        for record in &mut batch.records {
            for stage in &self.stages {
                stage.process_record(record, &self.context)?;
            }
        }
        Ok(())
    }
    
    /// Assign event IDs to records
    fn assign_event_ids(&self, batch: &mut FilterBatch) -> Result<()> {
        for record in &mut batch.records {
            let event_id = self.context.event_id_generator.generate(
                &batch.topic,
                batch.partition,
                &kp_eventid::modes::EventRecord {
                    key: record.key.clone(),
                    value: record.value.clone(),
                    headers: record.headers.clone(),
                    timestamp: record.timestamp,
                }
            )?;
            record.event_id = Some(event_id);
        }
        Ok(())
    }
    
    /// Run pre-send hooks
    pub fn pre_send(&self, batch: &mut FilterBatch) -> Result<()> {
        for hook in &self.hooks.pre_send {
            hook.on_pre_send(batch)?;
        }
        Ok(())
    }
    
    /// Run post-send hooks
    pub fn post_send(&self, batch: &FilterBatch, result: &SendResult) -> Result<()> {
        for hook in &self.hooks.post_send {
            hook.on_post_send(batch, result)?;
        }
        Ok(())
    }
    
    /// Reload topic configurations
    pub fn reload_topic_configs(&self, new_configs: HashMap<String, TopicFilterConfig>) {
        *self.topic_configs_map.write() = new_configs;
    }
}

fn create_stage(config: FilterStageConfig) -> Box<dyn FilterStage> {
    match config.name.as_str() {
        "enrichment" => Box::new(EnrichmentFilter::from_config(config.config)),
        "filtering" => Box::new(FilteringFilter::from_config(config.config)),
        "event-id" => Box::new(EventIdFilter),
        _ => Box::new(CustomFilterStage { name: config.name, config: config.config }),
    }
}

struct CustomFilterStage {
    name: String,
    config: serde_json::Value,
}

impl FilterStage for CustomFilterStage {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn process_batch(&self, _batch: &mut FilterBatch, _context: &FilterContext) -> Result<()> {
        Ok(())
    }
}

/// Event ID assignment filter stage
struct EventIdFilter;

impl FilterStage for EventIdFilter {
    fn name(&self) -> &str {
        "event-id"
    }
    
    fn process_batch(&self, batch: &mut FilterBatch, context: &FilterContext) -> Result<()> {
        // Event IDs are assigned by the chain, this stage just ensures they're present
        for record in &mut batch.records {
            if record.event_id.is_none() {
                let event_id = context.event_id_generator.generate(
                    &batch.topic,
                    batch.partition,
                    &kp_eventid::modes::EventRecord {
                        key: record.key.clone(),
                        value: record.value.clone(),
                        headers: record.headers.clone(),
                        timestamp: record.timestamp,
                    }
                )?;
                record.event_id = Some(event_id);
            }
        }
        Ok(())
    }
}