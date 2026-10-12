//! Filter hooks

use crate::{FilterError, Result, chain::{FilterBatch, FilterRecord}};
use super::config::TopicFilterConfig;
use kp_eventid::EventIdGenerator;
use std::sync::Arc;
use std::collections::HashMap;
use async_trait::async_trait;

/// Pre-receive hook: called before decoding the request
pub trait PreReceiveHook: Send + Sync {
    fn on_pre_receive(&self, data: &mut [u8]) -> Result<()>;
}

/// Post-receive hook: called after decoding, before event ID assignment
pub trait PostReceiveHook: Send + Sync {
    fn on_post_receive(&self, batch: &mut FilterBatch) -> Result<()>;
}

/// Pre-send hook: called before sending to downstream
pub trait PreSendHook: Send + Sync {
    fn on_pre_send(&self, batch: &mut FilterBatch) -> Result<()>;
}

/// Post-send hook: called after sending to downstream
pub trait PostSendHook: Send + Sync {
    fn on_post_send(&self, batch: &FilterBatch, result: &SendResult) -> Result<()>;
}

/// Result of a send operation
pub struct SendResult {
    pub success: bool,
    pub error: Option<String>,
    pub offset: Option<i64>,
}

/// WASM host trait for dynamic dispatch
#[async_trait]
pub trait WasmHost: Send + Sync {
    async fn call_process_batch(&self, input: &WasmPluginInput) -> Result<WasmPluginOutput>;
}

/// WASM plugin input/output types (local copies to avoid kp_wasm dependency)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WasmPluginInput {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<WasmPluginRecord>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WasmPluginRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WasmPluginOutput {
    pub records: Vec<WasmPluginRecord>,
    pub drop: bool,
}

/// Filter context passed to hooks
pub struct FilterContext {
    pub event_id_generator: Arc<EventIdGenerator>,
    pub wasm_host: Option<Arc<dyn WasmHost>>,
    pub topic_configs: Arc<HashMap<String, TopicFilterConfig>>,
}

impl FilterContext {
    pub fn new(
        event_id_generator: Arc<EventIdGenerator>,
        wasm_host: Option<Arc<dyn WasmHost>>,
        topic_configs: Arc<HashMap<String, TopicFilterConfig>>,
    ) -> Self {
        Self {
            event_id_generator,
            wasm_host,
            topic_configs,
        }
    }
    
    pub fn get_event_id_mode(&self, topic: &str) -> kp_eventid::EventIdMode {
        self.topic_configs.get(topic)
            .and_then(|c| c.event_id_mode)
            .unwrap_or_default()
    }
    
    pub fn is_per_record(&self, topic: &str) -> bool {
        self.topic_configs.get(topic)
            .map(|c| c.per_record_processing)
            .unwrap_or(false)
    }
}

/// Built-in pre-receive hook for request validation
pub struct ValidateRequestHook;

impl PreReceiveHook for ValidateRequestHook {
    fn on_pre_receive(&self, _data: &mut [u8]) -> Result<()> {
        // TODO: Add request validation logic
        Ok(())
    }
}

/// Built-in post-receive hook for event ID assignment
pub struct EventIdAssignmentHook;

impl PostReceiveHook for EventIdAssignmentHook {
    fn on_post_receive(&self, batch: &mut FilterBatch) -> Result<()> {
        // Event ID assignment is handled by the filter chain
        // This hook can add additional headers or metadata
        Ok(())
    }
}

/// Built-in pre-send hook for final modifications
pub struct PreSendEnrichmentHook;

impl PreSendHook for PreSendEnrichmentHook {
    fn on_pre_send(&self, _batch: &mut FilterBatch) -> Result<()> {
        // TODO: Add final enrichment before send
        Ok(())
    }
}