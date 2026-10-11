//! Event ID configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use super::EventIdMode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventIdConfig {
    pub default_mode: EventIdMode,
    pub per_topic: HashMap<String, EventIdMode>,
    pub hmac_enabled: bool,
    pub hmac_key: Option<String>,
    pub headers: EventIdHeaders,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventIdHeaders {
    pub event_id: bool,
    pub ingest_ts: bool,
    pub proxy_region: bool,
    pub proxy_id: bool,
    pub channel: bool,
}

impl Default for EventIdConfig {
    fn default() -> Self {
        Self {
            default_mode: EventIdMode::Preserve,
            per_topic: HashMap::new(),
            hmac_enabled: false,
            hmac_key: None,
            headers: EventIdHeaders::default(),
        }
    }
}

impl Default for EventIdHeaders {
    fn default() -> Self {
        Self {
            event_id: true,
            ingest_ts: true,
            proxy_region: true,
            proxy_id: true,
            channel: true,
        }
    }
}