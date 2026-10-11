//! Metadata configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataConfig {
    pub routing_mode: RoutingMode,
    pub broker_map: HashMap<String, String>, // SNI -> broker address
    pub rewrite_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RoutingMode {
    Sni,
    PortPerBroker,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            routing_mode: RoutingMode::Sni,
            broker_map: HashMap::new(),
            rewrite_enabled: true,
        }
    }
}