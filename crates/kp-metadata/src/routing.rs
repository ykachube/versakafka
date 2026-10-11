//! Routing logic

use crate::{MetadataError, Result};
use std::collections::HashMap;
use super::config::RoutingMode;

pub struct Router {
    mode: RoutingMode,
    broker_map: HashMap<String, String>,
}

impl Router {
    pub fn new(mode: RoutingMode, broker_map: HashMap<String, String>) -> Self {
        Self { mode, broker_map }
    }

    pub fn route(&self, sni: Option<&str>) -> Result<String> {
        match self.mode {
            RoutingMode::Sni => {
                if let Some(sni) = sni {
                    if let Some(broker) = self.broker_map.get(sni) {
                        return Ok(broker.clone());
                    }
                }
                // Default to first broker
                self.broker_map.values().next().cloned()
                    .ok_or_else(|| MetadataError::BrokerNotFound("No brokers configured".to_string()))
            }
            RoutingMode::PortPerBroker => {
                // TODO: Implement port-per-broker routing
                self.broker_map.values().next().cloned()
                    .ok_or_else(|| MetadataError::BrokerNotFound("No brokers configured".to_string()))
            }
        }
    }
}