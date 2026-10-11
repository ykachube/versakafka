//! Breaker composition

use crate::{BreakerError, Result, BreakerState, config::{BreakerConfig, BreakerComposition, LocalBreakerConfig, ArcConfig}};
use std::sync::Arc;
use parking_lot::RwLock;
use super::local::LocalBreaker;
use super::arc::ArcClient;

pub struct BreakerManager {
    config: BreakerConfig,
    local_breakers: Arc<RwLock<HashMap<String, Arc<LocalBreaker>>>>,
    arc_client: Option<Arc<ArcClient>>,
}

use std::collections::HashMap;

impl BreakerManager {
    pub fn new(config: BreakerConfig) -> Result<Self> {
        let arc_client = if config.arc.enabled {
            Some(Arc::new(ArcClient::new(config.arc.clone())))
        } else {
            None
        };

        Ok(Self {
            config,
            local_breakers: Arc::new(RwLock::new(HashMap::new())),
            arc_client,
        })
    }

    pub async fn start(&self) -> Result<()> {
        if let Some(arc) = &self.arc_client {
            arc.start().await?;
        }
        Ok(())
    }

    pub fn get_local_breaker(&self, key: &str) -> Arc<LocalBreaker> {
        let mut breakers = self.local_breakers.write();
        breakers.entry(key.to_string())
            .or_insert_with(|| {
                let config = self.config.per_broker.get(key)
                    .or_else(|| self.config.per_channel.get(key))
                    .cloned()
                    .unwrap_or_else(|| self.config.local.clone());
                Arc::new(LocalBreaker::new(config))
            })
            .clone()
    }

    pub fn is_open(&self, broker: &str, channel: &str) -> bool {
        let local_broker = self.get_local_breaker(broker);
        let local_channel = self.get_local_breaker(channel);
        
        let local_open = local_broker.state() == BreakerState::Open 
            || local_channel.state() == BreakerState::Open;
        
        let regional_open = if let Some(arc) = &self.arc_client {
            arc.any_region_unhealthy()
        } else {
            false
        };

        match self.config.composition {
            BreakerComposition::AnyOpen => local_open || regional_open,
            BreakerComposition::RegionalPriority => regional_open || local_open,
        }
    }

    pub fn get_state(&self, broker: &str, channel: &str) -> BreakerState {
        let local_broker = self.get_local_breaker(broker);
        let local_channel = self.get_local_breaker(channel);
        
        if local_broker.state() == BreakerState::Open 
            || local_channel.state() == BreakerState::Open {
            return BreakerState::Open;
        }
        
        if let Some(arc) = &self.arc_client {
            if arc.any_region_unhealthy() {
                return BreakerState::Open;
            }
        }
        
        BreakerState::Closed
    }
}