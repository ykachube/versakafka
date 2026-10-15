//! Breaker integration for proxy core

use crate::{ProxyCoreError, Result};
use kp_breaker::{BreakerManager, BreakerConfig, BreakerState};
use std::sync::Arc;

pub struct BreakerIntegration {
    manager: Arc<BreakerManager>,
}

impl BreakerIntegration {
    pub fn new(config: BreakerConfig) -> Result<Self> {
        let manager = Arc::new(BreakerManager::new(config)
            .map_err(|e| ProxyCoreError::Breaker(e))?);
        
        Ok(Self { manager })
    }

    pub async fn start(&self) -> Result<()> {
        self.manager.start().await
            .map_err(|e| ProxyCoreError::Breaker(e))
    }

    pub fn get_manager(&self) -> Arc<BreakerManager> {
        self.manager.clone()
    }

    pub fn is_open(&self, broker: &str, channel: &str) -> bool {
        self.manager.is_open(broker, channel)
    }

    pub fn get_state(&self, broker: &str, channel: &str) -> BreakerState {
        self.manager.get_state(broker, channel)
    }

    pub fn get_local_breaker(&self, key: &str) -> Arc<kp_breaker::local::LocalBreaker> {
        self.manager.get_local_breaker(key)
    }
}

impl Default for BreakerIntegration {
    fn default() -> Self {
        let config = BreakerConfig::default();
        Self::new(config).expect("Failed to create default breaker integration")
    }
}