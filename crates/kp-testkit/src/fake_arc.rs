//! Fake ARC (Application Recovery Controller) for testing

use crate::TestKitError;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

pub struct FakeArc {
    config: super::config::FakeArcConfig,
    state: Arc<RwLock<ArcState>>,
}

#[derive(Default)]
struct ArcState {
    regions: HashMap<String, RegionState>,
}

#[derive(Default, Clone, Debug)]
pub struct RegionState {
    pub healthy: bool,
    pub routing_control: String,
}

impl FakeArc {
    pub fn new(config: super::config::FakeArcConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(ArcState::default())),
        }
    }

    pub async fn start(&self) -> Result<(), TestKitError> {
        tracing::info!("Fake ARC started");
        Ok(())
    }

    pub fn set_region_health(&self, region: &str, healthy: bool) {
        self.state.write().regions.entry(region.to_string()).or_default().healthy = healthy;
    }

    pub fn get_region_health(&self, region: &str) -> bool {
        self.state.read().regions.get(region).map(|r| r.healthy).unwrap_or(true)
    }
}