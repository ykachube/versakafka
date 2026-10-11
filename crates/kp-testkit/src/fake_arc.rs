//! Fake ARC (Application Recovery Controller) for testing

use crate::TestKitError;
use crate::config::FakeArcConfig;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub struct FakeArc {
    config: FakeArcConfig,
    state: Arc<RwLock<ArcState>>,
}

#[derive(Default)]
struct ArcState {
    regions: HashMap<String, RegionState>,
    scenarios: Vec<ArcScenario>,
    current_scenario: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegionState {
    pub healthy: bool,
    pub routing_control: String,
    #[serde(skip, default = "default_system_time")]
    pub last_updated: std::time::SystemTime,
}

fn default_system_time() -> std::time::SystemTime {
    std::time::SystemTime::now()
}

impl Default for RegionState {
    fn default() -> Self {
        Self {
            healthy: true,
            routing_control: String::new(),
            last_updated: std::time::SystemTime::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArcScenario {
    pub name: String,
    pub steps: Vec<ArcScenarioStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArcScenarioStep {
    pub delay: Duration,
    pub actions: Vec<ArcAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArcAction {
    SetRegionHealth { region: String, healthy: bool },
    SetAllRegionsHealth { healthy: bool },
    FlapRegion { region: String, interval: Duration, count: usize },
    Unavailable { duration: Duration },
}

impl FakeArc {
    pub fn new(config: FakeArcConfig) -> Self {
        let arc = Self {
            config: config.clone(),
            state: Arc::new(RwLock::new(ArcState::default())),
        };
        
        // Initialize default regions
        let default_regions = vec!["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1"];
        for region in default_regions {
            arc.state.write().regions.insert(region.to_string(), RegionState {
                healthy: true,
                routing_control: format!("{}-routing-control", region),
                last_updated: std::time::SystemTime::now(),
            });
        }
        
        arc
    }

    pub async fn start(&self) -> Result<(), TestKitError> {
        tracing::info!("Fake ARC started");
        
        // Start scenario runner if scenarios configured
        if !self.config.scenarios.is_empty() {
            self.run_scenarios().await;
        }
        
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), TestKitError> {
        tracing::info!("Fake ARC stopped");
        Ok(())
    }

    pub fn set_region_health(&self, region: &str, healthy: bool) {
        self.state.write().regions.entry(region.to_string()).or_default().healthy = healthy;
    }

    pub fn get_region_health(&self, region: &str) -> bool {
        self.state.read().regions.get(region).map(|r| r.healthy).unwrap_or(true)
    }

    pub fn get_all_regions(&self) -> Vec<RegionState> {
        self.state.read().regions.values().cloned().collect()
    }

    pub fn set_all_regions_health(&self, healthy: bool) {
        let mut state = self.state.write();
        for region in state.regions.values_mut() {
            region.healthy = healthy;
        }
    }

    pub fn get_routing_control(&self, region: &str) -> Option<String> {
        self.state.read().regions.get(region).map(|r| r.routing_control.clone())
    }

    /// Load scenarios from config
    pub fn load_scenarios(&self, scenarios: Vec<ArcScenario>) {
        self.state.write().scenarios = scenarios;
    }

    /// Run scenarios in background
    async fn run_scenarios(&self) {
        let state = self.state.clone();
        let scenarios = self.config.scenarios.clone();
        
        tokio::spawn(async move {
            for (idx, scenario_name) in scenarios.iter().enumerate() {
                // In real implementation, load scenario by name
                tracing::info!("Running ARC scenario: {} ({}/{})", scenario_name, idx + 1, scenarios.len());
                
                // For now, just simulate some flap
                if scenario_name == "flap" {
                    let regions: Vec<String> = state.read().regions.keys().cloned().collect();
                    for region in regions {
                        for _ in 0..3 {
                            state.write().regions.entry(region.clone()).or_default().healthy = false;
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            state.write().regions.entry(region.clone()).or_default().healthy = true;
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        });
    }

    /// Simulate ARC flapping for testing
    pub fn simulate_flap(&self, region: &str, interval: Duration, count: usize) {
        let state = self.state.clone();
        let region = region.to_string();
        
        tokio::spawn(async move {
            for _ in 0..count {
                state.write().regions.entry(region.clone()).or_default().healthy = false;
                tokio::time::sleep(interval).await;
                state.write().regions.entry(region.clone()).or_default().healthy = true;
                tokio::time::sleep(interval).await;
            }
        });
    }

    /// Make ARC unavailable (simulating total failure)
    pub fn set_unavailable(&self, duration: Duration) {
        let state = self.state.clone();
        
        tokio::spawn(async move {
            let regions: Vec<String> = state.read().regions.keys().cloned().collect();
            for region in &regions {
                state.write().regions.entry(region.clone()).or_default().healthy = false;
            }
            tokio::time::sleep(duration).await;
            for region in &regions {
                state.write().regions.entry(region.clone()).or_default().healthy = true;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fake_arc() {
        let config = FakeArcConfig::default();
        let arc = FakeArc::new(config);
        
        // Test default health
        assert!(arc.get_region_health("us-east-1"));
        
        // Test setting health
        arc.set_region_health("us-east-1", false);
        assert!(!arc.get_region_health("us-east-1"));
        
        // Test all regions
        arc.set_all_regions_health(false);
        assert!(!arc.get_region_health("us-east-1"));
        assert!(!arc.get_region_health("us-west-2"));
        
        // Test routing control
        assert_eq!(arc.get_routing_control("us-east-1"), Some("us-east-1-routing-control".to_string()));
    }
}