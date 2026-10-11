//! Breaker configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakerConfig {
    pub local: LocalBreakerConfig,
    pub arc: ArcConfig,
    pub composition: BreakerComposition,
    pub per_broker: HashMap<String, LocalBreakerConfig>,
    pub per_channel: HashMap<String, LocalBreakerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalBreakerConfig {
    pub error_rate_threshold: f64,
    pub latency_threshold: Duration,
    pub timeout_threshold: Duration,
    pub window_size: Duration,
    pub min_requests: usize,
    pub half_open_requests: usize,
    pub half_open_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArcConfig {
    pub enabled: bool,
    pub endpoints: Vec<String>,
    pub poll_interval: Duration,
    pub cache_ttl: Duration,
    pub fail_static: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BreakerComposition {
    AnyOpen,
    RegionalPriority,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            local: LocalBreakerConfig::default(),
            arc: ArcConfig::default(),
            composition: BreakerComposition::AnyOpen,
            per_broker: HashMap::new(),
            per_channel: HashMap::new(),
        }
    }
}

impl Default for LocalBreakerConfig {
    fn default() -> Self {
        Self {
            error_rate_threshold: 0.5,
            latency_threshold: Duration::from_millis(5000),
            timeout_threshold: Duration::from_secs(30),
            window_size: Duration::from_secs(60),
            min_requests: 10,
            half_open_requests: 3,
            half_open_interval: Duration::from_secs(10),
        }
    }
}

impl Default for ArcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoints: vec![],
            poll_interval: Duration::from_secs(10),
            cache_ttl: Duration::from_secs(60),
            fail_static: true,
        }
    }
}