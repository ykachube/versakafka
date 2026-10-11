//! Testkit configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestKitConfig {
    pub fake_broker: FakeBrokerConfig,
    pub cluster_fixture: ClusterFixtureConfig,
    pub fake_arc: FakeArcConfig,
    pub fake_s3: FakeS3Config,
    pub load_generator: LoadGeneratorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FakeBrokerConfig {
    pub enabled: bool,
    pub delay_ms: u64,
    pub error_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterFixtureConfig {
    pub enabled: bool,
    pub kafka_version: String,
    pub num_brokers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FakeArcConfig {
    pub enabled: bool,
    pub scenarios: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FakeS3Config {
    pub enabled: bool,
    pub error_rate: f64,
    pub delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadGeneratorConfig {
    pub throughput: u64,
    pub batch_size: usize,
    pub duration: Duration,
}

impl Default for TestKitConfig {
    fn default() -> Self {
        Self {
            fake_broker: FakeBrokerConfig::default(),
            cluster_fixture: ClusterFixtureConfig::default(),
            fake_arc: FakeArcConfig::default(),
            fake_s3: FakeS3Config::default(),
            load_generator: LoadGeneratorConfig::default(),
        }
    }
}

impl Default for FakeBrokerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            delay_ms: 0,
            error_rate: 0.0,
        }
    }
}

impl Default for ClusterFixtureConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            kafka_version: "3.6".to_string(),
            num_brokers: 1,
        }
    }
}

impl Default for FakeArcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            scenarios: vec![],
        }
    }
}

impl Default for FakeS3Config {
    fn default() -> Self {
        Self {
            enabled: false,
            error_rate: 0.0,
            delay_ms: 0,
        }
    }
}

impl Default for LoadGeneratorConfig {
    fn default() -> Self {
        Self {
            throughput: 10000,
            batch_size: 100,
            duration: Duration::from_secs(60),
        }
    }
}