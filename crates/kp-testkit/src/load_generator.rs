//! Load generator for testing

use crate::TestKitError;
use crate::config::LoadGeneratorConfig;
use rand::Rng;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::Duration;

pub struct LoadGenerator {
    config: LoadGeneratorConfig,
    stats: Arc<RwLock<LoadStats>>,
}

#[derive(Default)]
struct LoadStats {
    messages_sent: u64,
    messages_acked: u64,
    errors: u64,
    latency_sum_ms: u64,
}

impl LoadGenerator {
    pub fn new(config: LoadGeneratorConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(LoadStats::default())),
        }
    }

    pub async fn run(&self) -> Result<LoadReport, TestKitError> {
        tracing::info!("Starting load generator: {} msg/s for {:?}", 
            self.config.throughput, self.config.duration);
        
        // TODO: Implement actual load generation
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        Ok(LoadReport {
            messages_sent: 100,
            messages_acked: 100,
            errors: 0,
            avg_latency_ms: 1.0,
            p99_latency_ms: 5.0,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LoadReport {
    pub messages_sent: u64,
    pub messages_acked: u64,
    pub errors: u64,
    pub avg_latency_ms: f64,
    pub p99_latency_ms: f64,
}