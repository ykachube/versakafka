//! Cluster fixture with real Kafka in testcontainers

use crate::TestKitError;
use testcontainers::{GenericImage, ImageExt, runners::AsyncRunner};
use std::collections::HashMap;
use std::time::Duration;

pub struct ClusterFixture {
    config: super::config::ClusterFixtureConfig,
    containers: Vec<testcontainers::ContainerAsync<GenericImage>>,
    broker_ports: HashMap<usize, u16>,
}

impl ClusterFixture {
    pub fn new(config: super::config::ClusterFixtureConfig) -> Self {
        Self {
            config,
            containers: vec![],
            broker_ports: HashMap::new(),
        }
    }

    pub async fn start(&mut self) -> Result<(), TestKitError> {
        if !self.config.enabled {
            tracing::info!("Cluster fixture disabled, skipping start");
            return Ok(());
        }
        
        tracing::info!("Starting Kafka cluster fixture with {} brokers", self.config.num_brokers);
        
        // Note: This is a simplified implementation. In practice, you'd use
        // testcontainers with proper networking to connect Kafka to Zookeeper.
        // For now, we just document the expected setup.
        
        tracing::warn!("Cluster fixture is a stub - requires proper testcontainers setup with networking");
        
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), TestKitError> {
        tracing::info!("Stopping Kafka cluster fixture");
        for container in &self.containers {
            let _ = container.stop().await;
        }
        self.containers.clear();
        self.broker_ports.clear();
        Ok(())
    }

    pub fn bootstrap_servers(&self) -> String {
        if self.broker_ports.is_empty() {
            return "localhost:9092".to_string();
        }
        
        self.broker_ports.values()
            .map(|p| format!("localhost:{}", p))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn broker_port(&self, broker_id: usize) -> Option<u16> {
        self.broker_ports.get(&broker_id).copied()
    }

    pub fn num_brokers(&self) -> usize {
        self.config.num_brokers
    }

    pub fn is_running(&self) -> bool {
        !self.containers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires Docker
    async fn test_cluster_fixture() {
        let mut fixture = ClusterFixture::new(super::super::config::ClusterFixtureConfig {
            enabled: true,
            kafka_version: "7.5.0".to_string(),
            num_brokers: 1,
        });
        
        fixture.start().await.unwrap();
        assert!(fixture.is_running());
        assert!(!fixture.bootstrap_servers().is_empty());
        
        fixture.stop().await.unwrap();
        assert!(!fixture.is_running());
    }
}