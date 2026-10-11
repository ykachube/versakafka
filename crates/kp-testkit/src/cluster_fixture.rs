//! Cluster fixture with real Kafka in testcontainers

use crate::TestKitError;
use testcontainers::{ContainerAsync, GenericImage, runners::AsyncRunner};

pub struct ClusterFixture {
    config: super::config::ClusterFixtureConfig,
    containers: Vec<ContainerAsync<GenericImage>>,
}

impl ClusterFixture {
    pub fn new(config: super::config::ClusterFixtureConfig) -> Self {
        Self {
            config,
            containers: vec![],
        }
    }

    pub async fn start(&mut self) -> Result<(), TestKitError> {
        tracing::info!("Starting Kafka cluster fixture");
        // TODO: Start testcontainers Kafka cluster
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), TestKitError> {
        tracing::info!("Stopping Kafka cluster fixture");
        for container in &self.containers {
            let _ = container.stop().await;
        }
        Ok(())
    }

    pub fn bootstrap_servers(&self) -> String {
        "localhost:9092".to_string()
    }
}