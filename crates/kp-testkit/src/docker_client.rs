//! Docker client for chaos testing (container kill/restart)

use crate::{TestKitError, Result};
use bollard::{Docker, container::{StartContainerOptions, StopContainerOptions, KillContainerOptions, ListContainersOptions}};
use bollard::models::ContainerStateStatusEnum;
use std::time::Duration;

pub struct DockerClient {
    docker: Docker,
}

impl DockerClient {
    pub fn new() -> Result<Self> {
        let docker = Docker::connect_with_http_defaults()
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(Self { docker })
    }

    pub fn connect_with_env() -> Result<Self> {
        // connect_with_env doesn't exist in bollard 0.16, use HTTP defaults
        Self::new()
    }

    /// Stop a container (simulates crash)
    pub async fn stop_container(&self, name: &str, timeout: Option<Duration>) -> Result<()> {
        let options = StopContainerOptions {
            t: timeout.map(|d| d.as_secs() as i64).unwrap_or(10),
        };
        self.docker.stop_container(name, Some(options)).await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(())
    }

    /// Start a container
    pub async fn start_container(&self, name: &str) -> Result<()> {
        self.docker.start_container(name, None::<StartContainerOptions<String>>).await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(())
    }

    /// Restart a container
    pub async fn restart_container(&self, name: &str, timeout: Option<Duration>) -> Result<()> {
        self.stop_container(name, timeout).await?;
        self.start_container(name).await?;
        Ok(())
    }

    /// Kill a container (SIGKILL)
    pub async fn kill_container(&self, name: &str) -> Result<()> {
        let options = KillContainerOptions {
            signal: "SIGKILL".to_string(),
        };
        self.docker.kill_container(name, Some(options)).await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(())
    }

    /// Get container status
    pub async fn get_container_status(&self, name: &str) -> Result<String> {
        let container = self.docker.inspect_container(name, None).await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        let state = container.state.unwrap_or_default();
        let status = state.status.unwrap_or(ContainerStateStatusEnum::EMPTY);
        Ok(status.to_string())
    }

    /// Wait for container to be running
    pub async fn wait_running(&self, name: &str, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let status = self.get_container_status(name).await?;
            if status == "running" {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(TestKitError::Io(std::io::Error::other(
            format!("Container {} not running after {:?}", name, timeout)
        )))
    }

    /// List containers with filter
    pub async fn list_containers(&self, filter: Option<&str>) -> Result<Vec<String>> {
        let mut filters = std::collections::HashMap::new();
        if let Some(f) = filter {
            filters.insert("name".to_string(), vec![f.to_string()]);
        }
        let containers = self.docker.list_containers(Some(ListContainersOptions {
            all: true,
            filters: filters,
            ..Default::default()
        })).await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(containers.iter()
            .filter_map(|c| c.names.as_ref()?.first().map(|n| n.trim_start_matches('/').to_string()))
            .collect())
    }
}

// Pre-built chaos operations
impl DockerClient {
    /// Kill primary Kafka broker
    pub async fn kill_kafka_primary(&self) -> Result<()> {
        self.kill_container("kp-test-kafka-1").await
    }

    /// Kill secondary Kafka broker
    pub async fn kill_kafka_secondary(&self) -> Result<()> {
        self.kill_container("kp-test-kafka-sec-1").await
    }

    /// Restart proxy container
    pub async fn restart_proxy(&self) -> Result<()> {
        self.restart_container("kp-test-proxy", Some(Duration::from_secs(30))).await
    }

    /// Kill proxy container
    pub async fn kill_proxy(&self) -> Result<()> {
        self.kill_container("kp-test-proxy").await
    }

    /// Restart MinIO
    pub async fn restart_minio(&self) -> Result<()> {
        self.restart_container("kp-test-minio", Some(Duration::from_secs(30))).await
    }

    /// Kill MinIO
    pub async fn kill_minio(&self) -> Result<()> {
        self.kill_container("kp-test-minio").await
    }

    /// Kill LocalStack
    pub async fn kill_localstack(&self) -> Result<()> {
        self.kill_container("kp-test-localstack").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn test_docker_client() {
        let client = DockerClient::new().unwrap();
        let containers = client.list_containers(None).await.unwrap();
        assert!(!containers.is_empty());
    }
}