//! Toxiproxy client for chaos testing

use crate::{TestKitError, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Toxic {
    pub name: String,
    #[serde(rename = "type")]
    pub toxic_type: String,
    pub stream: String,
    pub attributes: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proxy {
    pub name: String,
    pub upstream: String,
    pub listen: String,
    pub enabled: bool,
    pub toxics: Vec<Toxic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToxiproxyScenario {
    pub name: String,
    pub upstream: String,
    pub toxics: Vec<Toxic>,
}

pub struct ToxiproxyClient {
    client: Client,
    base_url: String,
}

impl ToxiproxyClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Get all proxies
    pub async fn get_proxies(&self) -> Result<Vec<Proxy>> {
        let url = format!("{}/proxies", self.base_url);
        let resp = self.client.get(&url).send().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        let proxies: Vec<Proxy> = resp.json().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(proxies)
    }

    /// Get a specific proxy
    pub async fn get_proxy(&self, name: &str) -> Result<Proxy> {
        let url = format!("{}/proxies/{}", self.base_url, name);
        let resp = self.client.get(&url).send().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        let proxy: Proxy = resp.json().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(proxy)
    }

    /// Create a new proxy
    pub async fn create_proxy(&self, name: &str, listen: &str, upstream: &str) -> Result<Proxy> {
        let url = format!("{}/proxies", self.base_url);
        let body = serde_json::json!({
            "name": name,
            "listen": listen,
            "upstream": upstream,
            "enabled": true
        });
        let resp = self.client.post(&url).json(&body).send().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        let proxy: Proxy = resp.json().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(proxy)
    }

    /// Delete a proxy
    pub async fn delete_proxy(&self, name: &str) -> Result<()> {
        let url = format!("{}/proxies/{}", self.base_url, name);
        let resp = self.client.delete(&url).send().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        if !resp.status().is_success() {
            return Err(TestKitError::Io(std::io::Error::other(
                format!("Failed to delete proxy: {}", resp.status())
            )));
        }
        Ok(())
    }

    /// Add a toxic to a proxy
    pub async fn add_toxic(&self, proxy_name: &str, toxic: &Toxic) -> Result<Toxic> {
        let url = format!("{}/proxies/{}/toxics", self.base_url, proxy_name);
        let resp = self.client.post(&url).json(toxic).send().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        let toxic: Toxic = resp.json().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        Ok(toxic)
    }

    /// Remove a toxic from a proxy
    pub async fn remove_toxic(&self, proxy_name: &str, toxic_name: &str) -> Result<()> {
        let url = format!("{}/proxies/{}/toxics/{}", self.base_url, proxy_name, toxic_name);
        let resp = self.client.delete(&url).send().await
            .map_err(|e| TestKitError::Io(std::io::Error::other(e)))?;
        if !resp.status().is_success() {
            return Err(TestKitError::Io(std::io::Error::other(
                format!("Failed to remove toxic: {}", resp.status())
            )));
        }
        Ok(())
    }

    /// Remove all toxics from a proxy
    pub async fn remove_all_toxics(&self, proxy_name: &str) -> Result<()> {
        let proxy = self.get_proxy(proxy_name).await?;
        for toxic in &proxy.toxics {
            self.remove_toxic(proxy_name, &toxic.name).await?;
        }
        Ok(())
    }

    /// Apply a scenario from JSON file
    pub async fn apply_scenario(&self, scenario: &ToxiproxyScenario) -> Result<()> {
        // Ensure proxy exists
        let proxy_exists = self.get_proxy(&scenario.name).await.is_ok();
        if !proxy_exists {
            // Try to create if it doesn't exist
            let listen = format!("0.0.0.0:{}", self.infer_port(&scenario.upstream));
            self.create_proxy(&scenario.name, &listen, &scenario.upstream).await?;
        }

        // Remove existing toxics
        self.remove_all_toxics(&scenario.name).await?;

        // Add new toxics
        for toxic in &scenario.toxics {
            self.add_toxic(&scenario.name, toxic).await?;
        }
        Ok(())
    }

    /// Reset all proxies (remove all toxics)
    pub async fn reset_all(&self) -> Result<()> {
        let proxies = self.get_proxies().await?;
        for proxy in proxies {
            self.remove_all_toxics(&proxy.name).await?;
        }
        Ok(())
    }

    fn infer_port(&self, upstream: &str) -> u16 {
        upstream.split(':').last()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8666)
    }

    /// Pre-built chaos scenarios
    pub async fn apply_kafka_latency(&self, latency_ms: u64, jitter_ms: u64) -> Result<()> {
        let toxic = Toxic {
            name: "latency".to_string(),
            toxic_type: "latency".to_string(),
            stream: "downstream".to_string(),
            attributes: serde_json::json!({
                "latency": latency_ms,
                "jitter": jitter_ms
            }),
        };
        self.add_toxic("kafka-primary", &toxic).await?;
        Ok(())
    }

    pub async fn apply_kafka_timeout(&self, timeout_ms: u64) -> Result<()> {
        let toxic = Toxic {
            name: "timeout".to_string(),
            toxic_type: "timeout".to_string(),
            stream: "downstream".to_string(),
            attributes: serde_json::json!({
                "timeout": timeout_ms
            }),
        };
        self.add_toxic("kafka-primary", &toxic).await?;
        Ok(())
    }

    pub async fn apply_kafka_bandwidth(&self, rate_kbps: u64) -> Result<()> {
        let toxic = Toxic {
            name: "bandwidth".to_string(),
            toxic_type: "bandwidth".to_string(),
            stream: "downstream".to_string(),
            attributes: serde_json::json!({
                "rate": rate_kbps
            }),
        };
        self.add_toxic("kafka-primary", &toxic).await?;
        Ok(())
    }

    pub async fn apply_kafka_slow_close(&self, delay_ms: u64) -> Result<()> {
        let toxic = Toxic {
            name: "slow_close".to_string(),
            toxic_type: "slow_close".to_string(),
            stream: "downstream".to_string(),
            attributes: serde_json::json!({
                "delay": delay_ms
            }),
        };
        self.add_toxic("kafka-primary", &toxic).await?;
        Ok(())
    }

    pub async fn apply_kafka_unavailable(&self) -> Result<()> {
        let toxic = Toxic {
            name: "reset".to_string(),
            toxic_type: "reset".to_string(),
            stream: "downstream".to_string(),
            attributes: serde_json::json!({}),
        };
        self.add_toxic("kafka-primary", &toxic).await?;
        Ok(())
    }

    pub async fn apply_localstack_latency(&self, latency_ms: u64) -> Result<()> {
        let toxic = Toxic {
            name: "latency".to_string(),
            toxic_type: "latency".to_string(),
            stream: "downstream".to_string(),
            attributes: serde_json::json!({
                "latency": latency_ms
            }),
        };
        self.add_toxic("localstack", &toxic).await?;
        Ok(())
    }

    pub async fn apply_localstack_unavailable(&self) -> Result<()> {
        let toxic = Toxic {
            name: "reset".to_string(),
            toxic_type: "reset".to_string(),
            stream: "downstream".to_string(),
            attributes: serde_json::json!({}),
        };
        self.add_toxic("localstack", &toxic).await?;
        Ok(())
    }

    pub async fn apply_minio_slow_upload(&self, latency_ms: u64) -> Result<()> {
        let toxic = Toxic {
            name: "latency".to_string(),
            toxic_type: "latency".to_string(),
            stream: "upstream".to_string(),
            attributes: serde_json::json!({
                "latency": latency_ms
            }),
        };
        self.add_toxic("minio", &toxic).await?;
        Ok(())
    }

    pub async fn apply_minio_5xx(&self, error_rate: f64) -> Result<()> {
        let toxic = Toxic {
            name: "http_error".to_string(),
            toxic_type: "http_response".to_string(),
            stream: "downstream".to_string(),
            attributes: serde_json::json!({
                "code": 503,
                "rate": error_rate
            }),
        };
        self.add_toxic("minio", &toxic).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires running toxiproxy
    async fn test_toxiproxy_client() {
        let client = ToxiproxyClient::new("http://localhost:8474");
        let proxies = client.get_proxies().await.unwrap();
        assert!(!proxies.is_empty());
    }
}