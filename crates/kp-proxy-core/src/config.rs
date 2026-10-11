//! Proxy core configuration

use config::{Config, File, Environment};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub listen_addr: String,
    pub tls: Option<TlsConfig>,
    pub max_connections: usize,
    pub connection_timeout: Duration,
    pub request_timeout: Duration,
    pub backpressure: BackpressureConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_file: String,
    pub key_file: String,
    pub ca_file: Option<String>,
    pub require_client_cert: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureConfig {
    pub max_pending_requests: usize,
    pub max_request_size: usize,
    pub enable: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9092".to_string(),
            tls: None,
            max_connections: 10000,
            connection_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
            backpressure: BackpressureConfig::default(),
        }
    }
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_pending_requests: 1000,
            max_request_size: 10 * 1024 * 1024, // 10MB
            enable: true,
        }
    }
}