//! Telemetry configuration

use config::{Config, File, Environment};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    pub service_name: String,
    pub otlp_endpoint: Option<String>,
    pub prometheus_port: u16,
    pub log_level: String,
    pub log_format: LogFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Text,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: "kp-proxy".to_string(),
            otlp_endpoint: None,
            prometheus_port: 9090,
            log_level: "info".to_string(),
            log_format: LogFormat::Json,
        }
    }
}