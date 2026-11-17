//! Proxy core configuration

use config::{Config, File, Environment};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use kp_metadata::config::MetadataConfig;
use kp_filters::config::FiltersConfig;
use kp_wasm::config::WasmConfig;
use kp_eventid::config::EventIdConfig;
use kp_breaker::config::BreakerConfig;
use kp_channel::config::{ChannelConfig, ChannelType};
use kp_wal::config::WalConfig;
use kp_spill::config::SpillConfig;
use kp_reconciler::config::ReconcilerConfig;
use kp_crypto::config::CryptoConfig;
use kp_telemetry::config::TelemetryConfig;

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

/// Unified server configuration that aggregates all sub-configs from kp-bin
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub proxy: ProxyConfig,
    pub telemetry: TelemetryConfig,
    pub metadata: MetadataConfig,
    pub filters: FiltersConfig,
    pub wasm: WasmConfig,
    pub event_id: EventIdConfig,
    pub breaker: BreakerConfig,
    pub channels: Vec<ChannelConfig>,
    pub wal: WalConfig,
    pub spill: SpillConfig,
    pub reconciler: ReconcilerConfig,
    pub crypto: CryptoConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            proxy: ProxyConfig::default(),
            telemetry: TelemetryConfig::default(),
            metadata: MetadataConfig::default(),
            filters: FiltersConfig::default(),
            wasm: WasmConfig::default(),
            event_id: EventIdConfig::default(),
            breaker: BreakerConfig::default(),
            channels: vec![],
            wal: WalConfig::default(),
            spill: SpillConfig::default(),
            reconciler: ReconcilerConfig::default(),
            crypto: CryptoConfig::default(),
        }
    }
}