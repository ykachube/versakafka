//! Configuration for kp-bin

use figment::{Figment, providers::{Toml, Env, Format}, Error as FigmentError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use kp_telemetry::config::TelemetryConfig;
use kp_proxy_core::config::ProxyConfig as CoreProxyConfig;
use kp_metadata::config::MetadataConfig;
use kp_filters::config::FiltersConfig;
use kp_wasm::config::WasmConfig;
use kp_eventid::config::EventIdConfig;
use kp_breaker::config::BreakerConfig;
use kp_channel::config::ChannelConfig;
use kp_wal::config::WalConfig;
use kp_spill::config::SpillConfig;
use kp_reconciler::config::ReconcilerConfig;
use kp_crypto::config::CryptoConfig;
use anyhow::{Error as AnyError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub proxy: CoreProxyConfig,
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

impl ProxyConfig {
    pub fn load() -> Result<Self> {
        let config_path = std::env::var("KP_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
        
        Figment::new()
            .merge(Toml::file(config_path))
            .merge(Env::prefixed("KP_").global())
            .extract()
            .map_err(|e: FigmentError| AnyError::new(e))
    }
}