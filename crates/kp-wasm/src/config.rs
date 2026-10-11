//! WASM configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfig {
    pub enabled: bool,
    pub plugin_dir: String,
    pub abi_version: u32,
    pub limits: WasmLimits,
    pub hot_reload: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmLimits {
    pub max_memory_mb: usize,
    pub max_cpu_time_ms: u64,
    pub max_instructions: u64,
    pub timeout: Duration,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            plugin_dir: "./plugins".to_string(),
            abi_version: 1,
            limits: WasmLimits::default(),
            hot_reload: true,
        }
    }
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            max_memory_mb: 16,
            max_cpu_time_ms: 10,
            max_instructions: 1_000_000,
            timeout: Duration::from_millis(50),
        }
    }
}