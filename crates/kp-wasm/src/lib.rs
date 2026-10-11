//! kp-wasm - WASM host for wasmtime with ABI versioning, limits, hot-reload

pub mod host;
pub mod abi;
pub mod limits;
pub mod sdk;
pub mod config;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WasmError {
    #[error("wasmtime error: {0}")]
    Wasmtime(wasmtime::Error),
    #[error("abi error: {0}")]
    Abi(String),
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("hot reload error: {0}")]
    HotReload(String),
    #[error("config error: {0}")]
    Config(String),
}

impl From<wasmtime::Error> for WasmError {
    fn from(e: wasmtime::Error) -> Self {
        WasmError::Wasmtime(e)
    }
}

impl WasmError {
    pub fn from_config_error(e: anyhow::Error) -> Self {
        WasmError::Config(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, WasmError>;