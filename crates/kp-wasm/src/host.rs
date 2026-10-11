//! WASM host

use crate::{WasmError, Result};
use wasmtime::{Engine, Module, Store, Linker, Config};
use std::sync::Arc;
use std::path::Path;
use super::config::{WasmConfig, WasmLimits};

pub struct WasmHost {
    engine: Engine,
    config: WasmConfig,
}

impl WasmHost {
    pub fn new(config: WasmConfig) -> Result<Self> {
        let mut wasmtime_config = Config::new();
        wasmtime_config
            .max_wasm_stack(config.limits.max_memory_mb as usize * 1024 * 1024)
            .consume_fuel(true);
        
        let engine = Engine::new(&wasmtime_config)
            .map_err(|e| WasmError::Wasmtime(e))?;
        
        Ok(Self { engine, config })
    }

    pub async fn load_plugin(&self, path: &Path) -> Result<WasmPlugin> {
        let module = Module::from_file(&self.engine, path)
            .map_err(|e| WasmError::Wasmtime(e))?;
        
        Ok(WasmPlugin { module })
    }
}

pub struct WasmPlugin {
    module: wasmtime::Module,
}

impl WasmPlugin {
    pub fn call(&self, func: &str, args: &[wasmtime::Val]) -> Result<Vec<wasmtime::Val>> {
        // TODO: Implement WASM function calling
        Ok(vec![])
    }
}