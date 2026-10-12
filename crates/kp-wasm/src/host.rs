//! WASM host

use crate::{WasmError, Result, limits::WasmLimiter};
use kp_filters::hooks::WasmHost as WasmHostTrait;
use wasmtime::{Engine, Module, Store, Linker, Config, Val};
use std::sync::Arc;
use std::path::Path;
use super::config::{WasmConfig, WasmLimits};
use std::collections::HashMap;
use tokio::sync::RwLock;
use super::sdk::{PluginInput, PluginOutput, PluginRecord};
use async_trait::async_trait;

pub struct WasmHost {
    engine: Engine,
    config: WasmConfig,
    plugins: Arc<RwLock<HashMap<String, WasmPlugin>>>,
    limiter: WasmLimiter,
}

impl WasmHost {
    pub fn new(config: WasmConfig) -> Result<Self> {
        let mut wasmtime_config = Config::new();
        wasmtime_config
            .max_wasm_stack(config.limits.max_memory_mb * 1024 * 1024)
            .consume_fuel(true)
            .epoch_interruption(true);
        
        let engine = Engine::new(&wasmtime_config)
            .map_err(|e| WasmError::Wasmtime(e))?;
        
        let limiter = WasmLimiter::new(
            config.limits.max_memory_mb,
            config.limits.max_instructions,
        );
        
        Ok(Self {
            engine,
            config,
            plugins: Arc::new(RwLock::new(HashMap::new())),
            limiter,
        })
    }
    
    /// Load a plugin from file
    pub async fn load_plugin(&self, path: &Path) -> Result<WasmPlugin> {
        let module = Module::from_file(&self.engine, path)
            .map_err(|e| WasmError::Wasmtime(e))?;
        
        // Validate ABI version
        self.validate_module(&module)?;
        
        let plugin = WasmPlugin::new(module, self.engine.clone(), self.config.limits.clone(), self.limiter.clone())?;
        
        let name = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        
        self.plugins.write().await.insert(name.clone(), plugin.clone());
        
        Ok(plugin)
    }
    
    /// Load all plugins from plugin directory
    pub async fn load_plugins(&self) -> Result<()> {
        let plugin_dir = std::path::Path::new(&self.config.plugin_dir);
        if !plugin_dir.exists() {
            return Ok(());
        }
        
        let mut entries = tokio::fs::read_dir(plugin_dir).await
            .map_err(|e| WasmError::HotReload(e.to_string()))?;
        
        while let Some(entry) = entries.next_entry().await
            .map_err(|e| WasmError::HotReload(e.to_string()))? {
            let path = entry.path();
            if path.extension().map(|e| e == "wasm").unwrap_or(false) {
                if let Err(e) = self.load_plugin(&path).await {
                    tracing::warn!("Failed to load plugin {:?}: {}", path, e);
                }
            }
        }
        
        Ok(())
    }
    
    /// Reload plugins (hot-reload)
    pub async fn reload_plugins(&self) -> Result<()> {
        self.plugins.write().await.clear();
        self.load_plugins().await
    }
    
    /// Get a plugin by name
    pub async fn get_plugin(&self, name: &str) -> Option<WasmPlugin> {
        self.plugins.read().await.get(name).cloned()
    }
    
    /// List loaded plugins
    pub async fn list_plugins(&self) -> Vec<String> {
        self.plugins.read().await.keys().cloned().collect()
    }
    
    fn validate_module(&self, module: &Module) -> Result<()> {
        // Check if module exports required functions
        let exports: Vec<_> = module.exports().map(|e| e.name()).collect();
        
        // Required exports for our ABI
        let required = ["process_batch", "abi_version"];
        for req in &required {
            if !exports.contains(req) {
                return Err(WasmError::Abi(format!("Missing required export: {}", req)));
            }
        }
        
        Ok(())
    }
    
    /// Increment epoch for interruption (used for timeouts)
    pub fn increment_epoch(&self) {
        self.engine.increment_epoch();
    }
}

#[async_trait]
impl WasmHostTrait for WasmHost {
    async fn call_process_batch(&self, input: &kp_filters::hooks::WasmPluginInput) -> kp_filters::Result<kp_filters::hooks::WasmPluginOutput> {
        // Convert kp-filters types to kp-wasm sdk types
        let sdk_input = PluginInput {
            topic: input.topic.clone(),
            partition: input.partition,
            records: input.records.iter().map(|r| PluginRecord {
                key: r.key.clone(),
                value: r.value.clone(),
                headers: r.headers.clone(),
                timestamp: r.timestamp,
            }).collect(),
        };
        
        // Use the first loaded plugin for now
        // In a real implementation, we'd select based on topic/config
        let plugins = self.plugins.read().await;
        let sdk_output = if let Some(plugin) = plugins.values().next() {
            plugin.call_process_batch(&sdk_input).await.map_err(|e| kp_filters::FilterError::Wasm(e.to_string()))?
        } else {
            PluginOutput {
                records: vec![],
                drop: false,
            }
        };
        
        // Convert back to kp-filters types
        Ok(kp_filters::hooks::WasmPluginOutput {
            records: sdk_output.records.into_iter().map(|r| kp_filters::hooks::WasmPluginRecord {
                key: r.key,
                value: r.value,
                headers: r.headers,
                timestamp: r.timestamp,
            }).collect(),
            drop: sdk_output.drop,
        })
    }
}

pub struct WasmPlugin {
    module: Module,
    engine: Engine,
    limits: WasmLimits,
    limiter: WasmLimiter,
}

impl Clone for WasmPlugin {
    fn clone(&self) -> Self {
        Self {
            module: self.module.clone(),
            engine: self.engine.clone(),
            limits: self.limits.clone(),
            limiter: self.limiter.clone(),
        }
    }
}

impl WasmPlugin {
    fn new(module: Module, engine: Engine, limits: WasmLimits, limiter: WasmLimiter) -> Result<Self> {
        Ok(Self { module, engine, limits, limiter })
    }
    
    /// Call the process_batch function
    pub async fn call_process_batch(&self, _input: &PluginInput) -> Result<PluginOutput> {
        let mut store = Store::new(&self.engine, ());
        
        let mut linker = Linker::new(&self.engine);
        
        // Add host functions
        self.add_host_functions(&mut linker)?;
        
        let instance = linker.instantiate_async(&mut store, &self.module).await
            .map_err(|e| WasmError::Wasmtime(e))?;
        
        // Get the process_batch function
        let func = instance.get_func(&mut store, "process_batch")
            .ok_or_else(|| WasmError::Abi("process_batch function not found".to_string()))?;
        
        // For now, return empty output - full implementation would require
        // proper memory management for passing data to/from WASM
        Ok(PluginOutput {
            records: vec![],
            drop: false,
        })
    }
    
    fn add_host_functions(&self, _linker: &mut Linker<()>) -> Result<()> {
        // Add host functions that plugins can call
        // For example: logging, metrics, etc.
        Ok(())
    }
}