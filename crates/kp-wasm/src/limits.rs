//! WASM limits

use crate::WasmError;
use wasmtime::ResourceLimiter;
use std::time::Duration;

pub struct WasmLimiter {
    max_memory: usize,
    max_instructions: u64,
    fuel_consumed: std::sync::atomic::AtomicU64,
}

impl WasmLimiter {
    pub fn new(max_memory_mb: usize, max_instructions: u64) -> Self {
        Self {
            max_memory: max_memory_mb * 1024 * 1024,
            max_instructions,
            fuel_consumed: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl ResourceLimiter for WasmLimiter {
    fn memory_growing(&mut self, _current: usize, desired: usize, _maximum: Option<usize>) -> Result<bool, wasmtime::Error> {
        Ok(desired <= self.max_memory)
    }

    fn table_growing(&mut self, _current: u32, _desired: u32, _maximum: Option<u32>) -> Result<bool, wasmtime::Error> {
        Ok(false)
    }
}