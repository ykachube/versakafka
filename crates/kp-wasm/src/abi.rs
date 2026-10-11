//! WASM ABI

use crate::WasmError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbiVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl AbiVersion {
    pub const CURRENT: Self = Self { major: 1, minor: 0, patch: 0 };
    
    pub fn is_compatible(&self, other: &Self) -> bool {
        self.major == other.major && self.minor >= other.minor
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub abi_version: AbiVersion,
    pub exports: Vec<String>,
}