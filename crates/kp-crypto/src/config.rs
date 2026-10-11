//! Crypto configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoConfig {
    pub enabled: bool,
    pub kms: KmsConfig,
    pub dek: DekConfig,
    pub headers: CryptoHeaders,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsConfig {
    pub key_id: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub cache_ttl: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DekConfig {
    pub algorithm: DekAlgorithm,
    pub rotation_interval: Duration,
    pub cache_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DekAlgorithm {
    Aes256Gcm,
    Aes128Gcm,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoHeaders {
    pub key_id: bool,
    pub iv: bool,
    pub tag: bool,
    pub algorithm: bool,
}

impl Default for CryptoConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            kms: KmsConfig::default(),
            dek: DekConfig::default(),
            headers: CryptoHeaders::default(),
        }
    }
}

impl Default for KmsConfig {
    fn default() -> Self {
        Self {
            key_id: "".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            cache_ttl: Duration::from_secs(300),
        }
    }
}

impl Default for DekConfig {
    fn default() -> Self {
        Self {
            algorithm: DekAlgorithm::Aes256Gcm,
            rotation_interval: Duration::from_secs(24 * 3600),
            cache_size: 100,
        }
    }
}

impl Default for CryptoHeaders {
    fn default() -> Self {
        Self {
            key_id: true,
            iv: true,
            tag: true,
            algorithm: true,
        }
    }
}