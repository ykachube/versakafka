//! Spill configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;
use object_store::ObjectStoreScheme;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpillConfig {
    pub enabled: bool,
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: Option<S3Credentials>,
    pub upload: UploadConfig,
    pub manifest: ManifestConfig,
    pub retention: RetentionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    pub multipart_threshold: usize,
    pub multipart_chunksize: usize,
    pub max_concurrent_uploads: usize,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestConfig {
    pub enabled: bool,
    pub format: ManifestFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestFormat {
    Json,
    Parquet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    pub max_age: Duration,
    pub max_size_bytes: Option<u64>,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bucket: "kafka-spill".to_string(),
            prefix: "spill/".to_string(),
            region: None,
            endpoint: None,
            credentials: None,
            upload: UploadConfig::default(),
            manifest: ManifestConfig::default(),
            retention: RetentionConfig::default(),
        }
    }
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: 8 * 1024 * 1024,
            multipart_chunksize: 8 * 1024 * 1024,
            max_concurrent_uploads: 10,
            timeout: Duration::from_secs(60),
        }
    }
}

impl Default for ManifestConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            format: ManifestFormat::Json,
        }
    }
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(7 * 24 * 3600), // 7 days
            max_size_bytes: None,
        }
    }
}