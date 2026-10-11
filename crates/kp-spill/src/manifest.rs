//! Spill manifest

use crate::{SpillError, Result, config::SpillConfig, config::ManifestConfig, config::ManifestFormat};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub created_at: DateTime<Utc>,
    pub segments: Vec<ManifestSegment>,
    pub index: HashMap<String, SegmentIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSegment {
    pub path: String,
    pub size: u64,
    pub record_count: u64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentIndex {
    pub segment_path: String,
    pub offset: u64,
    pub length: u64,
}

pub struct ManifestManager {
    config: SpillConfig,
    current_manifest: Option<Manifest>,
}

impl ManifestManager {
    pub fn new(config: SpillConfig) -> Self {
        Self {
            config,
            current_manifest: None,
        }
    }

    pub fn add_segment(&mut self, segment: ManifestSegment) {
        if self.current_manifest.is_none() {
            self.current_manifest = Some(Manifest {
                version: 1,
                created_at: Utc::now(),
                segments: Vec::new(),
                index: HashMap::new(),
            });
        }
        
        if let Some(manifest) = &mut self.current_manifest {
            manifest.segments.push(segment);
        }
    }

    pub async fn persist(&self, uploader: &super::uploader::Uploader) -> Result<()> {
        if let Some(manifest) = &self.current_manifest {
            let data = match self.config.manifest.format {
                ManifestFormat::Json => serde_json::to_vec_pretty(manifest)?,
                ManifestFormat::Parquet => {
                    // TODO: Implement parquet serialization
                    return Err(SpillError::Manifest("Parquet format not implemented".to_string()));
                }
            };
            
            let path = format!("{}manifest-{}.json", self.config.prefix, manifest.created_at.timestamp_millis());
            uploader.upload(path, bytes::Bytes::from(data)).await?;
        }
        Ok(())
    }
}