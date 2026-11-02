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
    
    pub fn start_new_manifest(&mut self) {
        self.current_manifest = Some(Manifest {
            version: 1,
            created_at: Utc::now(),
            segments: Vec::new(),
            index: HashMap::new(),
        });
    }
    
    pub fn add_segment(&mut self, segment: ManifestSegment) -> Result<()> {
        if self.current_manifest.is_none() {
            self.start_new_manifest();
        }
        
        if let Some(manifest) = &mut self.current_manifest {
            // Update index
            for i in 0..segment.record_count {
                let key = format!("{}-{}", segment.path, i);
                manifest.index.insert(key, SegmentIndex {
                    segment_path: segment.path.clone(),
                    offset: i,
                    length: segment.size / segment.record_count.max(1),
                });
            }
            manifest.segments.push(segment);
            Ok(())
        } else {
            Err(SpillError::Manifest("No active manifest".to_string()))
        }
    }
    
    pub fn get_current_manifest(&self) -> Option<&Manifest> {
        self.current_manifest.as_ref()
    }
    
    pub async fn persist(&self, uploader: &super::uploader::Uploader) -> Result<()> {
        if let Some(manifest) = &self.current_manifest {
            uploader.upload_manifest(manifest).await
        } else {
            Ok(())
        }
    }
    
    pub fn segment_count(&self) -> usize {
        self.current_manifest.as_ref().map(|m| m.segments.len()).unwrap_or(0)
    }
    
    pub fn total_records(&self) -> u64 {
        self.current_manifest.as_ref().map(|m| m.segments.iter().map(|s| s.record_count).sum()).unwrap_or(0)
    }
}