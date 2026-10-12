//! Built-in filters

use crate::{FilterError, Result, chain::{FilterStage, FilterBatch, FilterRecord}, hooks::FilterContext};
use serde_json::Value;
use std::collections::HashMap;

/// Enrichment filter - adds metadata headers to records
pub struct EnrichmentFilter {
    static_headers: HashMap<String, Vec<u8>>,
}

impl EnrichmentFilter {
    pub fn from_config(config: Value) -> Self {
        let static_headers = config.get("headers")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| {
                        v.as_str().map(|s| (k.clone(), s.as_bytes().to_vec()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        
        Self { static_headers }
    }
}

impl FilterStage for EnrichmentFilter {
    fn name(&self) -> &str {
        "enrichment"
    }
    
    fn process_batch(&self, batch: &mut FilterBatch, _context: &FilterContext) -> Result<()> {
        for record in &mut batch.records {
            for (key, value) in &self.static_headers {
                // Check if header already exists
                if !record.headers.iter().any(|(k, _)| k == key) {
                    record.headers.push((key.clone(), value.clone()));
                }
            }
        }
        Ok(())
    }
}

/// Filtering filter - drops records based on conditions
pub struct FilteringFilter {
    drop_if_header_missing: Option<String>,
    drop_if_header_value: Option<(String, Vec<u8>)>,
    drop_topic_prefix: Option<String>,
}

impl FilteringFilter {
    pub fn from_config(config: Value) -> Self {
        let drop_if_header_missing = config.get("drop_if_header_missing")
            .and_then(|v| v.as_str())
            .map(String::from);
        
        let drop_if_header_value = config.get("drop_if_header_value")
            .and_then(|v| v.as_object())
            .and_then(|obj| {
                obj.get("header").and_then(|h| h.as_str())
                    .zip(obj.get("value").and_then(|v| v.as_str()))
                    .map(|(h, v)| (h.to_string(), v.as_bytes().to_vec()))
            });
        
        let drop_topic_prefix = config.get("drop_topic_prefix")
            .and_then(|v| v.as_str())
            .map(String::from);
        
        Self {
            drop_if_header_missing,
            drop_if_header_value,
            drop_topic_prefix,
        }
    }
}

impl FilterStage for FilteringFilter {
    fn name(&self) -> &str {
        "filtering"
    }
    
    fn process_batch(&self, batch: &mut FilterBatch, _context: &FilterContext) -> Result<()> {
        // Check topic prefix first
        if let Some(prefix) = &self.drop_topic_prefix {
            if batch.topic.starts_with(prefix) {
                batch.records.clear();
                return Ok(());
            }
        }
        
        // Filter records
        batch.records.retain(|record| {
            // Check if header missing
            if let Some(header) = &self.drop_if_header_missing {
                if !record.headers.iter().any(|(k, _)| k == header) {
                    return false;
                }
            }
            
            // Check header value
            if let Some((header, expected_value)) = &self.drop_if_header_value {
                if let Some((_, value)) = record.headers.iter().find(|(k, _)| k == header) {
                    if value != expected_value {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            
            true
        });
        
        Ok(())
    }
}