//! WAL recovery

use crate::{WalError, Result, config::WalConfig, config::RecoveryConfig};
use super::segment::Segment;
use std::fs;
use std::path::Path;

pub fn recover(config: WalConfig, recovery_config: RecoveryConfig) -> Result<Vec<Segment>> {
    let mut segments = Vec::new();
    
    let entries = fs::read_dir(&config.dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "wal").unwrap_or(false))
        .collect::<Vec<_>>();
    
    // Sort by segment ID (filename)
    let mut entries = entries;
    entries.sort_by_key(|e| e.path().file_name().unwrap().to_string_lossy().to_string());
    
    for entry in entries {
        match Segment::open(config.clone(), &entry.path()) {
            Ok(segment) => {
                // Verify segment
                if recovery_config.verify_checksums {
                    // TODO: Verify segment checksums
                }
                segments.push(segment);
            }
            Err(e) => {
                if recovery_config.truncate_corrupted {
                    tracing::warn!("Truncating corrupted segment: {:?}", e);
                    // TODO: Remove corrupted segment file
                } else {
                    return Err(e);
                }
            }
        }
    }
    
    Ok(segments)
}