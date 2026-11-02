//! WAL recovery

use crate::{WalError, Result, config::WalConfig, config::RecoveryConfig};
use super::segment::Segment;
use std::fs;
use std::path::Path;
use tracing::{info, warn, error};

pub fn recover(config: WalConfig, recovery_config: RecoveryConfig) -> Result<Vec<Segment>> {
    let mut segments = Vec::new();
    
    let entries = fs::read_dir(&config.dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "wal").unwrap_or(false))
        .collect::<Vec<_>>();
    
    // Sort by segment ID (filename)
    let mut entries = entries;
    entries.sort_by_key(|e| e.path().file_name().unwrap().to_string_lossy().to_string());
    
    info!("Found {} WAL segments to recover", entries.len());
    
    for entry in entries {
        match Segment::open(config.clone(), &entry.path()) {
            Ok(mut segment) => {
                // Verify segment checksums if configured
                if recovery_config.verify_checksums {
                    match segment.verify_checksums() {
                        Ok(_) => {
                            info!("Segment {} checksums verified", segment.header.segment_id);
                        }
                        Err(e) => {
                            error!("Segment {} checksum verification failed: {}", segment.header.segment_id, e);
                            if recovery_config.truncate_corrupted {
                                warn!("Truncating corrupted segment: {}", entry.path().display());
                                if let Err(e) = fs::remove_file(&entry.path()) {
                                    error!("Failed to remove corrupted segment: {}", e);
                                }
                                continue;
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                segments.push(segment);
            }
            Err(e) => {
                error!("Failed to open segment {}: {}", entry.path().display(), e);
                if recovery_config.truncate_corrupted {
                    warn!("Removing corrupted segment file: {}", entry.path().display());
                    if let Err(e) = fs::remove_file(&entry.path()) {
                        error!("Failed to remove corrupted segment file: {}", e);
                    }
                } else {
                    return Err(e);
                }
            }
        }
    }
    
    info!("Successfully recovered {} segments", segments.len());
    Ok(segments)
}