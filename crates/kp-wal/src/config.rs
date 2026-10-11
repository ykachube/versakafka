//! WAL configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub segment_size: usize,
    pub segment_max_age: Duration,
    pub fsync_policy: FsyncPolicy,
    pub group_commit_interval: Duration,
    pub max_segments: usize,
    pub recovery: RecoveryConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FsyncPolicy {
    Never,
    Always,
    GroupCommit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryConfig {
    pub truncate_corrupted: bool,
    pub verify_checksums: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./wal"),
            segment_size: 64 * 1024 * 1024, // 64MB
            segment_max_age: Duration::from_secs(3600),
            fsync_policy: FsyncPolicy::GroupCommit,
            group_commit_interval: Duration::from_millis(10),
            max_segments: 1000,
            recovery: RecoveryConfig::default(),
        }
    }
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            truncate_corrupted: true,
            verify_checksums: true,
        }
    }
}