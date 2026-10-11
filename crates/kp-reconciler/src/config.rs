//! Reconciler configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub window: Duration,
    pub checksum: bool,
    pub auto_replay: bool,
    pub max_replay_batch: usize,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(300), // 5 minutes
            window: Duration::from_secs(3600),  // 1 hour
            checksum: true,
            auto_replay: true,
            max_replay_batch: 1000,
        }
    }
}