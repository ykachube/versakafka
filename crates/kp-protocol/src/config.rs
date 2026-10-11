//! Protocol configuration

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolConfig {
    pub supported_versions: Vec<i16>,
    pub flexible_versions_enabled: bool,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            supported_versions: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            flexible_versions_enabled: true,
        }
    }
}