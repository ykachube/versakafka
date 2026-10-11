//! Golden corpus of Kafka packets for compatibility testing

use crate::TestKitError;
use std::collections::HashMap;
use std::path::Path;

pub struct GoldenCorpus {
    packets: HashMap<String, Vec<u8>>,
}

impl GoldenCorpus {
    pub fn new() -> Self {
        Self {
            packets: HashMap::new(),
        }
    }

    pub fn load_from_dir(&mut self, dir: &Path) -> Result<(), TestKitError> {
        tracing::info!("Loading golden corpus from {:?}", dir);
        // TODO: Load golden packets from files
        Ok(())
    }

    pub fn add_packet(&mut self, name: String, data: Vec<u8>) {
        self.packets.insert(name, data);
    }

    pub fn get_packet(&self, name: &str) -> Option<&[u8]> {
        self.packets.get(name).map(|v| v.as_slice())
    }

    pub fn packet_names(&self) -> Vec<String> {
        self.packets.keys().cloned().collect()
    }
}