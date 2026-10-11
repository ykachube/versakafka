//! Golden corpus of Kafka packets for compatibility testing

use crate::TestKitError;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoldenPacket {
    pub name: String,
    pub api_key: i16,
    pub api_version: i16,
    pub direction: PacketDirection,
    pub client_type: String,
    pub data: Vec<u8>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PacketDirection {
    Request,
    Response,
}

pub struct GoldenCorpus {
    packets: HashMap<String, GoldenPacket>,
    corpus_dir: PathBuf,
}

impl GoldenCorpus {
    pub fn new(corpus_dir: impl Into<PathBuf>) -> Self {
        Self {
            packets: HashMap::new(),
            corpus_dir: corpus_dir.into(),
        }
    }

    pub fn load(&mut self) -> Result<(), TestKitError> {
        if !self.corpus_dir.exists() {
            info!("Corpus directory does not exist: {:?}", self.corpus_dir);
            return Ok(());
        }

        for entry in fs::read_dir(&self.corpus_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let content = fs::read_to_string(&path)?;
                let packet: GoldenPacket = serde_json::from_str(&content)?;
                self.packets.insert(packet.name.clone(), packet);
            }
        }

        info!("Loaded {} golden packets from {:?}", self.packets.len(), self.corpus_dir);
        Ok(())
    }

    pub fn add_packet(&mut self, packet: GoldenPacket) {
        self.packets.insert(packet.name.clone(), packet);
    }

    pub fn get_packet(&self, name: &str) -> Option<&GoldenPacket> {
        self.packets.get(name)
    }

    pub fn packet_names(&self) -> Vec<String> {
        self.packets.keys().cloned().collect()
    }

    pub fn save_all(&self) -> Result<(), TestKitError> {
        fs::create_dir_all(&self.corpus_dir)?;
        
        for packet in self.packets.values() {
            let filename = format!("{}.json", packet.name);
            let path = self.corpus_dir.join(filename);
            let content = serde_json::to_string_pretty(packet)?;
            fs::write(path, content)?;
        }

        info!("Saved {} golden packets to {:?}", self.packets.len(), self.corpus_dir);
        Ok(())
    }

    pub fn compare_with(&self, other: &GoldenCorpus) -> Vec<String> {
        let mut diffs = Vec::new();
        
        for (name, packet) in &self.packets {
            if let Some(other_packet) = other.packets.get(name) {
                if packet.data != other_packet.data {
                    diffs.push(format!("Packet '{}' differs", name));
                }
                if packet.api_version != other_packet.api_version {
                    diffs.push(format!("Packet '{}' version mismatch: {} vs {}", name, packet.api_version, other_packet.api_version));
                }
            } else {
                diffs.push(format!("Packet '{}' missing in other corpus", name));
            }
        }

        for name in other.packets.keys() {
            if !self.packets.contains_key(name) {
                diffs.push(format!("Packet '{}' extra in other corpus", name));
            }
        }

        diffs
    }
}

/// Utility for recording golden packets from live traffic
pub struct GoldenRecorder {
    corpus: GoldenCorpus,
    client_type: String,
}

impl GoldenRecorder {
    pub fn new(corpus_dir: impl Into<PathBuf>, client_type: impl Into<String>) -> Self {
        Self {
            corpus: GoldenCorpus::new(corpus_dir),
            client_type: client_type.into(),
        }
    }

    pub fn record_request(&mut self, api_key: i16, api_version: i16, name: impl Into<String>, data: Vec<u8>) {
        let packet = GoldenPacket {
            name: name.into(),
            api_key,
            api_version,
            direction: PacketDirection::Request,
            client_type: self.client_type.clone(),
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        self.corpus.add_packet(packet);
    }

    pub fn record_response(&mut self, api_key: i16, api_version: i16, name: impl Into<String>, data: Vec<u8>) {
        let packet = GoldenPacket {
            name: name.into(),
            api_key,
            api_version,
            direction: PacketDirection::Response,
            client_type: self.client_type.clone(),
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        self.corpus.add_packet(packet);
    }

    pub fn save(&self) -> Result<(), TestKitError> {
        self.corpus.save_all()
    }

    pub fn corpus(&self) -> &GoldenCorpus {
        &self.corpus
    }

    pub fn corpus_mut(&mut self) -> &mut GoldenCorpus {
        &mut self.corpus
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_golden_corpus() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut corpus = GoldenCorpus::new(temp_dir.path());
        
        corpus.add_packet(GoldenPacket {
            name: "test-produce-v1".to_string(),
            api_key: 0,
            api_version: 1,
            direction: PacketDirection::Request,
            client_type: "test".to_string(),
            data: vec![1, 2, 3, 4],
            timestamp: 1234567890,
        });
        
        corpus.save_all().unwrap();
        
        let mut corpus2 = GoldenCorpus::new(temp_dir.path());
        corpus2.load().unwrap();
        
        assert_eq!(corpus2.packet_names().len(), 1);
        let packet = corpus2.get_packet("test-produce-v1").unwrap();
        assert_eq!(packet.api_key, 0);
        assert_eq!(packet.data, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_golden_recorder() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut recorder = GoldenRecorder::new(temp_dir.path(), "test-client");
        
        recorder.record_request(0, 1, "produce-request-v1", vec![1, 2, 3]);
        recorder.record_response(0, 1, "produce-response-v1", vec![4, 5, 6]);
        
        recorder.save().unwrap();
        
        let mut corpus = GoldenCorpus::new(temp_dir.path());
        corpus.load().unwrap();
        
        assert_eq!(corpus.packet_names().len(), 2);
    }
}