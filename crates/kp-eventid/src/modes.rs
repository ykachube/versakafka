//! Event ID modes

use crate::{EventIdError, Result, EventIdMode, hmac};
use uuid::Uuid;
use sha2::{Sha256, Digest};
use super::config::EventIdConfig;

pub struct EventIdGenerator {
    config: EventIdConfig,
}

impl EventIdGenerator {
    pub fn new(config: EventIdConfig) -> Self {
        Self { config }
    }

    pub fn generate(&self, topic: &str, partition: i32, record: &EventRecord) -> Result<Vec<u8>> {
        let mode = self.get_mode(topic);
        
        let event_id = match mode {
            EventIdMode::Preserve => {
                // Check for existing event-id header
                if let Some(eid) = record.headers.iter().find(|(k, _)| k == "event-id") {
                    eid.1.clone()
                } else {
                    self.generate_uuidv7()?
                }
            }
            EventIdMode::GenerateUuidv7 => self.generate_uuidv7()?,
            EventIdMode::Deterministic => self.generate_deterministic(topic, partition, record)?,
            EventIdMode::ContentHash => self.generate_content_hash(record)?,
            EventIdMode::RejectDuplicate => self.generate_uuidv7()?, // Will be checked separately
        };
        
        // Add HMAC if enabled
        let final_id = if self.config.hmac_enabled {
            if let Some(key) = &self.config.hmac_key {
                hmac::compute_hmac(key.as_bytes(), &event_id)?
            } else {
                event_id
            }
        } else {
            event_id
        };
        
        Ok(final_id)
    }
    
    pub fn verify_hmac(&self, event_id: &[u8], expected_hmac: &[u8]) -> Result<bool> {
        if let Some(key) = &self.config.hmac_key {
            hmac::verify_hmac(key.as_bytes(), event_id, expected_hmac)
        } else {
            Ok(true)
        }
    }

    fn get_mode(&self, topic: &str) -> EventIdMode {
        self.config.per_topic.get(topic).copied().unwrap_or(self.config.default_mode)
    }

    fn generate_uuidv7(&self) -> Result<Vec<u8>> {
        let uuid = Uuid::now_v7();
        Ok(uuid.as_bytes().to_vec())
    }

    fn generate_deterministic(&self, topic: &str, partition: i32, record: &EventRecord) -> Result<Vec<u8>> {
        // Hash of producer identity + sequence
        let producer_id = record.headers.iter()
            .find(|(k, _)| k == "producer-id")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        
        let sequence = record.headers.iter()
            .find(|(k, _)| k == "sequence")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        
        let mut hasher = Sha256::new();
        hasher.update(&producer_id);
        hasher.update(&partition.to_be_bytes());
        hasher.update(&sequence);
        hasher.update(topic.as_bytes());
        
        let hash = hasher.finalize();
        Ok(hash[..16].to_vec()) // 16 bytes for event-id
    }

    fn generate_content_hash(&self, record: &EventRecord) -> Result<Vec<u8>> {
        let mut hasher = Sha256::new();
        hasher.update(record.key.as_ref().unwrap_or(&vec![]));
        hasher.update(&record.value);
        
        let hash = hasher.finalize();
        Ok(hash[..16].to_vec())
    }
}

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: i64,
}