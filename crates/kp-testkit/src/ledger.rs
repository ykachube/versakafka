//! Ledger for tracking sent and acknowledged events

use crate::TestKitError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub producer_id: String,
    pub sequence: i64,
    pub topic: String,
    pub partition: i32,
    pub offset: Option<i64>,
    pub event_id: Option<Vec<u8>>,
    pub timestamp: i64,
    pub acknowledged: bool,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerState {
    pub producer_id: String,
    pub sequence: i64,
    pub epoch: i16,
    pub transactional_id: Option<String>,
}

pub struct Ledger {
    entries: Arc<RwLock<HashMap<String, LedgerEntry>>>,
    producers: Arc<RwLock<HashMap<String, ProducerState>>>,
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            producers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn record_send(&self, entry: LedgerEntry) {
        let key = Self::make_key(&entry.producer_id, entry.sequence);
        self.entries.write().insert(key, entry);
    }

    pub fn record_batch_send(&self, entries: Vec<LedgerEntry>) {
        let mut write = self.entries.write();
        for entry in entries {
            let key = Self::make_key(&entry.producer_id, entry.sequence);
            write.insert(key, entry);
        }
    }

    pub fn record_ack(&self, producer_id: &str, sequence: i64, offset: i64) {
        let key = Self::make_key(producer_id, sequence);
        if let Some(entry) = self.entries.write().get_mut(&key) {
            entry.offset = Some(offset);
            entry.acknowledged = true;
        }
    }

    pub fn record_batch_ack(&self, acks: Vec<(String, i64, i64)>) {
        let mut write = self.entries.write();
        for (producer_id, sequence, offset) in acks {
            let key = Self::make_key(&producer_id, sequence);
            if let Some(entry) = write.get_mut(&key) {
                entry.offset = Some(offset);
                entry.acknowledged = true;
            }
        }
    }

    pub fn get_unacked(&self) -> Vec<LedgerEntry> {
        self.entries.read().values()
            .filter(|e| !e.acknowledged)
            .cloned()
            .collect()
    }

    pub fn get_acked(&self) -> Vec<LedgerEntry> {
        self.entries.read().values()
            .filter(|e| e.acknowledged)
            .cloned()
            .collect()
    }

    pub fn get_all(&self) -> Vec<LedgerEntry> {
        self.entries.read().values().cloned().collect()
    }

    pub fn get_by_producer(&self, producer_id: &str) -> Vec<LedgerEntry> {
        let prefix = format!("{}-", producer_id);
        self.entries.read().values()
            .filter(|e| Self::make_key(&e.producer_id, e.sequence).starts_with(&prefix))
            .cloned()
            .collect()
    }

    pub fn get_entry(&self, producer_id: &str, sequence: i64) -> Option<LedgerEntry> {
        let key = Self::make_key(producer_id, sequence);
        self.entries.read().get(&key).cloned()
    }

    pub fn register_producer(&self, producer_id: String, epoch: i16, transactional_id: Option<String>) {
        let state = ProducerState {
            producer_id: producer_id.clone(),
            sequence: 0,
            epoch,
            transactional_id,
        };
        self.producers.write().insert(producer_id, state);
    }

    pub fn get_producer_state(&self, producer_id: &str) -> Option<ProducerState> {
        self.producers.read().get(producer_id).cloned()
    }

    pub fn update_producer_sequence(&self, producer_id: &str, sequence: i64) {
        if let Some(state) = self.producers.write().get_mut(producer_id) {
            state.sequence = sequence;
        }
    }

    pub fn clear(&self) {
        self.entries.write().clear();
        self.producers.write().clear();
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    fn make_key(producer_id: &str, sequence: i64) -> String {
        format!("{}-{}", producer_id, sequence)
    }
}

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ledger_basic() {
        let ledger = Ledger::new();
        
        ledger.record_send(LedgerEntry {
            producer_id: "producer-1".to_string(),
            sequence: 0,
            topic: "test-topic".to_string(),
            partition: 0,
            offset: None,
            event_id: None,
            timestamp: 1234567890,
            acknowledged: false,
            key: None,
            value: None,
            headers: vec![],
        });
        
        assert_eq!(ledger.len(), 1);
        assert!(ledger.get_entry("producer-1", 0).is_some());
        
        ledger.record_ack("producer-1", 0, 100);
        let entry = ledger.get_entry("producer-1", 0).unwrap();
        assert!(entry.acknowledged);
        assert_eq!(entry.offset, Some(100));
        
        let unacked = ledger.get_unacked();
        assert_eq!(unacked.len(), 0);
        
        let acked = ledger.get_acked();
        assert_eq!(acked.len(), 1);
    }

    #[test]
    fn test_ledger_batch() {
        let ledger = Ledger::new();
        
        let entries = vec![
            LedgerEntry {
                producer_id: "producer-1".to_string(),
                sequence: 0,
                topic: "test-topic".to_string(),
                partition: 0,
                offset: None,
                event_id: None,
                timestamp: 1234567890,
                acknowledged: false,
                key: None,
                value: None,
                headers: vec![],
            },
            LedgerEntry {
                producer_id: "producer-1".to_string(),
                sequence: 1,
                topic: "test-topic".to_string(),
                partition: 0,
                offset: None,
                event_id: None,
                timestamp: 1234567891,
                acknowledged: false,
                key: None,
                value: None,
                headers: vec![],
            },
        ];
        
        ledger.record_batch_send(entries);
        assert_eq!(ledger.len(), 2);
        
        ledger.record_batch_ack(vec![
            ("producer-1".to_string(), 0, 100),
            ("producer-1".to_string(), 1, 101),
        ]);
        
        let acked = ledger.get_acked();
        assert_eq!(acked.len(), 2);
    }
}