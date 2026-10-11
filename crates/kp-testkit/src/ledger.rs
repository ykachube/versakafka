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
}

pub struct Ledger {
    entries: Arc<RwLock<HashMap<String, LedgerEntry>>>,
}

impl Ledger {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn record_send(&self, entry: LedgerEntry) {
        let key = format!("{}-{}", entry.producer_id, entry.sequence);
        self.entries.write().insert(key, entry);
    }

    pub fn record_ack(&self, producer_id: &str, sequence: i64, offset: i64) {
        let key = format!("{}-{}", producer_id, sequence);
        if let Some(entry) = self.entries.write().get_mut(&key) {
            entry.offset = Some(offset);
            entry.acknowledged = true;
        }
    }

    pub fn get_unacked(&self) -> Vec<LedgerEntry> {
        self.entries.read().values()
            .filter(|e| !e.acknowledged)
            .cloned()
            .collect()
    }

    pub fn get_all(&self) -> Vec<LedgerEntry> {
        self.entries.read().values().cloned().collect()
    }
}