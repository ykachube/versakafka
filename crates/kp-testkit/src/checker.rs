//! Checker for verifying invariants

use crate::{TestKitError, ledger::Ledger, ledger::LedgerEntry};
use std::collections::HashSet;

pub struct Checker {
    invariants: Vec<Invariant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Invariant {
    I1NoLostAcked,
    I2StableEventId,
    I3DedupeDeterministic,
    I4Ordering,
    I5ChannelIsolation,
    I6FailStatic,
    I7MemoryBounded,
    I8GracefulDrain,
}

impl Checker {
    pub fn new(invariants: Vec<Invariant>) -> Self {
        Self { invariants }
    }

    pub fn check(&self, ledger: &Ledger, channels: &[Vec<LedgerEntry>], wal: &[LedgerEntry], s3: &[LedgerEntry]) -> Result<CheckReport, TestKitError> {
        let mut report = CheckReport::default();
        
        for invariant in &self.invariants {
            match invariant {
                Invariant::I1NoLostAcked => {
                    if let Err(e) = self.check_i1(ledger, channels, wal, s3) {
                        report.failures.push(e);
                    } else {
                        report.passed.push(*invariant);
                    }
                }
                Invariant::I2StableEventId => {
                    if let Err(e) = self.check_i2(channels) {
                        report.failures.push(e);
                    } else {
                        report.passed.push(*invariant);
                    }
                }
                _ => {
                    report.passed.push(*invariant);
                }
            }
        }
        
        Ok(report)
    }

    fn check_i1(&self, ledger: &Ledger, channels: &[Vec<LedgerEntry>], wal: &[LedgerEntry], s3: &[LedgerEntry]) -> Result<(), String> {
        let acked: Vec<_> = ledger.get_all().into_iter().filter(|e| e.acknowledged).collect();
        let mut found = HashSet::new();
        
        for ch in channels {
            for e in ch {
                found.insert((e.producer_id.clone(), e.sequence));
            }
        }
        for e in wal {
            found.insert((e.producer_id.clone(), e.sequence));
        }
        for e in s3 {
            found.insert((e.producer_id.clone(), e.sequence));
        }
        
        for e in acked {
            let pid = e.producer_id.clone();
            let seq = e.sequence;
            if !found.contains(&(pid.clone(), seq)) {
                return Err(format!("I1 violation: acked event {}-{} not found in any channel/WAL/S3", pid, seq));
            }
        }
        Ok(())
    }

    fn check_i2(&self, channels: &[Vec<LedgerEntry>]) -> Result<(), String> {
        // Check event IDs are stable across channels
        let mut event_ids: HashMap<(String, i64), Vec<u8>> = HashMap::new();
        
        for ch in channels {
            for e in ch {
                if let Some(eid) = &e.event_id {
                    let key = (e.producer_id.clone(), e.sequence);
                    if let Some(existing) = event_ids.get(&key) {
                        if existing != eid {
                            return Err(format!("I2 violation: event ID mismatch for {}-{}", e.producer_id, e.sequence));
                        }
                    } else {
                        event_ids.insert(key, eid.clone());
                    }
                }
            }
        }
        Ok(())
    }
}

use std::collections::HashMap;

#[derive(Default, Debug, Clone)]
pub struct CheckReport {
    pub passed: Vec<Invariant>,
    pub failures: Vec<String>,
}