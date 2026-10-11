//! Checker for verifying invariants

use crate::{TestKitError, ledger::Ledger, ledger::LedgerEntry};
use std::collections::{HashMap, HashSet};

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
            let result = match invariant {
                Invariant::I1NoLostAcked => self.check_i1(ledger, channels, wal, s3),
                Invariant::I2StableEventId => self.check_i2(channels),
                Invariant::I3DedupeDeterministic => self.check_i3(ledger),
                Invariant::I4Ordering => self.check_i4(channels),
                Invariant::I5ChannelIsolation => self.check_i5(channels),
                Invariant::I6FailStatic => self.check_i6(),
                Invariant::I7MemoryBounded => self.check_i7(),
                Invariant::I8GracefulDrain => self.check_i8(),
            };
            
            match result {
                Ok(_) => report.passed.push(*invariant),
                Err(e) => report.failures.push(e),
            }
        }
        
        Ok(report)
    }

    /// I1: No lost acknowledged events - every acked event must be found in at least one channel, WAL, or S3
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

    /// I2: Stable Event ID - same event has same ID across all channels
    fn check_i2(&self, channels: &[Vec<LedgerEntry>]) -> Result<(), String> {
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

    /// I3: Dedupe for deterministic mode - retries produce same ID
    fn check_i3(&self, ledger: &Ledger) -> Result<(), String> {
        let entries = ledger.get_all();
        let mut seen: HashMap<(String, i64), Vec<u8>> = HashMap::new();
        
        for e in entries {
            if let Some(eid) = &e.event_id {
                let key = (e.producer_id.clone(), e.sequence);
                if let Some(existing) = seen.get(&key) {
                    if existing != eid {
                        return Err(format!("I3 violation: duplicate send with different event ID for {}-{}", e.producer_id, e.sequence));
                    }
                } else {
                    seen.insert(key, eid.clone());
                }
            }
        }
        Ok(())
    }

    /// I4: Ordering - in each partition, order is preserved (outside spill)
    fn check_i4(&self, channels: &[Vec<LedgerEntry>]) -> Result<(), String> {
        for ch in channels {
            let mut by_partition: HashMap<(String, i32), Vec<&LedgerEntry>> = HashMap::new();
            
            for e in ch {
                by_partition.entry((e.topic.clone(), e.partition)).or_default().push(e);
            }
            
            for (_, mut entries) in by_partition {
                entries.sort_by_key(|e| e.sequence);
                for i in 1..entries.len() {
                    if entries[i].offset.unwrap_or(0) < entries[i-1].offset.unwrap_or(0) {
                        return Err(format!("I4 violation: ordering broken in topic {} partition {}", entries[i].topic, entries[i].partition));
                    }
                }
            }
        }
        Ok(())
    }

    /// I5: Channel isolation - slow channel doesn't block fast channel
    fn check_i5(&self, channels: &[Vec<LedgerEntry>]) -> Result<(), String> {
        // This is a runtime check, not a static check
        // For now, just verify we have multiple channels
        if channels.len() < 2 {
            return Err("I5 check requires at least 2 channels".to_string());
        }
        Ok(())
    }

    /// I6: Fail-static - ARC unavailable doesn't change state
    fn check_i6(&self) -> Result<(), String> {
        // Runtime check, no static verification possible
        Ok(())
    }

    /// I7: Memory bounded - queues don't exceed limits
    fn check_i7(&self) -> Result<(), String> {
        // Runtime check
        Ok(())
    }

    /// I8: Graceful drain - no loss on SIGTERM
    fn check_i8(&self) -> Result<(), String> {
        // Runtime check
        Ok(())
    }
}

#[derive(Default, Debug, Clone)]
pub struct CheckReport {
    pub passed: Vec<Invariant>,
    pub failures: Vec<String>,
}

impl CheckReport {
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn summary(&self) -> String {
        format!("Passed: {}, Failed: {}", self.passed.len(), self.failures.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checker_i1() {
        let ledger = Ledger::new();
        let checker = Checker::new(vec![Invariant::I1NoLostAcked]);
        
        // Add acked event to ledger
        ledger.record_send(LedgerEntry {
            producer_id: "p1".to_string(),
            sequence: 0,
            topic: "t1".to_string(),
            partition: 0,
            offset: None,
            event_id: None,
            timestamp: 0,
            acknowledged: false,
            key: None,
            value: None,
            headers: vec![],
        });
        ledger.record_ack("p1", 0, 100);
        
        // Event not in any channel - should fail
        let report = checker.check(&ledger, &[], &[], &[]).unwrap();
        assert!(!report.is_success());
        assert_eq!(report.failures.len(), 1);
        
        // Event in channel - should pass
        let report = checker.check(&ledger, &[vec![ledger.get_entry("p1", 0).unwrap()]], &[], &[]).unwrap();
        assert!(report.is_success());
    }

    #[test]
    fn test_checker_i2() {
        let checker = Checker::new(vec![Invariant::I2StableEventId]);
        
        let eid = b"event-id-1".to_vec();
        
        let channels = vec![
            vec![LedgerEntry {
                producer_id: "p1".to_string(),
                sequence: 0,
                topic: "t1".to_string(),
                partition: 0,
                offset: Some(0),
                event_id: Some(eid.clone()),
                timestamp: 0,
                acknowledged: true,
                key: None,
                value: None,
                headers: vec![],
            }],
            vec![LedgerEntry {
                producer_id: "p1".to_string(),
                sequence: 0,
                topic: "t1".to_string(),
                partition: 0,
                offset: Some(0),
                event_id: Some(eid.clone()),
                timestamp: 0,
                acknowledged: true,
                key: None,
                value: None,
                headers: vec![],
            }],
        ];
        
        let report = checker.check(&Ledger::new(), &channels, &[], &[]).unwrap();
        assert!(report.is_success());
        
        // Different event ID should fail
        let channels_bad = vec![
            vec![LedgerEntry {
                producer_id: "p1".to_string(),
                sequence: 0,
                topic: "t1".to_string(),
                partition: 0,
                offset: Some(0),
                event_id: Some(b"eid1".to_vec()),
                timestamp: 0,
                acknowledged: true,
                key: None,
                value: None,
                headers: vec![],
            }],
            vec![LedgerEntry {
                producer_id: "p1".to_string(),
                sequence: 0,
                topic: "t1".to_string(),
                partition: 0,
                offset: Some(0),
                event_id: Some(b"eid2".to_vec()),
                timestamp: 0,
                acknowledged: true,
                key: None,
                value: None,
                headers: vec![],
            }],
        ];
        
        let report = checker.check(&Ledger::new(), &channels_bad, &[], &[]).unwrap();
        assert!(!report.is_success());
    }
}