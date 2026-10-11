//! Reconciler checker

use crate::{ReconcilerError, Result, config::ReconcilerConfig};
use kp_eventid::EventIdMode;
use std::collections::HashMap;

pub struct ReconcilerChecker {
    config: ReconcilerConfig,
}

impl ReconcilerChecker {
    pub fn new(config: ReconcilerConfig) -> Self {
        Self { config }
    }

    pub async fn check(&self) -> Result<ReconciliationReport> {
        // TODO: Implement reconciliation logic
        // 1. Read event counts from each channel
        // 2. Compare checksums
        // 3. Identify gaps
        // 4. Generate report
        
        Ok(ReconciliationReport {
            matched: true,
            gaps: Vec::new(),
            discrepancies: Vec::new(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReconciliationReport {
    pub matched: bool,
    pub gaps: Vec<Gap>,
    pub discrepancies: Vec<Discrepancy>,
}

#[derive(Debug, Clone)]
pub struct Gap {
    pub topic: String,
    pub partition: i32,
    pub start_offset: i64,
    pub end_offset: i64,
}

#[derive(Debug, Clone)]
pub struct Discrepancy {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub channel_a_event_id: Option<Vec<u8>>,
    pub channel_b_event_id: Option<Vec<u8>>,
}