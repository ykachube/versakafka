//! Scenario runner for chaos testing

use crate::{TestKitError, checker::{Checker, Invariant, CheckReport}, ledger::Ledger};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub seed: u64,
    pub duration: Duration,
    pub load: LoadConfig,
    pub failures: Vec<FailureConfig>,
    pub invariants: Vec<Invariant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadConfig {
    pub throughput: u64,
    pub batch_size: usize,
    pub num_producers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureConfig {
    pub failure_type: FailureType,
    pub target: String,
    pub start_time: Duration,
    pub duration: Duration,
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureType {
    KillBroker,
    NetworkPartition,
    SlowBroker,
    KillChannel,
    S3Unavailable,
    DiskFull,
    ProxyRestart,
    ArcFlapping,
}

pub struct ScenarioRunner {
    scenario: Scenario,
    ledger: Ledger,
    checker: Checker,
}

impl ScenarioRunner {
    pub fn new(scenario: Scenario) -> Self {
        let invariants = scenario.invariants.clone();
        Self {
            scenario,
            ledger: Ledger::new(),
            checker: Checker::new(invariants),
        }
    }

    pub async fn run(&mut self) -> Result<ScenarioResult, TestKitError> {
        tracing::info!("Running scenario: {} (seed: {})", self.scenario.name, self.scenario.seed);
        
        // TODO: Implement actual scenario execution
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let report = self.checker.check(&self.ledger, &[], &[], &[])?;
        
        Ok(ScenarioResult {
            scenario_name: self.scenario.name.clone(),
            seed: self.scenario.seed,
            check_report: report.clone(),
            passed: report.failures.is_empty(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub scenario_name: String,
    pub seed: u64,
    pub check_report: CheckReport,
    pub passed: bool,
}