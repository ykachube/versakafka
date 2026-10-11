//! Scenario runner for chaos testing

use crate::{TestKitError, checker::{Checker, Invariant, CheckReport}, ledger::Ledger};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use rand::{SeedableRng, rngs::StdRng};

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
    rng: StdRng,
}

impl ScenarioRunner {
    pub fn new(scenario: Scenario) -> Self {
        let invariants = scenario.invariants.clone();
        let rng = StdRng::seed_from_u64(scenario.seed);
        Self {
            scenario,
            ledger: Ledger::new(),
            checker: Checker::new(invariants),
            rng,
        }
    }

    pub fn with_ledger(mut self, ledger: Ledger) -> Self {
        self.ledger = ledger;
        self
    }

    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    pub fn ledger_mut(&mut self) -> &mut Ledger {
        &mut self.ledger
    }

    pub async fn run(&mut self) -> Result<ScenarioResult, TestKitError> {
        tracing::info!("Running scenario: {} (seed: {})", self.scenario.name, self.scenario.seed);
        
        // Execute load generation
        self.run_load().await?;
        
        // Execute failure injections
        self.inject_failures().await?;
        
        // Wait for scenario duration
        tokio::time::sleep(self.scenario.duration).await;
        
        // Run checks
        let report = self.checker.check(&self.ledger, &[], &[], &[])?;
        
        Ok(ScenarioResult {
            scenario_name: self.scenario.name.clone(),
            seed: self.scenario.seed,
            check_report: report.clone(),
            passed: report.failures.is_empty(),
        })
    }

    async fn run_load(&mut self) -> Result<(), TestKitError> {
        // Simulate load generation
        // In real implementation, this would use LoadGenerator to send messages
        tracing::info!("Generating load: {} msg/s for {:?}", self.scenario.load.throughput, self.scenario.duration);
        Ok(())
    }

    async fn inject_failures(&self) -> Result<(), TestKitError> {
        for failure in &self.scenario.failures {
            tracing::info!("Injecting failure: {:?} on {} at {:?}", failure.failure_type, failure.target, failure.start_time);
            // In real implementation, this would trigger actual failures
            // e.g., kill broker, add latency, etc.
        }
        Ok(())
    }

    pub fn scenario(&self) -> &Scenario {
        &self.scenario
    }
}

#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub scenario_name: String,
    pub seed: u64,
    pub check_report: CheckReport,
    pub passed: bool,
}

impl ScenarioResult {
    pub fn print_summary(&self) {
        println!("Scenario: {} (seed: {})", self.scenario_name, self.seed);
        println!("  Result: {}", if self.passed { "PASSED" } else { "FAILED" });
        println!("  {}", self.check_report.summary());
        for failure in &self.check_report.failures {
            println!("  FAILURE: {}", failure);
        }
    }
}

/// Pre-defined chaos scenarios from the implementation plan
pub mod chaos_scenarios {
    use super::*;
    use std::time::Duration;

    /// Scenario 1: Broker crash under load
    pub fn broker_crash(seed: u64) -> Scenario {
        Scenario {
            name: "broker_crash".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::KillBroker,
                    target: "primary-broker".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({}),
                },
            ],
            invariants: vec![Invariant::I1NoLostAcked, Invariant::I2StableEventId, Invariant::I5ChannelIsolation],
        }
    }

    /// Scenario 2: Network partition
    pub fn network_partition(seed: u64) -> Scenario {
        Scenario {
            name: "network_partition".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::NetworkPartition,
                    target: "primary-cluster".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({}),
                },
            ],
            invariants: vec![Invariant::I1NoLostAcked, Invariant::I2StableEventId],
        }
    }

    /// Scenario 3: Slow broker
    pub fn slow_broker(seed: u64) -> Scenario {
        Scenario {
            name: "slow_broker".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::SlowBroker,
                    target: "primary-broker".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({"latency_ms": 5000}),
                },
            ],
            invariants: vec![Invariant::I5ChannelIsolation],
        }
    }

    /// Scenario 4: One channel unavailable
    pub fn channel_unavailable(seed: u64) -> Scenario {
        Scenario {
            name: "channel_unavailable".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::KillChannel,
                    target: "secondary-channel".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({}),
                },
            ],
            invariants: vec![Invariant::I1NoLostAcked, Invariant::I5ChannelIsolation, Invariant::I7MemoryBounded],
        }
    }

    /// Scenario 5: S3 unavailable
    pub fn s3_unavailable(seed: u64) -> Scenario {
        Scenario {
            name: "s3_unavailable".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::S3Unavailable,
                    target: "spill-bucket".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({"error_rate": 1.0}),
                },
            ],
            invariants: vec![Invariant::I1NoLostAcked, Invariant::I7MemoryBounded],
        }
    }

    /// Scenario 6: Disk full
    pub fn disk_full(seed: u64) -> Scenario {
        Scenario {
            name: "disk_full".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::DiskFull,
                    target: "wal".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({}),
                },
            ],
            invariants: vec![Invariant::I1NoLostAcked, Invariant::I7MemoryBounded],
        }
    }

    /// Scenario 7: Proxy restart during replay
    pub fn proxy_restart_during_replay(seed: u64) -> Scenario {
        Scenario {
            name: "proxy_restart_during_replay".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::ProxyRestart,
                    target: "proxy".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({}),
                },
            ],
            invariants: vec![Invariant::I1NoLostAcked, Invariant::I2StableEventId],
        }
    }

    /// Scenario 8: ARC flapping
    pub fn arc_flapping(seed: u64) -> Scenario {
        Scenario {
            name: "arc_flapping".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::ArcFlapping,
                    target: "arc".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({"interval_secs": 5}),
                },
            ],
            invariants: vec![Invariant::I6FailStatic],
        }
    }

    /// Scenario 9: Pod restart under load
    pub fn pod_restart(seed: u64) -> Scenario {
        Scenario {
            name: "pod_restart".to_string(),
            seed,
            duration: Duration::from_secs(60),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::ProxyRestart,
                    target: "proxy".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({}),
                },
            ],
            invariants: vec![Invariant::I1NoLostAcked, Invariant::I8GracefulDrain],
        }
    }

    /// Scenario 10: Combination of multiple failures
    pub fn combined_failures(seed: u64) -> Scenario {
        Scenario {
            name: "combined_failures".to_string(),
            seed,
            duration: Duration::from_secs(120),
            load: LoadConfig {
                throughput: 10000,
                batch_size: 100,
                num_producers: 10,
            },
            failures: vec![
                FailureConfig {
                    failure_type: FailureType::KillBroker,
                    target: "primary-broker".to_string(),
                    start_time: Duration::from_secs(10),
                    duration: Duration::from_secs(30),
                    params: serde_json::json!({}),
                },
                FailureConfig {
                    failure_type: FailureType::SlowBroker,
                    target: "secondary-broker".to_string(),
                    start_time: Duration::from_secs(20),
                    duration: Duration::from_secs(40),
                    params: serde_json::json!({"latency_ms": 2000}),
                },
                FailureConfig {
                    failure_type: FailureType::S3Unavailable,
                    target: "spill-bucket".to_string(),
                    start_time: Duration::from_secs(30),
                    duration: Duration::from_secs(20),
                    params: serde_json::json!({"error_rate": 0.5}),
                },
            ],
            invariants: vec![
                Invariant::I1NoLostAcked, 
                Invariant::I2StableEventId, 
                Invariant::I5ChannelIsolation,
                Invariant::I7MemoryBounded,
                Invariant::I8GracefulDrain,
            ],
        }
    }

    /// Get all chaos scenarios
    pub fn all() -> Vec<fn(u64) -> Scenario> {
        vec![
            broker_crash,
            network_partition,
            slow_broker,
            channel_unavailable,
            s3_unavailable,
            disk_full,
            proxy_restart_during_replay,
            arc_flapping,
            pod_restart,
            combined_failures,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_scenario_runner() {
        // Use a minimal scenario for testing
        let scenario = Scenario {
            name: "test".to_string(),
            seed: 42,
            duration: Duration::from_millis(100),
            load: LoadConfig {
                throughput: 1000,
                batch_size: 10,
                num_producers: 1,
            },
            failures: vec![],
            invariants: vec![Invariant::I1NoLostAcked],
        };
        let mut runner = ScenarioRunner::new(scenario);
        let result = runner.run().await.unwrap();
        // Should pass since no actual failures are injected in test
        assert!(result.passed);
    }
}