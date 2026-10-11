//! kp-testkit - Test harness for Resilient Kafka Proxy
//! 
//! Provides fake broker, cluster fixtures, ledger, checker, and scenario runner.

pub mod fake_broker;
pub mod cluster_fixture;
pub mod fake_arc;
pub mod fake_s3;
pub mod fs_failures;
pub mod golden_corpus;
pub mod load_generator;
pub mod ledger;
pub mod checker;
pub mod scenario_runner;
pub mod config;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TestKitError {
    #[error("fake broker error: {0}")]
    FakeBroker(String),
    #[error("cluster fixture error: {0}")]
    ClusterFixture(String),
    #[error("fake arc error: {0}")]
    FakeArc(String),
    #[error("fake s3 error: {0}")]
    FakeS3(String),
    #[error("ledger error: {0}")]
    Ledger(String),
    #[error("checker error: {0}")]
    Checker(String),
    #[error("scenario error: {0}")]
    Scenario(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, TestKitError>;