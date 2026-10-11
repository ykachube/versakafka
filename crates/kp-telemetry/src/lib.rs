//! kp-telemetry - Metrics, traces, and structured logging

pub mod metrics;
pub mod tracing_init;
pub mod prometheus;
pub mod config;

use thiserror::Error;
use anyhow::Error as AnyError;

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("metrics error: {0}")]
    Metrics(String),
    #[error("tracing error: {0}")]
    Tracing(String),
    #[error("prometheus error: {0}")]
    Prometheus(String),
    #[error("config error: {0}")]
    Config(#[from] AnyError),
}

pub type Result<T> = std::result::Result<T, TelemetryError>;