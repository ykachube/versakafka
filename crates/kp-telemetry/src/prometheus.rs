//! Prometheus exporter

use crate::TelemetryError;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use super::config::TelemetryConfig;

pub fn start_prometheus_exporter(config: &TelemetryConfig) -> Result<(), TelemetryError> {
    let addr: SocketAddr = format!("0.0.0.0:{}", config.prometheus_port).parse()
        .map_err(|e: std::net::AddrParseError| TelemetryError::Prometheus(e.to_string()))?;
    
    let builder = PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e: metrics_exporter_prometheus::BuildError| TelemetryError::Prometheus(e.to_string()))?;
    
    tracing::info!("Prometheus exporter started on {}", addr);
    Ok(())
}