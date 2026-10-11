//! Metrics

use crate::TelemetryError;
use metrics::{counter, gauge, histogram};
use std::time::Duration;

pub fn init_metrics() {
    // Metrics are auto-registered on first use in metrics 0.22+
}

pub fn record_request(latency: Duration, added_latency: Duration) {
    histogram!("kp_proxy_request_latency_seconds").record(latency.as_secs_f64());
    histogram!("kp_proxy_added_latency_seconds").record(added_latency.as_secs_f64());
    counter!("kp_proxy_requests_total").increment(1);
}

pub fn record_error() {
    counter!("kp_proxy_errors_total").increment(1);
}

pub fn record_connection(active: bool) {
    if active {
        gauge!("kp_proxy_active_connections").increment(1.0);
    } else {
        gauge!("kp_proxy_active_connections").decrement(1.0);
    }
}

pub fn record_breaker_state(state: u8) {
    gauge!("kp_proxy_breaker_state").set(state as f64);
}