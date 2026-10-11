//! Tracing initialization

use crate::TelemetryError;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};
use super::config::{TelemetryConfig, LogFormat};

pub fn init_tracing(config: &TelemetryConfig) -> Result<(), TelemetryError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    
    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true);
    
    let fmt_layer: Box<dyn Layer<_> + Send + Sync> = match config.log_format {
        LogFormat::Json => Box::new(fmt_layer.json()),
        LogFormat::Text => Box::new(fmt_layer),
    };
    
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init()
        .map_err(|e| TelemetryError::Tracing(e.to_string()))?;
    
    Ok(())
}