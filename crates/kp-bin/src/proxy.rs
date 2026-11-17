//! Main proxy implementation

use crate::config::ProxyConfig;
use kp_proxy_core::server::run_server;
use kp_proxy_core::config::ServerConfig;
use crate::Result;
use std::sync::Arc;

pub async fn run_proxy(config: ProxyConfig) -> Result<()> {
    // Convert to ServerConfig
    let server_config = ServerConfig {
        proxy: config.proxy,
        telemetry: config.telemetry,
        metadata: config.metadata,
        filters: config.filters,
        wasm: config.wasm,
        event_id: config.event_id,
        breaker: config.breaker,
        channels: config.channels,
        wal: config.wal,
        spill: config.spill,
        reconciler: config.reconciler,
        crypto: config.crypto,
    };
    
    run_server(server_config).await?;
    Ok(())
}