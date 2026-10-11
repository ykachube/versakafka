//! Proxy server

use crate::{ProxyCoreError, Result, connection::Connection, framing::KafkaFrameCodec, drain::DrainManager, backpressure::Backpressure};
use super::config::ProxyConfig;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use std::sync::Arc;
use tokio::signal;
use kp_metadata::MetadataRewriter;
use kp_metadata::config::MetadataConfig;
use std::time::Duration;
use std::collections::HashMap;

pub async fn run_server(config: ProxyConfig) -> Result<()> {
    // Initialize metadata rewriter
    let metadata_config = MetadataConfig {
        routing_mode: kp_metadata::config::RoutingMode::Sni,
        broker_map: HashMap::new(),
        rewrite_enabled: true,
    };
    let metadata_rewriter = MetadataRewriter::new(metadata_config);
    
    // Initialize backpressure
    let backpressure = Backpressure::new(config.backpressure.clone());
    
    let listener = TcpListener::bind(&config.listen_addr).await
        .map_err(|e| ProxyCoreError::Io(e))?;
    
    let config = Arc::new(config);
    let drain = Arc::new(DrainManager::new());
    let metadata_rewriter = Arc::new(metadata_rewriter);
    let backpressure = Arc::new(backpressure);
    
    // Handle shutdown signals
    let drain_clone = drain.clone();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        tracing::info!("Shutdown signal received, starting drain");
        drain_clone.start_drain();
    });
    
    tracing::info!("Server listening on {}", config.listen_addr);
    
    loop {
        // Check if draining
        if drain.is_draining() {
            tracing::info!("Draining, not accepting new connections");
            break;
        }
        
        let (stream, addr) = tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => (stream, addr),
                    Err(e) => {
                        tracing::error!("Accept error: {}", e);
                        continue;
                    }
                }
            }
        };
        
        // Check connection limit
        let current_connections = drain.in_flight_count();
        if current_connections >= config.max_connections {
            tracing::warn!("Max connections reached, rejecting connection from {}", addr);
            continue;
        }
        
        let config = config.clone();
        let drain = drain.clone();
        let metadata_rewriter = metadata_rewriter.clone();
        let backpressure = backpressure.clone();
        
        drain.increment_in_flight();
        
        tokio::spawn(async move {
            let mut connection = Connection::new(stream, config, metadata_rewriter.clone(), (*backpressure).clone());
            let _ = connection.handle().await;
            drain.decrement_in_flight();
            tracing::debug!("Connection from {} closed", addr);
        });
    }
    
    // Wait for in-flight requests to complete
    let _ = drain.wait_for_drain(Duration::from_secs(30)).await;
    tracing::info!("Graceful shutdown complete");
    
    Ok(())
}