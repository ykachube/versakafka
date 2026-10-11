//! Proxy server

use crate::{ProxyCoreError, Result, connection::Connection, framing::KafkaFrameCodec, drain::DrainManager};
use super::config::ProxyConfig;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use std::sync::Arc;
use tokio::signal;

pub async fn run_server(config: ProxyConfig) -> Result<()> {
    let listener = TcpListener::bind(&config.listen_addr).await
        .map_err(|e| ProxyCoreError::Io(e))?;
    
    let config = Arc::new(config);
    let drain = Arc::new(DrainManager::new());
    
    // Handle shutdown signals
    let drain_clone = drain.clone();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        tracing::info!("Shutdown signal received, starting drain");
        drain_clone.start_drain();
    });
    
    tracing::info!("Server listening on {}", config.listen_addr);
    
    loop {
        let (stream, addr) = listener.accept().await
            .map_err(|e| ProxyCoreError::Io(e))?;
        
        let config = config.clone();
        let drain = drain.clone();
        
        drain.increment_in_flight();
        
        tokio::spawn(async move {
            let mut connection = Connection::new(stream, config);
            let _ = connection.handle().await;
            drain.decrement_in_flight();
            tracing::debug!("Connection from {} closed", addr);
        });
    }
}