//! Proxy server

use crate::{ProxyCoreError, Result, connection::Connection, framing::KafkaFrameCodec, drain::DrainManager, backpressure::Backpressure, breaker::BreakerIntegration};
use super::config::ProxyConfig;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use std::sync::Arc;
use tokio::signal;
use kp_metadata::MetadataRewriter;
use kp_metadata::config::MetadataConfig;
use kp_filters::{FilterChain, FiltersConfig};
use kp_eventid::{EventIdGenerator, EventIdConfig};
use kp_breaker::BreakerConfig;
use kp_channel::{AckPolicyEngine, Channel, KafkaChannel, ChannelConfig, KafkaChannelConfig, AckPolicy};
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
    
    // Initialize filter chain
    let filters_config = FiltersConfig::default();
    let event_id_config = EventIdConfig::default();
    let filter_chain = Arc::new(FilterChain::new(filters_config, event_id_config, None));
    
    // Initialize event ID generator
    let event_id_config = EventIdConfig::default();
    let event_id_generator = Arc::new(EventIdGenerator::new(event_id_config));
    
    // Initialize breaker
    let breaker_config = BreakerConfig::default();
    let breaker_integration = Arc::new(BreakerIntegration::new(breaker_config)?);
    breaker_integration.start().await?;
    let breaker_manager = breaker_integration.get_manager();
    
    // Initialize channels and ack policy engine
    let channel_config = ChannelConfig::default();
    let kafka_config = KafkaChannelConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        client_id: "kp-proxy".to_string(),
        producer_config: serde_json::json!({}),
        topic_mapping: HashMap::new(),
    };
    let kafka_channel = Arc::new(KafkaChannel::new(channel_config, kafka_config));
    let ack_policy_engine = Some(AckPolicyEngine::new(vec![kafka_channel]));
    
    let listener = TcpListener::bind(&config.listen_addr).await
        .map_err(|e| ProxyCoreError::Io(e))?;
    
    let config = Arc::new(config);
    let drain = Arc::new(DrainManager::new());
    let metadata_rewriter = Arc::new(metadata_rewriter);
    let backpressure = Arc::new(backpressure);
    let filter_chain = filter_chain;
    let event_id_generator = event_id_generator;
    let breaker_manager = breaker_manager;
    let ack_policy_engine = ack_policy_engine;
    
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
        let filter_chain = filter_chain.clone();
        let event_id_generator = event_id_generator.clone();
        let breaker_manager = breaker_manager.clone();
        let ack_policy_engine = ack_policy_engine.clone();
        
        drain.increment_in_flight();
        
        tokio::spawn(async move {
            let mut connection = Connection::new(
                stream, config, metadata_rewriter, backpressure,
                filter_chain, event_id_generator, breaker_manager, ack_policy_engine
            );
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