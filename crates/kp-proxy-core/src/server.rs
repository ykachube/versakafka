//! Proxy server

use crate::{ProxyCoreError, Result, connection::Connection, framing::KafkaFrameCodec, drain::DrainManager, backpressure::Backpressure, breaker::BreakerIntegration, crypto::CryptoIntegration};
use super::config::ServerConfig;
use tokio::net::TcpListener;
use std::sync::Arc;
use tokio::signal;
use kp_metadata::MetadataRewriter;
use kp_filters::{FilterChain, hooks::WasmHost};
use kp_eventid::EventIdGenerator;
use kp_channel::{AckPolicyEngine, Channel, KafkaChannel, ChannelConfig, KafkaChannelConfig};
use kp_channel::config::ChannelType as ConfigChannelType;
use kp_wasm::WasmHost as WasmHostImpl;
use std::time::Duration;
use std::collections::HashMap;

pub async fn run_server(config: ServerConfig) -> Result<()> {
    // Initialize telemetry (tracing already initialized by binary)
    tracing::info!("Starting proxy with config: listen_addr={}, channels={}", config.proxy.listen_addr, config.channels.len());
    
    // Initialize metadata rewriter
    let metadata_rewriter = MetadataRewriter::new(config.metadata);
    
    // Initialize backpressure
    let backpressure = Backpressure::new(config.proxy.backpressure.clone());
    
    // Initialize filter chain with WASM host if enabled
    let wasm_host: Option<Arc<dyn WasmHost>> = if config.wasm.enabled {
        let wasm_host = Arc::new(WasmHostImpl::new(config.wasm)?);
        wasm_host.load_plugins().await?;
        Some(wasm_host)
    } else {
        None
    };
    
    let event_id_config = config.event_id.clone();
    let filter_chain = Arc::new(FilterChain::new(
        config.filters,
        event_id_config,
        wasm_host,
    ));
    
    // Initialize event ID generator
    let event_id_generator = Arc::new(EventIdGenerator::new(config.event_id));
    
    // Initialize breaker
    let breaker_integration = Arc::new(BreakerIntegration::new(config.breaker)?);
    breaker_integration.start().await?;
    let breaker_manager = breaker_integration.get_manager();
    
    // Initialize channels from config
    let mut channels: Vec<Arc<dyn Channel>> = Vec::new();
    for channel_config in &config.channels {
        match channel_config.channel_type {
            ConfigChannelType::Kafka => {
                if let Some(kafka_config) = &channel_config.kafka {
                    let kafka_channel = Arc::new(KafkaChannel::new(
                        channel_config.clone(),
                        kafka_config.clone(),
                    ));
                    channels.push(kafka_channel as Arc<dyn Channel>);
                }
            }
            ConfigChannelType::S3 => {
                // Spill channel will be created separately with WAL path
                // For now, skip - need WAL integration
            }
            ConfigChannelType::Wal => {
                // WAL is handled separately, not as a fan-out channel
            }
            _ => {
                tracing::warn!("Channel type {:?} not yet implemented", channel_config.channel_type);
            }
        }
    }
    
    // If no channels configured, create default primary Kafka channel
    if channels.is_empty() {
        tracing::warn!("No channels configured, creating default primary Kafka channel");
        let default_channel_config = ChannelConfig::default();
        let kafka_config = KafkaChannelConfig {
            bootstrap_servers: "localhost:9092".to_string(),
            client_id: "kp-proxy".to_string(),
            producer_config: serde_json::json!({}),
            topic_mapping: HashMap::new(),
        };
        let kafka_channel = Arc::new(KafkaChannel::new(default_channel_config, kafka_config));
        channels.push(kafka_channel as Arc<dyn Channel>);
    }
    
    let ack_policy_engine = Some(AckPolicyEngine::new(channels));
    
    // Initialize crypto
    let crypto_integration = Arc::new(CryptoIntegration::new(config.crypto).await?);
    
    let listener = TcpListener::bind(&config.proxy.listen_addr).await
        .map_err(|e| ProxyCoreError::Io(e))?;
    
    let config = Arc::new(config.proxy);
    let drain = Arc::new(DrainManager::new());
    let metadata_rewriter = Arc::new(metadata_rewriter);
    let backpressure = Arc::new(backpressure);
    let filter_chain = filter_chain;
    let event_id_generator = event_id_generator;
    let breaker_manager = breaker_manager;
    let ack_policy_engine = ack_policy_engine;
    let crypto_integration = crypto_integration;
    
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
        let crypto_integration = crypto_integration.clone();
        
        drain.increment_in_flight();
        
        tokio::spawn(async move {
            let mut connection = Connection::new(
                stream, config, metadata_rewriter, backpressure,
                filter_chain, event_id_generator, breaker_manager, ack_policy_engine, crypto_integration
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