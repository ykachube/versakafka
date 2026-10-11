use kp_bin::config::ProxyConfig;
use tracing_subscriber::{EnvFilter, fmt};

fn main() -> kp_bin::Result<()> {
    // Initialize tracing
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = ProxyConfig::load()?;
    tracing::info!("Starting Kafka Proxy with config: {:?}", config);

    // TODO: Start proxy server
    tracing::info!("Proxy started (placeholder)");

    Ok(())
}