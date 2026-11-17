use kp_bin::config::ProxyConfig;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> kp_bin::Result<()> {
    // Initialize tracing
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = ProxyConfig::load()?;
    tracing::info!("Starting Kafka Proxy with config: {:?}", config);

    // Start proxy server
    kp_bin::proxy::run_proxy(config).await?;

    Ok(())
}