//! Main proxy implementation

use crate::config::ProxyConfig;
use kp_proxy_core::server::run_server;
use crate::Result;

pub async fn run_proxy(config: ProxyConfig) -> Result<()> {
    run_server(config.proxy).await?;
    Ok(())
}