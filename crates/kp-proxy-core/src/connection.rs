//! Connection handling

use crate::{ProxyCoreError, Result};
use tokio::net::TcpStream;
use std::sync::Arc;

pub struct Connection {
    stream: TcpStream,
    config: Arc<super::config::ProxyConfig>,
}

impl Connection {
    pub fn new(stream: TcpStream, config: Arc<super::config::ProxyConfig>) -> Self {
        Self { stream, config }
    }

    pub async fn handle(&mut self) -> Result<()> {
        // TODO: Handle connection
        Ok(())
    }
}