//! Backpressure handling

use crate::{ProxyCoreError, Result};
use std::sync::Arc;
use tokio::sync::Semaphore;
use super::config::BackpressureConfig;

#[derive(Clone)]
pub struct Backpressure {
    permits: Arc<Semaphore>,
    config: BackpressureConfig,
}

impl Backpressure {
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(config.max_pending_requests)),
            config,
        }
    }

    pub async fn acquire(&self) -> Result<BackpressurePermit> {
        if !self.config.enable {
            return Ok(BackpressurePermit { _permit: None });
        }
        
        let permit = self.permits.clone().acquire_owned().await
            .map_err(|_| ProxyCoreError::Backpressure("Backpressure semaphore closed".to_string()))?;
        
        Ok(BackpressurePermit { _permit: Some(permit) })
    }
}

pub struct BackpressurePermit {
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}