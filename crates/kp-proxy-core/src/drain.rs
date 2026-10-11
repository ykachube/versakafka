//! Graceful drain

use crate::{ProxyCoreError, Result};
use tokio::signal;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::Duration;

#[derive(Default)]
pub struct DrainState {
    pub draining: bool,
    pub in_flight: usize,
}

pub struct DrainManager {
    state: Arc<RwLock<DrainState>>,
}

impl DrainManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(DrainState::default())),
        }
    }

    pub async fn wait_for_drain(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            let state = self.state.read();
            if !state.draining && state.in_flight == 0 {
                return Ok(());
            }
            drop(state);
            
            if start.elapsed() > timeout {
                return Err(ProxyCoreError::Drain("Drain timeout".to_string()));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn start_drain(&self) {
        self.state.write().draining = true;
    }

    pub fn increment_in_flight(&self) {
        self.state.write().in_flight += 1;
    }

    pub fn decrement_in_flight(&self) {
        self.state.write().in_flight = self.state.read().in_flight.saturating_sub(1);
    }
}