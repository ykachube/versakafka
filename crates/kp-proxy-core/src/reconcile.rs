//! Reconciliation integration for proxy core

use crate::{ProxyCoreError, Result};
use kp_reconciler::{ReconcilerChecker, GapReplayer, ReconcilerConfig, ReconciliationReport, Gap};
use kp_spill::Replayer;
use kp_channel::Channel;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{info, warn};

pub struct ReconciliationIntegration {
    config: ReconcilerConfig,
    checker: Arc<ReconcilerChecker>,
    replayer: Arc<GapReplayer>,
}

impl ReconciliationIntegration {
    pub fn new(config: ReconcilerConfig) -> Self {
        let checker = Arc::new(ReconcilerChecker::new(config.clone()));
        let replayer = Arc::new(GapReplayer::new(config.clone()));
        
        Self { config, checker, replayer }
    }
    
    pub async fn check(&self, channels: &[Arc<dyn Channel>]) -> Result<ReconciliationReport> {
        self.checker.check(channels).await
            .map_err(|e| ProxyCoreError::Reconciler(e))
    }
    
    pub async fn replay_gaps(&self, gaps: &[Gap], spill_replayer: &Replayer, channel: &dyn Channel) -> Result<()> {
        self.replayer.replay_gaps(gaps, spill_replayer, channel).await
            .map_err(|e| ProxyCoreError::Reconciler(e))
    }
    
    pub async fn start_background(&self, channels: Vec<Arc<dyn Channel>>, spill_replayer: Arc<Replayer>) {
        if !self.config.enabled {
            return;
        }
        
        let checker = self.checker.clone();
        let replayer = self.replayer.clone();
        let interval = self.config.interval;
        let auto_replay = self.config.auto_replay;
        
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                info!("Running periodic reconciliation check");
                match checker.check(&channels).await {
                    Ok(report) => {
                        if !report.matched {
                            warn!("Reconciliation found gaps: {}, discrepancies: {}", 
                                report.gaps.len(), report.discrepancies.len());
                            
                            if auto_replay && !report.gaps.is_empty() {
                                // Replay gaps for each channel
                                for channel in &channels {
                                    if let Err(e) = replayer.replay_gaps(&report.gaps, &spill_replayer, channel.as_ref()).await {
                                        warn!("Failed to replay gaps: {}", e);
                                    }
                                }
                            }
                        } else {
                            info!("Reconciliation: all channels matched");
                        }
                    }
                    Err(e) => {
                        warn!("Reconciliation check failed: {}", e);
                    }
                }
            }
        });
    }
}

impl Default for ReconciliationIntegration {
    fn default() -> Self {
        let config = ReconcilerConfig::default();
        Self::new(config)
    }
}