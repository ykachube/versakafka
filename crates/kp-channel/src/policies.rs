//! Ack policies

use crate::{ChannelError, Result, trait_channel::{AckPolicy, Channel, ChannelBatch, SendResult}};
use std::sync::Arc;

#[derive(Clone)]
pub struct AckPolicyEngine {
    channels: Vec<Arc<dyn Channel>>,
}

impl AckPolicyEngine {
    pub fn new(channels: Vec<Arc<dyn Channel>>) -> Self {
        Self { channels }
    }

    pub async fn send(&self, batch: ChannelBatch) -> Result<Vec<SendResult>> {
        if self.channels.is_empty() {
            return Err(ChannelError::Policy("No channels configured".to_string()));
        }

        // Determine primary channel
        let primary = &self.channels[0];
        let policy = primary.ack_policy();

        match policy {
            AckPolicy::PrimarySyncSecondaryAsync => {
                // Send to primary synchronously
                let primary_result = primary.send(batch.clone()).await?;
                
                // Send to secondaries asynchronously
                for channel in &self.channels[1..] {
                    let ch = channel.clone();
                    let b = batch.clone();
                    tokio::spawn(async move {
                        let _ = ch.send(b).await;
                    });
                }
                
                Ok(vec![primary_result])
            }
            AckPolicy::Any => {
                // Send to all, return first success
                let mut handles = Vec::new();
                for channel in &self.channels {
                    let ch = channel.clone();
                    let b = batch.clone();
                    handles.push(tokio::spawn(async move {
                        (ch.name().to_string(), ch.send(b).await)
                    }));
                }
                
                let mut results = Vec::new();
                for handle in handles {
                    let (name, result) = handle.await.map_err(|e| ChannelError::Policy(e.to_string()))?;
                    let is_ok = result.is_ok();
                    results.push((name, result));
                    if is_ok {
                        // Return early on first success
                        break;
                    }
                }
                
                Ok(results.into_iter().map(|(_, r)| r.unwrap_or_else(|e| SendResult {
                    success: false,
                    offset: None,
                    error: Some(e.to_string()),
                })).collect())
            }
            AckPolicy::All => {
                // Wait for all to succeed
                let mut handles = Vec::new();
                for channel in &self.channels {
                    let ch = channel.clone();
                    let b = batch.clone();
                    handles.push(tokio::spawn(async move {
                        ch.send(b).await
                    }));
                }
                
                let mut results = Vec::new();
                for handle in handles {
                    results.push(handle.await.map_err(|e| ChannelError::Policy(e.to_string()))??);
                }
                
                Ok(results)
            }
            AckPolicy::AllOrSpill => {
                // TODO: Implement with spill fallback
                self.send_all(batch).await
            }
        }
    }

    async fn send_all(&self, batch: ChannelBatch) -> Result<Vec<SendResult>> {
        let mut handles = Vec::new();
        for channel in &self.channels {
            let ch = channel.clone();
            let b = batch.clone();
            handles.push(tokio::spawn(async move {
                ch.send(b).await
            }));
        }
        
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.map_err(|e| ChannelError::Policy(e.to_string()))??);
        }
        
        Ok(results)
    }
}