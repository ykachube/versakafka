//! Fan-out logic

use crate::{ChannelError, Result, trait_channel::{Channel, ChannelBatch}};
use std::sync::Arc;

pub struct FanoutEngine {
    channels: Vec<Arc<dyn Channel>>,
}

impl FanoutEngine {
    pub fn new(channels: Vec<Arc<dyn Channel>>) -> Self {
        Self { channels }
    }

    pub async fn fanout(&self, batch: &ChannelBatch) -> Result<()> {
        // Clone the batch reference for each channel (zero-copy)
        let mut handles = Vec::new();
        
        for channel in &self.channels {
            let ch = channel.clone();
            let b = batch.clone(); // This clones the Arc, not the data
            handles.push(tokio::spawn(async move {
                ch.send(b).await
            }));
        }
        
        for handle in handles {
            handle.await.map_err(|e| ChannelError::Fanout(e.to_string()))??;
        }
        
        Ok(())
    }
}