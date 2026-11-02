//! Spill integration for proxy core

use crate::{ProxyCoreError, Result};
use kp_spill::{Uploader, SpillChannel, Replayer, SpillConfig, ManifestManager};
use kp_channel::{Channel, ChannelType, AckPolicy};
use kp_wal::{WalConfig, WalRecord};
use object_store::{ObjectStore, aws::AmazonS3Builder};
use std::sync::Arc;
use std::path::PathBuf;
use tracing::{info, warn};

pub struct SpillIntegration {
    config: SpillConfig,
    uploader: Option<Arc<Uploader>>,
    spill_channel: Option<Arc<SpillChannel>>,
    replayer: Option<Arc<Replayer>>,
    manifest_manager: ManifestManager,
}

impl SpillIntegration {
    pub fn new(config: SpillConfig) -> Result<Self> {
        let manifest_manager = ManifestManager::new(config.clone());
        
        let (uploader, replayer, spill_channel) = if config.enabled {
            // Create object store
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(&config.bucket)
                .with_region(config.region.as_deref().unwrap_or("us-east-1"));
            
            if let Some(endpoint) = &config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            
            if let Some(creds) = &config.credentials {
                builder = builder
                    .with_access_key_id(&creds.access_key_id)
                    .with_secret_access_key(&creds.secret_access_key);
                if let Some(token) = &creds.session_token {
                    builder = builder.with_token(token);
                }
            }
            
            let store = Arc::new(builder.build()
                .map_err(|e| ProxyCoreError::Spill(kp_spill::SpillError::ObjectStore(e)))?);
            
            let uploader = Arc::new(Uploader::new(config.clone())
                .map_err(|e| ProxyCoreError::Spill(e))?);
            
            let replayer = Arc::new(Replayer::new(config.clone(), store.clone()));
            
            let wal_config = WalConfig {
                dir: PathBuf::from("./wal/spill"),
                ..Default::default()
            };
            let spill_channel = Arc::new(SpillChannel::new(config.clone(), wal_config.dir.to_string_lossy().to_string()));
            
            (Some(uploader), Some(replayer), Some(spill_channel))
        } else {
            (None, None, None)
        };
        
        Ok(Self {
            config,
            uploader,
            spill_channel,
            replayer,
            manifest_manager,
        })
    }
    
    pub async fn upload_segment(&self, segment_path: &str, data: bytes::Bytes) -> Result<()> {
        if let Some(uploader) = &self.uploader {
            uploader.upload_segment(segment_path, data).await
                .map_err(|e| ProxyCoreError::Spill(e))?;
        }
        Ok(())
    }
    
    pub async fn upload_manifest(&self, manifest: &kp_spill::Manifest) -> Result<()> {
        if let Some(uploader) = &self.uploader {
            uploader.upload_manifest(manifest).await
                .map_err(|e| ProxyCoreError::Spill(e))?;
        }
        Ok(())
    }
    
    pub fn get_spill_channel(&self) -> Option<Arc<dyn Channel>> {
        self.spill_channel.clone().map(|c| c as Arc<dyn Channel>)
    }
    
    pub async fn replay(&self, channel: &dyn Channel) -> Result<()> {
        if let Some(replayer) = &self.replayer {
            replayer.replay(channel).await
                .map_err(|e| ProxyCoreError::Spill(e))?;
        }
        Ok(())
    }
    
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

impl Default for SpillIntegration {
    fn default() -> Self {
        let config = SpillConfig::default();
        Self::new(config).expect("Failed to create default spill integration")
    }
}