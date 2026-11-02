//! Crypto integration for proxy core

use crate::{ProxyCoreError, Result};
use kp_crypto::{CryptoPipeline, CryptoConfig, pipeline::EncryptedRecord};
use std::sync::Arc;
use tracing::{info, warn};

pub struct CryptoIntegration {
    pipeline: Option<Arc<CryptoPipeline>>,
    config: CryptoConfig,
}

impl CryptoIntegration {
    pub async fn new(config: CryptoConfig) -> Result<Self> {
        let pipeline = if config.enabled {
            Some(Arc::new(CryptoPipeline::new(config.clone()).await?))
        } else {
            None
        };
        
        Ok(Self { pipeline, config })
    }
    
    pub async fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedRecord> {
        match &self.pipeline {
            Some(pipeline) => pipeline.encrypt(plaintext).await
                .map_err(|e| ProxyCoreError::Crypto(e)),
            None => Ok(EncryptedRecord {
                ciphertext: plaintext.to_vec(),
                headers: Vec::new(),
                encrypted_dek: None,
                key_id: String::new(),
                nonce: [0u8; 12],
                tag: Vec::new(),
                algorithm: String::new(),
            }),
        }
    }
    
    pub async fn decrypt(&self, encrypted: &EncryptedRecord) -> Result<Vec<u8>> {
        match &self.pipeline {
            Some(pipeline) => pipeline.decrypt(encrypted).await
                .map_err(|e| ProxyCoreError::Crypto(e)),
            None => Ok(encrypted.ciphertext.clone()),
        }
    }
    
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
    
    pub fn get_config(&self) -> &CryptoConfig {
        &self.config
    }
}

impl Default for CryptoIntegration {
    fn default() -> Self {
        let config = CryptoConfig::default();
        Self { pipeline: None, config }
    }
}