//! KMS integration

use crate::{CryptoError, Result, config::KmsConfig};
use aws_sdk_kms::{Client, types::DataKeySpec, Config as KmsConfigBuilder};

pub struct KmsClient {
    config: KmsConfig,
    client: Client,
}

impl KmsClient {
    pub async fn new(config: KmsConfig) -> Result<Self> {
        let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        
        let mut builder = KmsConfigBuilder::builder().region(aws_sdk_kms::config::Region::new(config.region.clone()));
        
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        
        let client = Client::from_conf(builder.build());
        
        Ok(Self {
            config,
            client,
        })
    }

    pub async fn generate_dek(&self, key_spec: DataKeySpec) -> Result<Vec<u8>> {
        let output = self.client.generate_data_key()
            .key_id(&self.config.key_id)
            .key_spec(key_spec)
            .send()
            .await
            .map_err(|e| CryptoError::Kms(e.to_string()))?;
        
        output.plaintext()
            .map(|p| p.as_ref().to_vec())
            .ok_or_else(|| CryptoError::Kms("No plaintext in response".to_string()))
    }

    pub async fn decrypt_dek(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let output = self.client.decrypt()
            .ciphertext_blob(ciphertext.into())
            .send()
            .await
            .map_err(|e| CryptoError::Kms(e.to_string()))?;
        
        output.plaintext()
            .map(|p| p.as_ref().to_vec())
            .ok_or_else(|| CryptoError::Kms("No plaintext in response".to_string()))
    }
}