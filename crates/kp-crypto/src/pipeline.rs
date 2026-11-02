//! Crypto pipeline - integrates KMS, DEK cache, envelope encryption, and headers

use crate::{CryptoError, Result, config::CryptoConfig, config::CryptoHeaders};
use crate::kms::KmsClient;
use crate::dek_cache::DekCache;
use crate::envelope::{EnvelopeEncryption, EncryptedData};
use crate::headers::add_crypto_headers;
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{debug, info};

pub struct CryptoPipeline {
    config: CryptoConfig,
    kms_client: Arc<KmsClient>,
    dek_cache: Arc<DekCache>,
    envelope: Arc<EnvelopeEncryption>,
}

impl CryptoPipeline {
    pub async fn new(config: CryptoConfig) -> Result<Self> {
        let kms_client = Arc::new(KmsClient::new(config.kms.clone()).await?);
        let dek_cache = Arc::new(DekCache::new(config.dek.clone()));
        let envelope = Arc::new(EnvelopeEncryption::new(config.clone()));
        
        Ok(Self {
            config,
            kms_client,
            dek_cache,
            envelope,
        })
    }
    
    /// Encrypt a record value, returning encrypted data and headers to add
    pub async fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedRecord> {
        // Get or generate DEK
        let key_id = &self.config.kms.key_id;
        let dek = self.get_or_generate_dek(key_id).await?;
        
        // Encrypt with envelope encryption
        let encrypted = self.envelope.encrypt(plaintext, &dek)?;
        
        // Prepare headers
        let mut headers = Vec::new();
        let algorithm = match self.config.dek.algorithm {
            crate::config::DekAlgorithm::Aes256Gcm => "AES-256-GCM",
            crate::config::DekAlgorithm::Aes128Gcm => "AES-128-GCM",
        };
        
        add_crypto_headers(
            &mut headers,
            key_id,
            &encrypted.nonce,
            &encrypted.tag,
            algorithm,
            &self.config.headers,
        );
        
        // Get encrypted DEK for header
        let encrypted_dek = self.dek_cache.get_encrypted_dek(key_id);
        
        Ok(EncryptedRecord {
            ciphertext: encrypted.ciphertext,
            headers,
            encrypted_dek,
            key_id: key_id.clone(),
            nonce: encrypted.nonce,
            tag: encrypted.tag,
            algorithm: algorithm.to_string(),
        })
    }
    
    /// Decrypt a record value using headers
    pub async fn decrypt(&self, encrypted: &EncryptedRecord) -> Result<Vec<u8>> {
        // Get DEK from cache or decrypt from header
        let dek = self.get_dek_for_decrypt(&encrypted.key_id, encrypted.encrypted_dek.as_deref()).await?;
        
        // Decrypt
        let encrypted_data = crate::envelope::EncryptedData {
            ciphertext: encrypted.ciphertext.clone(),
            nonce: encrypted.nonce,
            tag: encrypted.tag.clone(),
        };
        
        self.envelope.decrypt(&encrypted_data, &dek)
    }
    
    async fn get_or_generate_dek(&self, key_id: &str) -> Result<Vec<u8>> {
        // Try cache first
        if let Some(dek) = self.dek_cache.get(key_id) {
            debug!("DEK cache hit for key: {}", key_id);
            return Ok(dek);
        }
        
        // Generate new DEK from KMS
        info!("Generating new DEK from KMS for key: {}", key_id);
        let key_spec = match self.config.dek.algorithm {
            crate::config::DekAlgorithm::Aes256Gcm => aws_sdk_kms::types::DataKeySpec::Aes256,
            crate::config::DekAlgorithm::Aes128Gcm => aws_sdk_kms::types::DataKeySpec::Aes128,
        };
        
        let plaintext_dek = self.kms_client.generate_dek(key_spec).await?;
        
        // Cache the plaintext DEK (ciphertext would come from KMS response in real impl)
        self.dek_cache.put(key_id.to_string(), plaintext_dek.clone(), plaintext_dek.clone());
        
        Ok(plaintext_dek)
    }
    
    async fn get_dek_for_decrypt(&self, key_id: &str, encrypted_dek: Option<&[u8]>) -> Result<Vec<u8>> {
        // Try cache first
        if let Some(dek) = self.dek_cache.get(key_id) {
            debug!("DEK cache hit for decrypt, key: {}", key_id);
            return Ok(dek);
        }
        
        // Decrypt from encrypted DEK header
        if let Some(ct) = encrypted_dek {
            info!("Decrypting DEK from header for key: {}", key_id);
            let dek = self.kms_client.decrypt_dek(ct).await?;
            self.dek_cache.put(key_id.to_string(), dek.clone(), ct.to_vec());
            return Ok(dek);
        }
        
        Err(CryptoError::Envelope("No DEK available for decryption".to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct EncryptedRecord {
    pub ciphertext: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub encrypted_dek: Option<Vec<u8>>,
    pub key_id: String,
    pub nonce: [u8; 12],
    pub tag: Vec<u8>,
    pub algorithm: String,
}