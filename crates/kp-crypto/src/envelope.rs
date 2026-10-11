//! Envelope encryption

use crate::{CryptoError, Result, config::CryptoConfig, config::DekConfig, config::DekAlgorithm};
use aes_gcm::{Aes256Gcm, Aes128Gcm, Key, Nonce, aead::{Aead, KeyInit}};
use rand::rngs::ThreadRng;
use rand::RngCore;

pub struct EnvelopeEncryption {
    config: CryptoConfig,
}

impl EnvelopeEncryption {
    pub fn new(config: CryptoConfig) -> Self {
        Self { config }
    }

    pub fn encrypt(&self, plaintext: &[u8], dek: &[u8]) -> Result<EncryptedData> {
        let nonce = self.generate_nonce();
        
        match self.config.dek.algorithm {
            DekAlgorithm::Aes256Gcm => {
                let key = Key::<Aes256Gcm>::from_slice(dek);
                let cipher = Aes256Gcm::new(key);
                let ciphertext = cipher.encrypt(Nonce::from_slice(&nonce), plaintext)
                    .map_err(|e| CryptoError::Envelope(e.to_string()))?;
                Ok(EncryptedData { ciphertext, nonce, tag: vec![] })
            }
            DekAlgorithm::Aes128Gcm => {
                let key = Key::<Aes128Gcm>::from_slice(dek);
                let cipher = Aes128Gcm::new(key);
                let ciphertext = cipher.encrypt(Nonce::from_slice(&nonce), plaintext)
                    .map_err(|e| CryptoError::Envelope(e.to_string()))?;
                Ok(EncryptedData { ciphertext, nonce, tag: vec![] })
            }
        }
    }

    pub fn decrypt(&self, encrypted: &EncryptedData, dek: &[u8]) -> Result<Vec<u8>> {
        match self.config.dek.algorithm {
            DekAlgorithm::Aes256Gcm => {
                let key = Key::<Aes256Gcm>::from_slice(dek);
                let cipher = Aes256Gcm::new(key);
                cipher.decrypt(Nonce::from_slice(&encrypted.nonce), encrypted.ciphertext.as_ref())
                    .map_err(|e| CryptoError::Envelope(e.to_string()))
            }
            DekAlgorithm::Aes128Gcm => {
                let key = Key::<Aes128Gcm>::from_slice(dek);
                let cipher = Aes128Gcm::new(key);
                cipher.decrypt(Nonce::from_slice(&encrypted.nonce), encrypted.ciphertext.as_ref())
                    .map_err(|e| CryptoError::Envelope(e.to_string()))
            }
        }
    }

    fn generate_nonce(&self) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        ThreadRng::default().fill_bytes(&mut nonce);
        nonce
    }
}

#[derive(Debug, Clone)]
pub struct EncryptedData {
    pub ciphertext: Vec<u8>,
    pub nonce: [u8; 12],
    pub tag: Vec<u8>, // For AES-GCM, tag is included in ciphertext
}