//! DEK cache

use crate::{CryptoError, Result, config::CryptoConfig, config::DekConfig};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct DekCache {
    config: DekConfig,
    cache: Arc<RwLock<HashMap<String, CachedDek>>>,
}

#[derive(Debug, Clone)]
struct CachedDek {
    plaintext: Vec<u8>,
    ciphertext: Vec<u8>,
    created_at: Instant,
    ttl: Duration,
}

impl DekCache {
    pub fn new(config: DekConfig) -> Self {
        Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, key_id: &str) -> Option<Vec<u8>> {
        let cache = self.cache.read();
        cache.get(key_id).and_then(|cached| {
            if cached.created_at.elapsed() < cached.ttl {
                Some(cached.plaintext.clone())
            } else {
                None
            }
        })
    }

    pub fn put(&self, key_id: String, plaintext: Vec<u8>, ciphertext: Vec<u8>) {
        if self.cache.read().len() >= self.config.cache_size {
            // Evict oldest
            let mut cache = self.cache.write();
            if let Some(oldest_key) = cache.iter()
                .min_by_key(|(_, v)| v.created_at)
                .map(|(k, _)| k.clone()) {
                cache.remove(&oldest_key);
            }
        }
        
        self.cache.write().insert(key_id, CachedDek {
            plaintext,
            ciphertext,
            created_at: Instant::now(),
            ttl: self.config.rotation_interval,
        });
    }

    pub fn invalidate(&self, key_id: &str) {
        self.cache.write().remove(key_id);
    }
}