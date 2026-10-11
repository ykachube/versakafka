//! Fake S3/MinIO for testing

use crate::TestKitError;
use crate::config::FakeS3Config;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Duration;
use futures::StreamExt;

pub struct FakeS3 {
    config: FakeS3Config,
    store: Arc<InMemory>,
    error_config: Arc<RwLock<ErrorConfig>>,
}

#[derive(Default)]
struct ErrorConfig {
    error_rate: f64,
    delay_ms: u64,
    fail_operations: HashMap<String, bool>, // operation -> should_fail
    fail_keys: HashMap<String, bool>, // key prefix -> should_fail
}

impl FakeS3 {
    pub fn new(config: FakeS3Config) -> Self {
        Self {
            config: config.clone(),
            store: Arc::new(InMemory::new()),
            error_config: Arc::new(RwLock::new(ErrorConfig::default())),
        }
    }

    pub async fn start(&self) -> Result<(), TestKitError> {
        tracing::info!("Fake S3 started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), TestKitError> {
        tracing::info!("Fake S3 stopped");
        Ok(())
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub fn set_error_rate(&self, rate: f64) {
        self.error_config.write().error_rate = rate.clamp(0.0, 1.0);
    }

    pub fn set_delay(&self, delay_ms: u64) {
        self.error_config.write().delay_ms = delay_ms;
    }

    pub fn set_operation_failure(&self, operation: &str, should_fail: bool) {
        self.error_config.write().fail_operations.insert(operation.to_string(), should_fail);
    }

    pub fn set_key_failure(&self, key_prefix: &str, should_fail: bool) {
        self.error_config.write().fail_keys.insert(key_prefix.to_string(), should_fail);
    }

    pub fn should_fail(&self, operation: &str, key: &str) -> bool {
        let config = self.error_config.read();
        
        // Check operation-specific failure
        if config.fail_operations.get(operation).copied().unwrap_or(false) {
            return true;
        }
        
        // Check key-specific failure
        for (prefix, should_fail) in &config.fail_keys {
            if key.starts_with(prefix) && *should_fail {
                return true;
            }
        }
        
        // Check global error rate
        if config.error_rate > 0.0 && rand::random::<f64>() < config.error_rate {
            return true;
        }
        
        false
    }

    pub fn get_delay(&self) -> Duration {
        Duration::from_millis(self.error_config.read().delay_ms)
    }

    /// List all objects in the store
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, TestKitError> {
        let path = Path::from(prefix);
        let mut results = Vec::new();
        
        let stream = self.store.list(Some(&path));
        let mut stream = Box::pin(stream);
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => results.push(meta.location.to_string()),
                Err(e) => return Err(TestKitError::FakeS3(e.to_string())),
            }
        }
        
        Ok(results)
    }

    /// Get object size
    pub async fn get_size(&self, key: &str) -> Result<usize, TestKitError> {
        let path = Path::from(key);
        let meta = self.store.head(&path).await
            .map_err(|e| TestKitError::FakeS3(e.to_string()))?;
        Ok(meta.size)
    }

    /// Delete object
    pub async fn delete(&self, key: &str) -> Result<(), TestKitError> {
        let path = Path::from(key);
        self.store.delete(&path).await
            .map_err(|e| TestKitError::FakeS3(e.to_string()))?;
        Ok(())
    }

    /// Clear all objects
    pub async fn clear(&self) -> Result<(), TestKitError> {
        let objects = self.list_objects("").await?;
        for obj in objects {
            self.delete(&obj).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStore;

    #[tokio::test]
    async fn test_fake_s3() {
        let config = FakeS3Config::default();
        let s3 = FakeS3::new(config);
        
        let store = s3.store();
        let path = Path::from("test-key");
        let data = bytes::Bytes::from("test data");
        
        // Put object
        store.put(&path, data.clone().into()).await.unwrap();
        
        // Get object
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, data);
        
        // List objects
        let objects = s3.list_objects("").await.unwrap();
        assert_eq!(objects.len(), 1);
        
        // Delete object
        s3.delete("test-key").await.unwrap();
        let objects = s3.list_objects("").await.unwrap();
        assert_eq!(objects.len(), 0);
    }
}