//! Fake S3/MinIO for testing

use crate::TestKitError;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

pub struct FakeS3 {
    config: super::config::FakeS3Config,
    store: Arc<dyn ObjectStore>,
}

impl FakeS3 {
    pub fn new(config: super::config::FakeS3Config) -> Self {
        Self {
            config,
            store: Arc::new(InMemory::new()),
        }
    }

    pub async fn start(&self) -> Result<(), TestKitError> {
        tracing::info!("Fake S3 started");
        Ok(())
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }
}