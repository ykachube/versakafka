//! S3 uploader for spill

use crate::{SpillError, Result, config::SpillConfig, config::UploadConfig};
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

pub struct Uploader {
    config: SpillConfig,
    store: Arc<dyn ObjectStore>,
    sender: mpsc::Sender<UploadRequest>,
}

struct UploadRequest {
    path: String,
    data: Bytes,
    response: tokio::sync::oneshot::Sender<Result<()>>,
}

impl Uploader {
    pub fn new(config: SpillConfig) -> Result<Self> {
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
        
        let store = Arc::new(builder.build()?);
        
        let (sender, mut receiver) = mpsc::channel::<UploadRequest>(config.upload.max_concurrent_uploads * 2);
        let store_clone = store.clone();
        let config_clone = config.upload.clone();
        
        tokio::spawn(async move {
            while let Some(req) = receiver.recv().await {
                let store = store_clone.clone();
                let path = ObjectPath::from(req.path.clone());
                let data = req.data;
                
                let result = store.put(&path, data.into()).await.map(|_| ());
                
                let _ = req.response.send(result.map(|_| ()).map_err(|e| SpillError::ObjectStore(e)));
            }
        });
        
        Ok(Self { config, store, sender })
    }
    
    pub async fn upload(&self, path: String, data: Bytes) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender.send(UploadRequest { path, data, response: tx }).await
            .map_err(|e| SpillError::Uploader(e.to_string()))?;
        
        rx.await.map_err(|e| SpillError::Uploader(e.to_string()))?
    }
    
    pub async fn upload_segment(&self, segment_path: &str, data: Bytes) -> Result<()> {
        let path = format!("{}{}", self.config.prefix, segment_path);
        self.upload(path, data).await
    }
    
    pub async fn upload_manifest(&self, manifest: &crate::manifest::Manifest) -> Result<()> {
        let data = match self.config.manifest.format {
            crate::config::ManifestFormat::Json => serde_json::to_vec_pretty(manifest)?,
            crate::config::ManifestFormat::Parquet => {
                return Err(SpillError::Manifest("Parquet format not implemented".to_string()));
            }
        };
        
        let path = format!("{}manifest-{}.json", self.config.prefix, manifest.created_at.timestamp_millis());
        self.upload(path, Bytes::from(data)).await
    }
}