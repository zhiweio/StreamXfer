use crate::error::{Result, StreamXferError};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageKind {
    Local,
    S3,
    Gcs,
    Azure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageUri {
    pub kind: StorageKind,
    pub bucket_or_container: Option<String>,
    pub prefix: String,
}

impl StorageUri {
    pub fn parse(raw: &str) -> Result<Self> {
        if let Some(rest) = raw.strip_prefix("s3://") {
            return parse_cloud(StorageKind::S3, rest);
        }
        if let Some(rest) = raw.strip_prefix("gs://") {
            return parse_cloud(StorageKind::Gcs, rest);
        }
        if let Some(rest) = raw
            .strip_prefix("az://")
            .or_else(|| raw.strip_prefix("abfs://"))
        {
            return parse_cloud(StorageKind::Azure, rest);
        }
        let prefix = raw.strip_prefix("file://").unwrap_or(raw).to_string();
        Ok(Self {
            kind: StorageKind::Local,
            bucket_or_container: None,
            prefix,
        })
    }
}

fn parse_cloud(kind: StorageKind, rest: &str) -> Result<StorageUri> {
    let mut parts = rest.splitn(2, '/');
    let bucket = parts.next().unwrap_or_default();
    if bucket.is_empty() {
        return Err(StreamXferError::Storage(
            "cloud target requires a bucket or container".into(),
        ));
    }
    Ok(StorageUri {
        kind,
        bucket_or_container: Some(bucket.to_string()),
        prefix: parts.next().unwrap_or_default().to_string(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutResult {
    pub path: String,
    pub bytes: u64,
    pub etag: Option<String>,
}

#[async_trait]
pub trait StorageSink: Send + Sync {
    async fn put_atomic(&self, path: &str, bytes: Bytes) -> Result<PutResult>;
    async fn head(&self, path: &str) -> Result<Option<PutResult>>;
}

#[derive(Debug, Clone)]
pub struct LocalSink {
    root: PathBuf,
}

impl LocalSink {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
    fn full_path(&self, path: &str) -> PathBuf {
        self.root.join(path.trim_start_matches('/'))
    }
}

#[async_trait]
impl StorageSink for LocalSink {
    async fn put_atomic(&self, path: &str, bytes: Bytes) -> Result<PutResult> {
        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = temp_path(&full_path);
        tokio::fs::write(&tmp, &bytes).await?;
        tokio::fs::rename(&tmp, &full_path).await?;
        Ok(PutResult {
            path: full_path.to_string_lossy().to_string(),
            bytes: bytes.len() as u64,
            etag: None,
        })
    }
    async fn head(&self, path: &str) -> Result<Option<PutResult>> {
        let full_path = self.full_path(path);
        match tokio::fs::metadata(&full_path).await {
            Ok(meta) => Ok(Some(PutResult {
                path: full_path.to_string_lossy().to_string(),
                bytes: meta.len(),
                etag: None,
            })),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

fn temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("part");
    path.with_file_name(format!(".{file_name}.tmp"))
}

#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    pub kind: StorageKind,
    pub bucket_or_container: String,
    pub prefix: String,
    pub endpoint: Option<String>,
}

pub fn default_sink_from_uri(uri: &StorageUri) -> Result<Arc<dyn StorageSink>> {
    match uri.kind {
        StorageKind::Local => Ok(Arc::new(LocalSink::new(&uri.prefix))),
        StorageKind::S3 | StorageKind::Gcs | StorageKind::Azure => Err(StreamXferError::Storage(
            "cloud sinks are configured by runtime object_store builders".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_local_and_cloud_targets() {
        assert_eq!(
            StorageUri::parse("/tmp/out").unwrap().kind,
            StorageKind::Local
        );
        let s3 = StorageUri::parse("s3://bucket/a/b").unwrap();
        assert_eq!(s3.bucket_or_container.as_deref(), Some("bucket"));
        assert_eq!(s3.prefix, "a/b");
    }
}
