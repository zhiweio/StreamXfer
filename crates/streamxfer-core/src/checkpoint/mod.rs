use crate::error::{Result, StreamXferError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CheckpointKey {
    pub job_id: String,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub query_name: Option<String>,
    pub partition_id: String,
    pub file_index: u64,
}
impl CheckpointKey {
    pub fn stable_id(&self) -> String {
        let payload = serde_json::to_vec(self).expect("checkpoint key serializes");
        hex::encode(Sha256::digest(payload))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStatus {
    Planned,
    Running,
    Uploaded,
    Committed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub key: CheckpointKey,
    pub status: CheckpointStatus,
    pub object_path: String,
    pub row_count: u64,
    pub bytes_uncompressed: u64,
    pub bytes_compressed: u64,
    pub etag: Option<String>,
    pub attempt: u32,
    pub error: Option<String>,
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn get(&self, key: &CheckpointKey) -> Result<Option<CheckpointRecord>>;
    async fn put(&self, record: CheckpointRecord) -> Result<()>;
    async fn is_committed(&self, key: &CheckpointKey) -> Result<bool> {
        Ok(matches!(
            self.get(key).await?.map(|record| record.status),
            Some(CheckpointStatus::Committed)
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryCheckpointStore {
    records: Arc<Mutex<HashMap<String, CheckpointRecord>>>,
}
#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn get(&self, key: &CheckpointKey) -> Result<Option<CheckpointRecord>> {
        let records = self
            .records
            .lock()
            .map_err(|err| StreamXferError::Checkpoint(err.to_string()))?;
        Ok(records.get(&key.stable_id()).cloned())
    }
    async fn put(&self, record: CheckpointRecord) -> Result<()> {
        let mut records = self
            .records
            .lock()
            .map_err(|err| StreamXferError::Checkpoint(err.to_string()))?;
        records.insert(record.key.stable_id(), record);
        Ok(())
    }
}

#[cfg(feature = "rocksdb-checkpoint")]
pub mod rocks {
    use super::*;
    use rocksdb::DB;
    use std::path::Path;
    use std::sync::Arc;
    #[derive(Clone)]
    pub struct RocksDbCheckpointStore {
        db: Arc<DB>,
    }
    impl RocksDbCheckpointStore {
        pub fn open(path: impl AsRef<Path>) -> Result<Self> {
            let db = DB::open_default(path)
                .map_err(|err| StreamXferError::Checkpoint(err.to_string()))?;
            Ok(Self { db: Arc::new(db) })
        }
    }
    #[async_trait]
    impl CheckpointStore for RocksDbCheckpointStore {
        async fn get(&self, key: &CheckpointKey) -> Result<Option<CheckpointRecord>> {
            let value = self
                .db
                .get(key.stable_id())
                .map_err(|err| StreamXferError::Checkpoint(err.to_string()))?;
            value
                .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
                .transpose()
        }
        async fn put(&self, record: CheckpointRecord) -> Result<()> {
            self.db
                .put(record.key.stable_id(), serde_json::to_vec(&record)?)
                .map_err(|err| StreamXferError::Checkpoint(err.to_string()))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn committed_records_are_skipped() {
        let store = InMemoryCheckpointStore::default();
        let key = CheckpointKey {
            job_id: "job".into(),
            database: Some("db".into()),
            schema: Some("dbo".into()),
            table: Some("orders".into()),
            query_name: None,
            partition_id: "p0".into(),
            file_index: 0,
        };
        store
            .put(CheckpointRecord {
                key: key.clone(),
                status: CheckpointStatus::Committed,
                object_path: "part.parquet".into(),
                row_count: 1,
                bytes_uncompressed: 10,
                bytes_compressed: 8,
                etag: Some("etag".into()),
                attempt: 1,
                error: None,
            })
            .await
            .unwrap();
        assert!(store.is_committed(&key).await.unwrap());
    }
}
