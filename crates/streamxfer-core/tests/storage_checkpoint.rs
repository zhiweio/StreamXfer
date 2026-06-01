use bytes::Bytes;
use streamxfer_core::checkpoint::{
    CheckpointKey, CheckpointRecord, CheckpointStatus, CheckpointStore, InMemoryCheckpointStore,
};
use streamxfer_core::sink::storage::{LocalSink, StorageKind, StorageSink, StorageUri};

#[test]
fn storage_uri_parses_targets() {
    assert_eq!(
        StorageUri::parse("file:///tmp/out").unwrap().kind,
        StorageKind::Local
    );
    assert_eq!(
        StorageUri::parse("s3://bucket/prefix")
            .unwrap()
            .bucket_or_container
            .as_deref(),
        Some("bucket")
    );
    assert_eq!(
        StorageUri::parse("gs://bucket/prefix").unwrap().kind,
        StorageKind::Gcs
    );
    assert_eq!(
        StorageUri::parse("az://container/prefix").unwrap().kind,
        StorageKind::Azure
    );
    assert!(StorageUri::parse("s3://").is_err());
}

#[tokio::test]
async fn local_sink_writes_and_heads() {
    let root = std::env::temp_dir().join(format!("streamxfer-test-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&root).await;
    let sink = LocalSink::new(&root);
    let result = sink
        .put_atomic("dbo/orders/part.parquet", Bytes::from_static(b"PAR1"))
        .await
        .unwrap();
    assert_eq!(result.bytes, 4);
    assert_eq!(
        sink.head("dbo/orders/part.parquet")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        4
    );
    assert!(sink.head("missing").await.unwrap().is_none());
    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn checkpoint_is_file_scoped() {
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
    let mut other = key.clone();
    other.file_index = 1;
    assert_ne!(key.stable_id(), other.stable_id());
    store
        .put(CheckpointRecord {
            key: key.clone(),
            status: CheckpointStatus::Committed,
            object_path: "part.parquet".into(),
            row_count: 10,
            bytes_uncompressed: 100,
            bytes_compressed: 50,
            etag: Some("abc".into()),
            attempt: 1,
            error: None,
        })
        .await
        .unwrap();
    assert!(store.is_committed(&key).await.unwrap());
    assert!(!store.is_committed(&other).await.unwrap());
}
