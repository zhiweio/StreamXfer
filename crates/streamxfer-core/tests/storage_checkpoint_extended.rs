use bytes::Bytes;
use streamxfer_core::checkpoint::{
    CheckpointKey, CheckpointRecord, CheckpointStatus, CheckpointStore, InMemoryCheckpointStore,
};
use streamxfer_core::sink::storage::{
    default_sink_from_uri, LocalSink, StorageKind, StorageSink, StorageUri,
};

// ============================================================
// StorageUri parsing - comprehensive
// ============================================================

#[test]
fn parse_local_absolute_path() {
    let uri = StorageUri::parse("/data/export/").unwrap();
    assert_eq!(uri.kind, StorageKind::Local);
    assert_eq!(uri.prefix, "/data/export/");
    assert!(uri.bucket_or_container.is_none());
}

#[test]
fn parse_local_relative_path() {
    let uri = StorageUri::parse("./output/").unwrap();
    assert_eq!(uri.kind, StorageKind::Local);
    assert_eq!(uri.prefix, "./output/");
}

#[test]
fn parse_file_scheme() {
    let uri = StorageUri::parse("file:///tmp/data").unwrap();
    assert_eq!(uri.kind, StorageKind::Local);
    assert_eq!(uri.prefix, "/tmp/data");
}

#[test]
fn parse_s3_uri() {
    let uri = StorageUri::parse("s3://my-bucket/prefix/path").unwrap();
    assert_eq!(uri.kind, StorageKind::S3);
    assert_eq!(uri.bucket_or_container.as_deref(), Some("my-bucket"));
    assert_eq!(uri.prefix, "prefix/path");
}

#[test]
fn parse_s3_uri_with_only_bucket() {
    let uri = StorageUri::parse("s3://my-bucket/").unwrap();
    assert_eq!(uri.kind, StorageKind::S3);
    assert_eq!(uri.bucket_or_container.as_deref(), Some("my-bucket"));
    assert_eq!(uri.prefix, "");
}

#[test]
fn parse_s3_uri_no_prefix() {
    let uri = StorageUri::parse("s3://bucket").unwrap();
    assert_eq!(uri.kind, StorageKind::S3);
    assert_eq!(uri.bucket_or_container.as_deref(), Some("bucket"));
    assert_eq!(uri.prefix, "");
}

#[test]
fn parse_gcs_uri() {
    let uri = StorageUri::parse("gs://my-bucket/data/").unwrap();
    assert_eq!(uri.kind, StorageKind::Gcs);
    assert_eq!(uri.bucket_or_container.as_deref(), Some("my-bucket"));
    assert_eq!(uri.prefix, "data/");
}

#[test]
fn parse_azure_az_scheme() {
    let uri = StorageUri::parse("az://mycontainer/path/to/data").unwrap();
    assert_eq!(uri.kind, StorageKind::Azure);
    assert_eq!(uri.bucket_or_container.as_deref(), Some("mycontainer"));
    assert_eq!(uri.prefix, "path/to/data");
}

#[test]
fn parse_azure_abfs_scheme() {
    let uri = StorageUri::parse("abfs://container/prefix").unwrap();
    assert_eq!(uri.kind, StorageKind::Azure);
    assert_eq!(uri.bucket_or_container.as_deref(), Some("container"));
    assert_eq!(uri.prefix, "prefix");
}

#[test]
fn parse_s3_empty_bucket_fails() {
    assert!(StorageUri::parse("s3://").is_err());
    assert!(StorageUri::parse("s3:///prefix").is_err());
}

#[test]
fn parse_gcs_empty_bucket_fails() {
    assert!(StorageUri::parse("gs://").is_err());
    assert!(StorageUri::parse("gs:///prefix").is_err());
}

#[test]
fn parse_azure_empty_container_fails() {
    assert!(StorageUri::parse("az://").is_err());
    assert!(StorageUri::parse("abfs://").is_err());
}

// ============================================================
// default_sink_from_uri
// ============================================================

#[test]
fn default_sink_from_local_uri() {
    let uri = StorageUri::parse("/tmp/test").unwrap();
    assert!(default_sink_from_uri(&uri).is_ok());
}

#[test]
fn default_sink_from_cloud_uri_returns_error() {
    let s3 = StorageUri::parse("s3://bucket/prefix").unwrap();
    assert!(default_sink_from_uri(&s3).is_err());

    let gcs = StorageUri::parse("gs://bucket/prefix").unwrap();
    assert!(default_sink_from_uri(&gcs).is_err());

    let az = StorageUri::parse("az://container/prefix").unwrap();
    assert!(default_sink_from_uri(&az).is_err());
}

// ============================================================
// LocalSink - comprehensive I/O tests
// ============================================================

#[tokio::test]
async fn local_sink_creates_nested_directories() {
    let root = std::env::temp_dir().join(format!("stx-test-nested-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&root).await;
    let sink = LocalSink::new(&root);

    sink.put_atomic("a/b/c/d/file.parquet", Bytes::from_static(b"data"))
        .await
        .unwrap();

    let meta = tokio::fs::metadata(root.join("a/b/c/d/file.parquet"))
        .await
        .unwrap();
    assert!(meta.is_file());
    assert_eq!(meta.len(), 4);

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn local_sink_overwrites_existing_file() {
    let root = std::env::temp_dir().join(format!("stx-test-overwrite-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&root).await;
    let sink = LocalSink::new(&root);

    sink.put_atomic("file.dat", Bytes::from_static(b"first"))
        .await
        .unwrap();
    let r1 = sink.head("file.dat").await.unwrap().unwrap();
    assert_eq!(r1.bytes, 5);

    sink.put_atomic("file.dat", Bytes::from_static(b"second_longer"))
        .await
        .unwrap();
    let r2 = sink.head("file.dat").await.unwrap().unwrap();
    assert_eq!(r2.bytes, 13);

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn local_sink_head_returns_none_for_missing() {
    let root = std::env::temp_dir().join(format!("stx-test-head-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&root).await;
    tokio::fs::create_dir_all(&root).await.unwrap();
    let sink = LocalSink::new(&root);

    assert!(sink.head("nonexistent.parquet").await.unwrap().is_none());

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn local_sink_put_result_contains_correct_path() {
    let root = std::env::temp_dir().join(format!("stx-test-path-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&root).await;
    let sink = LocalSink::new(&root);

    let result = sink
        .put_atomic("dbo/orders/part-00000000.parquet", Bytes::from_static(b"X"))
        .await
        .unwrap();

    assert!(result.path.contains("dbo/orders/part-00000000.parquet"));
    assert_eq!(result.bytes, 1);
    assert!(result.etag.is_none());

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn local_sink_strips_leading_slash_from_path() {
    let root = std::env::temp_dir().join(format!("stx-test-strip-{}", std::process::id()));
    let _ = tokio::fs::remove_dir_all(&root).await;
    let sink = LocalSink::new(&root);

    sink.put_atomic("/leading/slash.parquet", Bytes::from_static(b"ok"))
        .await
        .unwrap();

    assert!(sink.head("/leading/slash.parquet").await.unwrap().is_some());
    // Verify file is under root, not at absolute /leading/slash.parquet
    assert!(root.join("leading/slash.parquet").exists());

    let _ = tokio::fs::remove_dir_all(&root).await;
}

// ============================================================
// Checkpoint - comprehensive state machine tests
// ============================================================

fn test_key(table: &str, partition: &str, file_index: u64) -> CheckpointKey {
    CheckpointKey {
        job_id: "test-job".into(),
        database: Some("testdb".into()),
        schema: Some("dbo".into()),
        table: Some(table.into()),
        query_name: None,
        partition_id: partition.into(),
        file_index,
    }
}

fn test_record(key: CheckpointKey, status: CheckpointStatus) -> CheckpointRecord {
    CheckpointRecord {
        key,
        status,
        object_path: "s3://bucket/path/part.parquet".into(),
        row_count: 1000,
        bytes_uncompressed: 50000,
        bytes_compressed: 20000,
        etag: Some("etag123".into()),
        attempt: 1,
        error: None,
    }
}

#[tokio::test]
async fn checkpoint_get_returns_none_for_unknown_key() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    assert!(store.get(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn checkpoint_put_and_get_round_trip() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    let record = test_record(key.clone(), CheckpointStatus::Running);

    store.put(record.clone()).await.unwrap();
    let retrieved = store.get(&key).await.unwrap().unwrap();
    assert_eq!(retrieved.status, CheckpointStatus::Running);
    assert_eq!(retrieved.row_count, 1000);
    assert_eq!(retrieved.object_path, "s3://bucket/path/part.parquet");
}

#[tokio::test]
async fn checkpoint_put_updates_existing() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);

    store
        .put(test_record(key.clone(), CheckpointStatus::Running))
        .await
        .unwrap();
    store
        .put(test_record(key.clone(), CheckpointStatus::Committed))
        .await
        .unwrap();

    let retrieved = store.get(&key).await.unwrap().unwrap();
    assert_eq!(retrieved.status, CheckpointStatus::Committed);
}

#[tokio::test]
async fn checkpoint_is_committed_true_for_committed() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    store
        .put(test_record(key.clone(), CheckpointStatus::Committed))
        .await
        .unwrap();
    assert!(store.is_committed(&key).await.unwrap());
}

#[tokio::test]
async fn checkpoint_is_committed_false_for_running() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    store
        .put(test_record(key.clone(), CheckpointStatus::Running))
        .await
        .unwrap();
    assert!(!store.is_committed(&key).await.unwrap());
}

#[tokio::test]
async fn checkpoint_is_committed_false_for_uploaded() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    store
        .put(test_record(key.clone(), CheckpointStatus::Uploaded))
        .await
        .unwrap();
    assert!(!store.is_committed(&key).await.unwrap());
}

#[tokio::test]
async fn checkpoint_is_committed_false_for_failed() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    store
        .put(test_record(key.clone(), CheckpointStatus::Failed))
        .await
        .unwrap();
    assert!(!store.is_committed(&key).await.unwrap());
}

#[tokio::test]
async fn checkpoint_is_committed_false_for_planned() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    store
        .put(test_record(key.clone(), CheckpointStatus::Planned))
        .await
        .unwrap();
    assert!(!store.is_committed(&key).await.unwrap());
}

#[tokio::test]
async fn checkpoint_is_committed_false_for_missing() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    assert!(!store.is_committed(&key).await.unwrap());
}

#[tokio::test]
async fn checkpoint_different_file_indices_are_independent() {
    let store = InMemoryCheckpointStore::default();
    let key0 = test_key("orders", "p0", 0);
    let key1 = test_key("orders", "p0", 1);
    let key2 = test_key("orders", "p0", 2);

    store
        .put(test_record(key0.clone(), CheckpointStatus::Committed))
        .await
        .unwrap();
    store
        .put(test_record(key1.clone(), CheckpointStatus::Running))
        .await
        .unwrap();

    assert!(store.is_committed(&key0).await.unwrap());
    assert!(!store.is_committed(&key1).await.unwrap());
    assert!(!store.is_committed(&key2).await.unwrap());
}

#[tokio::test]
async fn checkpoint_different_partitions_are_independent() {
    let store = InMemoryCheckpointStore::default();
    let key_p0 = test_key("orders", "p0", 0);
    let key_p1 = test_key("orders", "p1", 0);

    store
        .put(test_record(key_p0.clone(), CheckpointStatus::Committed))
        .await
        .unwrap();

    assert!(store.is_committed(&key_p0).await.unwrap());
    assert!(!store.is_committed(&key_p1).await.unwrap());
}

#[tokio::test]
async fn checkpoint_different_tables_are_independent() {
    let store = InMemoryCheckpointStore::default();
    let key_orders = test_key("orders", "p0", 0);
    let key_items = test_key("items", "p0", 0);

    store
        .put(test_record(key_orders.clone(), CheckpointStatus::Committed))
        .await
        .unwrap();

    assert!(store.is_committed(&key_orders).await.unwrap());
    assert!(!store.is_committed(&key_items).await.unwrap());
}

#[tokio::test]
async fn checkpoint_stable_id_is_deterministic() {
    let key1 = test_key("orders", "p0", 0);
    let key2 = test_key("orders", "p0", 0);
    assert_eq!(key1.stable_id(), key2.stable_id());
}

#[tokio::test]
async fn checkpoint_stable_id_differs_for_different_keys() {
    let key1 = test_key("orders", "p0", 0);
    let key2 = test_key("orders", "p0", 1);
    let key3 = test_key("orders", "p1", 0);
    let key4 = test_key("items", "p0", 0);

    assert_ne!(key1.stable_id(), key2.stable_id());
    assert_ne!(key1.stable_id(), key3.stable_id());
    assert_ne!(key1.stable_id(), key4.stable_id());
}

#[tokio::test]
async fn checkpoint_with_error_field() {
    let store = InMemoryCheckpointStore::default();
    let key = test_key("orders", "p0", 0);
    let mut record = test_record(key.clone(), CheckpointStatus::Failed);
    record.error = Some("connection timed out".into());
    record.attempt = 3;

    store.put(record).await.unwrap();
    let retrieved = store.get(&key).await.unwrap().unwrap();
    assert_eq!(retrieved.status, CheckpointStatus::Failed);
    assert_eq!(retrieved.error.as_deref(), Some("connection timed out"));
    assert_eq!(retrieved.attempt, 3);
}

#[tokio::test]
async fn checkpoint_query_key_without_table() {
    let store = InMemoryCheckpointStore::default();
    let key = CheckpointKey {
        job_id: "job".into(),
        database: None,
        schema: None,
        table: None,
        query_name: Some("my_report".into()),
        partition_id: "single".into(),
        file_index: 0,
    };
    store
        .put(test_record(key.clone(), CheckpointStatus::Committed))
        .await
        .unwrap();
    assert!(store.is_committed(&key).await.unwrap());
}
