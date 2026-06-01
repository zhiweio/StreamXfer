use streamxfer_core::config::{
    CompressionCodec, ConsistencyMode, ExportConfig, ExportScope, OutputFormat, TableRef,
};
use streamxfer_core::planner::catalog::StaticCatalog;
use streamxfer_core::planner::scope::TaskKind;
use streamxfer_core::ExecutionEngine;

fn base_config(scope: ExportScope) -> ExportConfig {
    ExportConfig {
        connection_url: "mssql://user:pass@host:1433/warehouse".into(),
        scope,
        target: "s3://lake/raw/{database}/{schema}/{table}/".into(),
        format: OutputFormat::Parquet,
        compression: CompressionCodec::Snappy,
        consistency: ConsistencyMode::SnapshotTransaction,
        target_file_size: 256 * 1024 * 1024,
        max_rows_per_file: None,
        batch_rows: 65_536,
        memory_limit_mb: 512,
        table_concurrency: 2,
        partition_concurrency_per_table: 3,
        global_io_concurrency: 8,
        checkpoint_dir: None,
        resume: false,
    }
}

#[tokio::test]
async fn table_scope_creates_one_task_with_predicate() {
    let table = TableRef::new("dbo", "orders").with_database("warehouse");
    let config = base_config(ExportScope::Table {
        table,
        predicate: Some("id > 10".into()),
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].kind, TaskKind::Table);
    assert_eq!(
        tasks[0].target_prefix,
        "s3://lake/raw/warehouse/dbo/orders/"
    );
}
