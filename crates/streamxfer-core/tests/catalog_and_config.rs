use streamxfer_core::config::{
    CompressionCodec, ConsistencyMode, ExportConfig, ExportScope, OutputFormat, TableRef,
};
use streamxfer_core::planner::catalog::{
    ensure_non_empty, list_tables_sql, Catalog, StaticCatalog,
};

// ============================================================
// StaticCatalog
// ============================================================

#[tokio::test]
async fn static_catalog_list_schema_tables() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("dbo", "items"),
        TableRef::new("sales", "invoices"),
    ]);
    let dbo_tables = catalog.list_schema_tables("dbo").await.unwrap();
    assert_eq!(dbo_tables.len(), 2);
    assert!(dbo_tables.iter().all(|t| t.schema == "dbo"));
}

#[tokio::test]
async fn static_catalog_list_schema_tables_case_insensitive() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("DBO", "orders"),
        TableRef::new("dbo", "items"),
    ]);
    let tables = catalog.list_schema_tables("dbo").await.unwrap();
    assert_eq!(tables.len(), 2);
}

#[tokio::test]
async fn static_catalog_list_schema_tables_empty_for_unknown_schema() {
    let catalog = StaticCatalog::new(vec![TableRef::new("dbo", "orders")]);
    let tables = catalog.list_schema_tables("sales").await.unwrap();
    assert!(tables.is_empty());
}

#[tokio::test]
async fn static_catalog_list_database_tables() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("sales", "invoices"),
    ]);
    let tables = catalog.list_database_tables().await.unwrap();
    assert_eq!(tables.len(), 2);
}

#[tokio::test]
async fn static_catalog_empty() {
    let catalog = StaticCatalog::default();
    let tables = catalog.list_database_tables().await.unwrap();
    assert!(tables.is_empty());
}

// ============================================================
// list_tables_sql
// ============================================================

#[test]
fn list_tables_sql_with_schema() {
    let sql = list_tables_sql(Some("dbo"));
    assert!(sql.contains("information_schema.tables"));
    assert!(sql.contains("table_schema = @P1"));
    assert!(sql.contains("BASE TABLE"));
}

#[test]
fn list_tables_sql_without_schema() {
    let sql = list_tables_sql(None);
    assert!(sql.contains("information_schema.tables"));
    assert!(!sql.contains("@P1"));
    assert!(sql.contains("BASE TABLE"));
}

// ============================================================
// ensure_non_empty
// ============================================================

#[test]
fn ensure_non_empty_with_tables_ok() {
    ensure_non_empty("dbo", &[TableRef::new("dbo", "orders")]).unwrap();
}

#[test]
fn ensure_non_empty_with_no_tables_fails() {
    let err = ensure_non_empty("missing_schema", &[]).unwrap_err();
    assert!(err.to_string().contains("no exportable tables"));
    assert!(err.to_string().contains("missing_schema"));
}

// ============================================================
// ExportConfig - edge cases in JSON deserialization
// ============================================================

#[test]
fn config_with_all_formats() {
    for (format_str, expected) in [
        ("\"parquet\"", OutputFormat::Parquet),
        ("\"csv\"", OutputFormat::Csv),
        ("\"tsv\"", OutputFormat::Tsv),
        ("\"json\"", OutputFormat::Json),
    ] {
        let json = format!(
            r#"{{"connection_url":"mssql://u:p@h/d","scope":{{"type":"table","table":{{"schema":"dbo","table":"t"}},"predicate":null}},"target":"/tmp/","format":{}}}"#,
            format_str
        );
        let config: ExportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.format, expected);
    }
}

#[test]
fn config_with_all_compression_codecs() {
    for (codec_str, expected) in [
        ("\"none\"", CompressionCodec::None),
        ("\"snappy\"", CompressionCodec::Snappy),
        ("\"zstd\"", CompressionCodec::Zstd),
        ("\"gzip\"", CompressionCodec::Gzip),
    ] {
        let json = format!(
            r#"{{"connection_url":"mssql://u:p@h/d","scope":{{"type":"table","table":{{"schema":"dbo","table":"t"}},"predicate":null}},"target":"/tmp/","compression":{}}}"#,
            codec_str
        );
        let config: ExportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.compression, expected);
    }
}

#[test]
fn config_with_all_consistency_modes() {
    for (mode_str, expected) in [
        ("\"none\"", ConsistencyMode::None),
        (
            "\"snapshot_transaction\"",
            ConsistencyMode::SnapshotTransaction,
        ),
        ("\"database_snapshot\"", ConsistencyMode::DatabaseSnapshot),
        ("\"high_watermark\"", ConsistencyMode::HighWatermark),
    ] {
        let json = format!(
            r#"{{"connection_url":"mssql://u:p@h/d","scope":{{"type":"table","table":{{"schema":"dbo","table":"t"}},"predicate":null}},"target":"/tmp/","consistency":{}}}"#,
            mode_str
        );
        let config: ExportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.consistency, expected);
    }
}

#[test]
fn config_with_checkpoint_dir() {
    let json = r#"{
        "connection_url": "mssql://u:p@h/d",
        "scope": {"type": "table", "table": {"schema": "dbo", "table": "t"}, "predicate": null},
        "target": "/tmp/",
        "checkpoint_dir": "/var/checkpoints",
        "resume": true
    }"#;
    let config: ExportConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.checkpoint_dir.as_deref(), Some("/var/checkpoints"));
    assert!(config.resume);
}

#[test]
fn config_scope_database_with_filters() {
    let json = r#"{
        "connection_url": "mssql://u:p@h/d",
        "scope": {
            "type": "database",
            "include": ["dbo.*", "sales.*"],
            "exclude": ["*.tmp_*"]
        },
        "target": "s3://bucket/output/"
    }"#;
    let config: ExportConfig = serde_json::from_str(json).unwrap();
    match &config.scope {
        ExportScope::Database { include, exclude } => {
            assert_eq!(include, &["dbo.*", "sales.*"]);
            assert_eq!(exclude, &["*.tmp_*"]);
        }
        _ => panic!("Expected Database scope"),
    }
}

#[test]
fn config_scope_query_round_trip() {
    let config = ExportConfig {
        connection_url: "mssql://u:p@h/d".into(),
        scope: ExportScope::Query {
            sql: "SELECT * FROM orders WHERE id > 100".into(),
            name: "filtered_orders".into(),
            partition_template: Some("id BETWEEN {lo} AND {hi}".into()),
        },
        target: "/output/".into(),
        format: OutputFormat::Json,
        compression: CompressionCodec::Gzip,
        consistency: ConsistencyMode::None,
        target_file_size: 100 * 1024 * 1024,
        batch_rows: 10_000,
        memory_limit_mb: 256,
        table_concurrency: 2,
        partition_concurrency_per_table: 2,
        global_io_concurrency: 8,
        checkpoint_dir: Some("/tmp/ckpt".into()),
        resume: true,
    };
    let json = serde_json::to_string(&config).unwrap();
    let deserialized: ExportConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.format, OutputFormat::Json);
    assert_eq!(deserialized.compression, CompressionCodec::Gzip);
    assert_eq!(deserialized.consistency, ConsistencyMode::None);
    assert_eq!(deserialized.batch_rows, 10_000);
    assert!(deserialized.resume);
    match &deserialized.scope {
        ExportScope::Query {
            sql,
            name,
            partition_template,
        } => {
            assert_eq!(sql, "SELECT * FROM orders WHERE id > 100");
            assert_eq!(name, "filtered_orders");
            assert_eq!(
                partition_template.as_deref(),
                Some("id BETWEEN {lo} AND {hi}")
            );
        }
        _ => panic!("Expected Query scope"),
    }
}
