use streamxfer_core::config::{
    CompressionCodec, ConsistencyMode, ExportConfig, ExportScope, OutputFormat, TableRef,
};
use streamxfer_core::planner::catalog::StaticCatalog;
use streamxfer_core::planner::scope::{render_target_template, TaskKind};
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
        tasks[0].sql,
        "select * from [warehouse].[dbo].[orders] where id > 10"
    );
    assert_eq!(
        tasks[0].target_prefix,
        "s3://lake/raw/warehouse/dbo/orders/"
    );
}

#[tokio::test]
async fn query_scope_preserves_sql_and_query_target() {
    let mut config = base_config(ExportScope::Query {
        sql: "select id, name from dbo.orders".into(),
        name: "orders_delta".into(),
        partition_template: Some("id between {start} and {end}".into()),
    });
    config.target = "/tmp/export/{query}/".into();
    let tasks = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].kind, TaskKind::Query);
    assert_eq!(tasks[0].query_name.as_deref(), Some("orders_delta"));
    assert_eq!(tasks[0].target_prefix, "/tmp/export/orders_delta/");
    assert!(tasks[0]
        .partition_hint
        .as_deref()
        .unwrap()
        .contains("{start}"));
}

#[tokio::test]
async fn schema_and_database_scopes_expand_to_independent_table_tasks() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("dbo", "customers"),
        TableRef::new("audit", "events"),
    ]);
    let schema_config = base_config(ExportScope::Schema {
        schema: "dbo".into(),
        include: vec!["dbo.*".into()],
        exclude: vec!["*.customers".into()],
    });
    let schema_tasks = ExecutionEngine::new()
        .plan(&schema_config, &catalog)
        .await
        .unwrap();
    assert_eq!(schema_tasks.len(), 1);
    assert!(schema_tasks[0].id.contains("orders"));

    let db_config = base_config(ExportScope::Database {
        include: vec!["*.*".into()],
        exclude: vec!["audit.*".into()],
    });
    let db_tasks = ExecutionEngine::new()
        .plan(&db_config, &catalog)
        .await
        .unwrap();
    assert_eq!(db_tasks.len(), 2);
    assert!(db_tasks.iter().all(|task| task.table.is_some()));
}

#[tokio::test]
async fn empty_schema_fails_fast() {
    let config = base_config(ExportScope::Schema {
        schema: "missing".into(),
        include: vec![],
        exclude: vec![],
    });
    let err = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no exportable tables"));
}

#[test]
fn table_identifiers_and_target_templates_are_sanitized() {
    let table: TableRef = "sales.[daily/orders]".parse().unwrap();
    assert_eq!(table.sql_name(), "[sales].[daily/orders]");
    assert_eq!(table.path_name(), "sales/daily_orders");
    assert_eq!(
        render_target_template(
            "/data/{schema}/{table}/{partition}/part-{file_index}.parquet",
            Some(&table),
            None,
            Some("p:1"),
            Some(7)
        ),
        "/data/sales/daily_orders/p_1/part-00000007.parquet"
    );
}
