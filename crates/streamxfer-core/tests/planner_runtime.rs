use streamxfer_core::config::{
    CompressionCodec, ConsistencyMode, ExportConfig, ExportScope, OutputFormat, TableRef,
};
use streamxfer_core::planner::catalog::StaticCatalog;
use streamxfer_core::planner::partition::PartitionStrategy;
use streamxfer_core::planner::scope::{render_target_template, TaskKind};
use streamxfer_core::runtime::limits::RuntimeLimits;
use streamxfer_core::ExecutionEngine;

fn base_config(scope: ExportScope) -> ExportConfig {
    ExportConfig {
        connection_url: "mssql://user:pass@host:1433/db".into(),
        scope,
        target: "/output/{schema}/{table}/".into(),
        format: OutputFormat::Parquet,
        compression: CompressionCodec::Snappy,
        consistency: ConsistencyMode::SnapshotTransaction,
        target_file_size: 256 * 1024 * 1024,
        max_rows_per_file: None,
        batch_rows: 65_536,
        memory_limit_mb: 512,
        table_concurrency: 4,
        partition_concurrency_per_table: 4,
        global_io_concurrency: 16,
        checkpoint_dir: None,
        resume: false,
    }
}

// ============================================================
// Planner - scope expansion
// ============================================================

#[tokio::test]
async fn table_scope_without_predicate() {
    let config = base_config(ExportScope::Table {
        table: TableRef::new("dbo", "users"),
        predicate: None,
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].kind, TaskKind::Table);
    assert_eq!(tasks[0].sql, "select * from [dbo].[users]");
    assert_eq!(tasks[0].target_prefix, "/output/dbo/users/");
    assert!(tasks[0].partition_hint.is_none());
}

#[tokio::test]
async fn table_scope_with_predicate() {
    let config = base_config(ExportScope::Table {
        table: TableRef::new("dbo", "orders"),
        predicate: Some("status = 'active'".into()),
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert!(tasks[0].sql.contains("where status = 'active'"));
}

#[tokio::test]
async fn query_scope_with_partition_template() {
    let config = base_config(ExportScope::Query {
        sql: "SELECT * FROM large_table".into(),
        name: "big_export".into(),
        partition_template: Some("id BETWEEN {lo} AND {hi}".into()),
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].kind, TaskKind::Query);
    assert_eq!(tasks[0].query_name.as_deref(), Some("big_export"));
    assert_eq!(
        tasks[0].partition_hint.as_deref(),
        Some("id BETWEEN {lo} AND {hi}")
    );
}

#[tokio::test]
async fn schema_scope_filters_by_schema_name() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("dbo", "customers"),
        TableRef::new("sales", "invoices"),
        TableRef::new("hr", "employees"),
    ]);
    let config = base_config(ExportScope::Schema {
        schema: "dbo".into(),
        include: vec![],
        exclude: vec![],
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &catalog)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
    assert!(tasks
        .iter()
        .all(|t| t.table.as_ref().unwrap().schema == "dbo"));
}

#[tokio::test]
async fn schema_scope_with_include_filter() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("dbo", "order_items"),
        TableRef::new("dbo", "customers"),
    ]);
    let config = base_config(ExportScope::Schema {
        schema: "dbo".into(),
        include: vec!["*.order*".into()],
        exclude: vec![],
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &catalog)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
    assert!(tasks
        .iter()
        .all(|t| t.table.as_ref().unwrap().table.starts_with("order")));
}

#[tokio::test]
async fn schema_scope_with_exclude_filter() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("dbo", "tmp_orders"),
        TableRef::new("dbo", "bak_orders"),
    ]);
    let config = base_config(ExportScope::Schema {
        schema: "dbo".into(),
        include: vec![],
        exclude: vec!["*.tmp_*".into(), "*.bak_*".into()],
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &catalog)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].table.as_ref().unwrap().table, "orders");
}

#[tokio::test]
async fn schema_scope_include_and_exclude_combined() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("dbo", "order_archive"),
        TableRef::new("dbo", "customers"),
    ]);
    let config = base_config(ExportScope::Schema {
        schema: "dbo".into(),
        include: vec!["*.order*".into()],
        exclude: vec!["*_archive".into()],
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &catalog)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].table.as_ref().unwrap().table, "orders");
}

#[tokio::test]
async fn database_scope_includes_all_schemas() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("sales", "items"),
        TableRef::new("hr", "employees"),
    ]);
    let config = base_config(ExportScope::Database {
        include: vec![],
        exclude: vec![],
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &catalog)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 3);
}

#[tokio::test]
async fn database_scope_with_include_filter() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("sales", "items"),
        TableRef::new("hr", "employees"),
    ]);
    let config = base_config(ExportScope::Database {
        include: vec!["dbo.*".into(), "sales.*".into()],
        exclude: vec![],
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &catalog)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
}

#[tokio::test]
async fn empty_database_fails_fast() {
    let config = base_config(ExportScope::Database {
        include: vec![],
        exclude: vec![],
    });
    let err = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no exportable tables"));
}

#[tokio::test]
async fn schema_case_insensitive_matching() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("DBO", "orders"),
        TableRef::new("dbo", "items"),
    ]);
    let config = base_config(ExportScope::Schema {
        schema: "dbo".into(),
        include: vec![],
        exclude: vec![],
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &catalog)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
}

// ============================================================
// Target template rendering
// ============================================================

#[test]
fn render_template_with_all_variables() {
    let table = TableRef::new("sales", "orders").with_database("warehouse");
    let result = render_target_template(
        "s3://lake/{database}/{schema}/{table}/{partition}/part-{file_index}.parquet",
        Some(&table),
        None,
        Some("p0"),
        Some(3),
    );
    assert_eq!(
        result,
        "s3://lake/warehouse/sales/orders/p0/part-00000003.parquet"
    );
}

#[test]
fn render_template_query_mode() {
    let result = render_target_template(
        "/output/{query}/{file_index}.json",
        None,
        Some("my_report"),
        None,
        Some(0),
    );
    assert_eq!(result, "/output/my_report/00000000.json");
}

#[test]
fn render_template_no_database_uses_default() {
    let table = TableRef::new("dbo", "orders");
    let result = render_target_template(
        "s3://bucket/{database}/{schema}/{table}/",
        Some(&table),
        None,
        None,
        None,
    );
    assert_eq!(result, "s3://bucket/default/dbo/orders/");
}

#[test]
fn render_template_leaves_unknown_variables_intact() {
    let result = render_target_template("/output/{unknown}/data/", None, None, None, None);
    assert_eq!(result, "/output/{unknown}/data/");
}

// ============================================================
// PartitionStrategy
// ============================================================

#[test]
fn partition_none_produces_single_partition() {
    let partitions = PartitionStrategy::None.plan(None);
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].id, "single");
    assert!(partitions[0].predicate.is_none());
}

#[test]
fn partition_predicate_list_produces_correct_count() {
    let strategy = PartitionStrategy::PredicateList {
        predicates: vec![
            "id < 1000".into(),
            "id >= 1000 AND id < 2000".into(),
            "id >= 2000".into(),
        ],
    };
    let partitions = strategy.plan(None);
    assert_eq!(partitions.len(), 3);
    assert_eq!(partitions[0].id, "p00000000");
    assert_eq!(partitions[0].predicate.as_deref(), Some("id < 1000"));
    assert_eq!(partitions[1].id, "p00000001");
    assert_eq!(partitions[2].id, "p00000002");
}

#[test]
fn partition_range_produces_correct_count() {
    let strategy = PartitionStrategy::Range {
        column: "id".into(),
        start: "0".into(),
        end: "1000".into(),
        partitions: 4,
    };
    let table = TableRef::new("dbo", "orders");
    let partitions = strategy.plan(Some(&table));
    assert_eq!(partitions.len(), 4);
    for p in &partitions {
        assert!(p.predicate.is_some());
        assert!(p.predicate.as_ref().unwrap().contains("id"));
    }
}

// ============================================================
// RuntimeLimits
// ============================================================

#[test]
fn runtime_limits_valid_config() {
    RuntimeLimits {
        table_concurrency: 4,
        partition_concurrency_per_table: 4,
        global_io_concurrency: 16,
        memory_limit_mb: 512,
    }
    .validate()
    .unwrap();
}

#[test]
fn runtime_limits_zero_table_concurrency_fails() {
    assert!(RuntimeLimits {
        table_concurrency: 0,
        partition_concurrency_per_table: 4,
        global_io_concurrency: 16,
        memory_limit_mb: 512,
    }
    .validate()
    .is_err());
}

#[test]
fn runtime_limits_zero_partition_concurrency_fails() {
    assert!(RuntimeLimits {
        table_concurrency: 4,
        partition_concurrency_per_table: 0,
        global_io_concurrency: 16,
        memory_limit_mb: 512,
    }
    .validate()
    .is_err());
}

#[test]
fn runtime_limits_zero_io_concurrency_fails() {
    assert!(RuntimeLimits {
        table_concurrency: 4,
        partition_concurrency_per_table: 4,
        global_io_concurrency: 0,
        memory_limit_mb: 512,
    }
    .validate()
    .is_err());
}

// ============================================================
// ExecutionEngine dry_run
// ============================================================

#[tokio::test]
async fn dry_run_returns_correct_task_count() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("dbo", "items"),
    ]);
    let config = base_config(ExportScope::Schema {
        schema: "dbo".into(),
        include: vec![],
        exclude: vec![],
    });
    let summary = ExecutionEngine::new()
        .dry_run(&config, &catalog)
        .await
        .unwrap();
    assert_eq!(summary.planned_tasks, 2);
    assert_eq!(summary.completed_tasks, 0);
    assert_eq!(summary.bytes_written, 0);
}

#[tokio::test]
async fn dry_run_validates_limits() {
    let mut config = base_config(ExportScope::Table {
        table: TableRef::new("dbo", "orders"),
        predicate: None,
    });
    config.table_concurrency = 0;
    let err = ExecutionEngine::new()
        .dry_run(&config, &StaticCatalog::default())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("concurrency"));
}

// ============================================================
// Task ID generation
// ============================================================

#[tokio::test]
async fn task_id_for_table_scope() {
    let config = base_config(ExportScope::Table {
        table: TableRef::new("dbo", "orders"),
        predicate: None,
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap();
    assert_eq!(tasks[0].id, "table:dbo/orders");
}

#[tokio::test]
async fn task_id_for_table_with_database() {
    let config = base_config(ExportScope::Table {
        table: TableRef::new("dbo", "orders").with_database("warehouse"),
        predicate: None,
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap();
    assert_eq!(tasks[0].id, "table:warehouse/dbo/orders");
}

#[tokio::test]
async fn task_id_for_query_scope() {
    let mut config = base_config(ExportScope::Query {
        sql: "SELECT 1".into(),
        name: "my report".into(),
        partition_template: None,
    });
    config.target = "/output/{query}/".into();
    let tasks = ExecutionEngine::new()
        .plan(&config, &StaticCatalog::default())
        .await
        .unwrap();
    assert_eq!(tasks[0].id, "query:my report");
}

// ============================================================
// Multiple tables generate separate target prefixes
// ============================================================

#[tokio::test]
async fn each_table_gets_unique_target_prefix() {
    let catalog = StaticCatalog::new(vec![
        TableRef::new("dbo", "orders"),
        TableRef::new("dbo", "items"),
        TableRef::new("sales", "invoices"),
    ]);
    let config = base_config(ExportScope::Database {
        include: vec![],
        exclude: vec![],
    });
    let tasks = ExecutionEngine::new()
        .plan(&config, &catalog)
        .await
        .unwrap();
    let prefixes: Vec<_> = tasks.iter().map(|t| &t.target_prefix).collect();
    // All prefixes should be unique
    let unique: std::collections::HashSet<_> = prefixes.iter().collect();
    assert_eq!(unique.len(), 3);
}
