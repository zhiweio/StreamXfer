use streamxfer_core::config::{
    quote_ident, sanitize_path, CompressionCodec, ConsistencyMode, ExportConfig, ExportScope,
    OutputFormat, TableRef,
};

// ============================================================
// TableRef parsing - comprehensive edge cases
// ============================================================

#[test]
fn table_ref_single_part_defaults_to_dbo() {
    let table: TableRef = "orders".parse().unwrap();
    assert_eq!(table.schema, "dbo");
    assert_eq!(table.table, "orders");
    assert!(table.database.is_none());
}

#[test]
fn table_ref_two_part() {
    let table: TableRef = "sales.orders".parse().unwrap();
    assert_eq!(table.schema, "sales");
    assert_eq!(table.table, "orders");
    assert!(table.database.is_none());
}

#[test]
fn table_ref_three_part() {
    let table: TableRef = "warehouse.dbo.orders".parse().unwrap();
    assert_eq!(table.database.as_deref(), Some("warehouse"));
    assert_eq!(table.schema, "dbo");
    assert_eq!(table.table, "orders");
}

#[test]
fn table_ref_strips_brackets() {
    let table: TableRef = "[dbo].[order items]".parse().unwrap();
    assert_eq!(table.schema, "dbo");
    assert_eq!(table.table, "order items");
}

#[test]
fn table_ref_strips_double_quotes() {
    let table: TableRef = "\"dbo\".\"orders\"".parse().unwrap();
    assert_eq!(table.schema, "dbo");
    assert_eq!(table.table, "orders");
}

#[test]
fn table_ref_trims_whitespace() {
    let table: TableRef = "  dbo.orders  ".parse().unwrap();
    assert_eq!(table.schema, "dbo");
    assert_eq!(table.table, "orders");
}

#[test]
fn table_ref_empty_string_fails() {
    assert!("".parse::<TableRef>().is_err());
}

#[test]
fn table_ref_whitespace_only_fails() {
    assert!("   ".parse::<TableRef>().is_err());
}

#[test]
fn table_ref_too_many_parts_fails() {
    assert!("a.b.c.d".parse::<TableRef>().is_err());
}

#[test]
fn table_ref_sql_name_two_part() {
    let table = TableRef::new("dbo", "orders");
    assert_eq!(table.sql_name(), "[dbo].[orders]");
}

#[test]
fn table_ref_sql_name_three_part() {
    let table = TableRef::new("dbo", "orders").with_database("warehouse");
    assert_eq!(table.sql_name(), "[warehouse].[dbo].[orders]");
}

#[test]
fn table_ref_sql_name_escapes_brackets_in_identifiers() {
    let table = TableRef::new("dbo", "order]items");
    assert_eq!(table.sql_name(), "[dbo].[order]]items]");
}

#[test]
fn table_ref_path_name_sanitizes_special_chars() {
    let table = TableRef::new("dbo", "order/items");
    assert_eq!(table.path_name(), "dbo/order_items");
}

#[test]
fn table_ref_path_name_three_part() {
    let table = TableRef::new("sales", "items").with_database("warehouse");
    assert_eq!(table.path_name(), "warehouse/sales/items");
}

#[test]
fn table_ref_display_format() {
    let table = TableRef::new("dbo", "orders");
    assert_eq!(format!("{}", table), "dbo.orders");

    let table = TableRef::new("dbo", "orders").with_database("db");
    assert_eq!(format!("{}", table), "db.dbo.orders");
}

// ============================================================
// quote_ident and sanitize_path
// ============================================================

#[test]
fn quote_ident_wraps_in_brackets() {
    assert_eq!(quote_ident("orders"), "[orders]");
}

#[test]
fn quote_ident_escapes_closing_bracket() {
    assert_eq!(quote_ident("my]table"), "[my]]table]");
}

#[test]
fn sanitize_path_replaces_dangerous_chars() {
    assert_eq!(
        sanitize_path("a/b\\c:d*e?f\"g<h>i|j"),
        "a_b_c_d_e_f_g_h_i_j"
    );
}

#[test]
fn sanitize_path_preserves_normal_chars() {
    assert_eq!(sanitize_path("orders_2024"), "orders_2024");
}

// ============================================================
// ExportConfig validation
// ============================================================

fn valid_config() -> ExportConfig {
    ExportConfig {
        connection_url: "mssql://user:pass@host:1433/db".into(),
        scope: ExportScope::Table {
            table: TableRef::new("dbo", "orders"),
            predicate: None,
        },
        target: "/tmp/output/".into(),
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

#[test]
fn validate_accepts_valid_config() {
    valid_config().validate().unwrap();
}

#[test]
fn validate_rejects_empty_connection_url() {
    let mut config = valid_config();
    config.connection_url = "".into();
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_whitespace_connection_url() {
    let mut config = valid_config();
    config.connection_url = "   ".into();
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_empty_target() {
    let mut config = valid_config();
    config.target = "".into();
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_zero_table_concurrency() {
    let mut config = valid_config();
    config.table_concurrency = 0;
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_zero_partition_concurrency() {
    let mut config = valid_config();
    config.partition_concurrency_per_table = 0;
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_zero_io_concurrency() {
    let mut config = valid_config();
    config.global_io_concurrency = 0;
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_memory_below_64mb() {
    let mut config = valid_config();
    config.memory_limit_mb = 63;
    assert!(config.validate().is_err());
}

#[test]
fn validate_accepts_memory_at_64mb() {
    let mut config = valid_config();
    config.memory_limit_mb = 64;
    config.validate().unwrap();
}

#[test]
fn validate_rejects_query_with_empty_sql() {
    let mut config = valid_config();
    config.scope = ExportScope::Query {
        sql: "".into(),
        name: "valid_name".into(),
        partition_template: None,
    };
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_query_with_empty_name() {
    let mut config = valid_config();
    config.scope = ExportScope::Query {
        sql: "SELECT 1".into(),
        name: "".into(),
        partition_template: None,
    };
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_query_with_whitespace_name() {
    let mut config = valid_config();
    config.scope = ExportScope::Query {
        sql: "SELECT 1".into(),
        name: "   ".into(),
        partition_template: None,
    };
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_schema_with_empty_schema_name() {
    let mut config = valid_config();
    config.scope = ExportScope::Schema {
        schema: "".into(),
        include: vec![],
        exclude: vec![],
    };
    assert!(config.validate().is_err());
}

#[test]
fn validate_accepts_valid_query_scope() {
    let mut config = valid_config();
    config.scope = ExportScope::Query {
        sql: "SELECT * FROM orders".into(),
        name: "all_orders".into(),
        partition_template: None,
    };
    config.validate().unwrap();
}

#[test]
fn validate_accepts_database_scope() {
    let mut config = valid_config();
    config.scope = ExportScope::Database {
        include: vec![],
        exclude: vec![],
    };
    config.validate().unwrap();
}

// ============================================================
// normalized_connection_url
// ============================================================

#[test]
fn normalized_url_strips_pymssql_prefix() {
    let mut config = valid_config();
    config.connection_url = "mssql+pymssql:://user:pass@host:1433/db".into();
    assert_eq!(
        config.normalized_connection_url(),
        "mssql://user:pass@host:1433/db"
    );
}

#[test]
fn normalized_url_preserves_standard_url() {
    let config = valid_config();
    assert_eq!(
        config.normalized_connection_url(),
        "mssql://user:pass@host:1433/db"
    );
}

// ============================================================
// Serialization round-trip
// ============================================================

#[test]
fn export_config_serializes_and_deserializes() {
    let config = valid_config();
    let json = serde_json::to_string(&config).unwrap();
    let deserialized: ExportConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.connection_url, config.connection_url);
    assert_eq!(deserialized.target, config.target);
    assert_eq!(deserialized.memory_limit_mb, config.memory_limit_mb);
}

#[test]
fn export_config_deserializes_with_defaults() {
    let json = r#"{
        "connection_url": "mssql://u:p@h/db",
        "scope": {"type": "table", "table": {"schema": "dbo", "table": "t"}, "predicate": null},
        "target": "/tmp/out/"
    }"#;
    let config: ExportConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.format, OutputFormat::Parquet);
    assert_eq!(config.compression, CompressionCodec::Snappy);
    assert_eq!(config.consistency, ConsistencyMode::SnapshotTransaction);
    assert_eq!(config.batch_rows, 65_536);
    assert_eq!(config.memory_limit_mb, 512);
    assert_eq!(config.table_concurrency, 4);
    assert_eq!(config.partition_concurrency_per_table, 4);
    assert_eq!(config.global_io_concurrency, 16);
    assert!(!config.resume);
    assert!(config.checkpoint_dir.is_none());
}

#[test]
fn table_ref_serializes_and_deserializes() {
    let table = TableRef::new("sales", "items").with_database("warehouse");
    let json = serde_json::to_string(&table).unwrap();
    let deserialized: TableRef = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.database.as_deref(), Some("warehouse"));
    assert_eq!(deserialized.schema, "sales");
    assert_eq!(deserialized.table, "items");
}

#[test]
fn export_scope_variants_serialize_with_type_tag() {
    let table_scope = ExportScope::Table {
        table: TableRef::new("dbo", "orders"),
        predicate: Some("id > 10".into()),
    };
    let json = serde_json::to_string(&table_scope).unwrap();
    assert!(json.contains("\"type\":\"table\""));

    let query_scope = ExportScope::Query {
        sql: "SELECT 1".into(),
        name: "q".into(),
        partition_template: None,
    };
    let json = serde_json::to_string(&query_scope).unwrap();
    assert!(json.contains("\"type\":\"query\""));

    let schema_scope = ExportScope::Schema {
        schema: "dbo".into(),
        include: vec![],
        exclude: vec![],
    };
    let json = serde_json::to_string(&schema_scope).unwrap();
    assert!(json.contains("\"type\":\"schema\""));

    let db_scope = ExportScope::Database {
        include: vec!["a.*".into()],
        exclude: vec![],
    };
    let json = serde_json::to_string(&db_scope).unwrap();
    assert!(json.contains("\"type\":\"database\""));
}
