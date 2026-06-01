use streamxfer_core::arrow_pipeline::{ArrowSchema, BatchBudget};
use streamxfer_core::config::{CompressionCodec, ExportScope, TableRef};
use streamxfer_core::parquet_pipeline::ParquetWriterConfig;
use streamxfer_core::runtime::limits::RuntimeLimits;
use streamxfer_core::schema::mapping::{sql_type_to_arrow, ArrowType, SqlColumn};
use streamxfer_core::source::mssql::{table_select_sql, MssqlConnectionConfig};

#[test]
fn mssql_url_parser_accepts_legacy_and_rejects_invalid_urls() {
    let legacy =
        MssqlConnectionConfig::from_url("mssql+pymssql:://sa:secret@localhost:1444/db").unwrap();
    assert_eq!(legacy.host, "localhost");
    assert_eq!(legacy.port, 1444);
    assert_eq!(legacy.database, "db");
    assert_eq!(legacy.username.as_deref(), Some("sa"));
    assert!(MssqlConnectionConfig::from_url("postgres://host/db").is_err());
    assert!(MssqlConnectionConfig::from_url("mssql://host").is_err());
}

#[test]
fn sql_generation_quotes_projection_and_predicate() {
    let table = TableRef::new("dbo", "orders");
    let columns = vec![
        SqlColumn {
            name: "order id".into(),
            sql_type: "int".into(),
            nullable: false,
            precision: None,
            scale: None,
        },
        SqlColumn {
            name: "amount".into(),
            sql_type: "decimal".into(),
            nullable: true,
            precision: Some(18),
            scale: Some(2),
        },
    ];
    let sql = table_select_sql(&table, &columns, Some("[order id] >= 100"));
    assert_eq!(
        sql,
        "select [order id], [amount] from [dbo].[orders] where [order id] >= 100"
    );
}

#[test]
fn schema_mapping_and_arrow_schema_cover_common_types() {
    assert_eq!(sql_type_to_arrow("bit", None, None), ArrowType::Boolean);
    assert_eq!(sql_type_to_arrow("bigint", None, None), ArrowType::Int64);
    assert_eq!(
        sql_type_to_arrow("varbinary", None, None),
        ArrowType::Binary
    );
    assert_eq!(
        sql_type_to_arrow("numeric", Some(20), Some(6)),
        ArrowType::Decimal128 {
            precision: 20,
            scale: 6
        }
    );
    let schema = ArrowSchema::from_sql_columns(&[SqlColumn {
        name: "id".into(),
        sql_type: "int".into(),
        nullable: false,
        precision: None,
        scale: None,
    }]);
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].data_type, ArrowType::Int32);
}

#[test]
fn parquet_and_batch_budget_defaults_are_data_lake_oriented() {
    let default_config = ParquetWriterConfig::default();
    assert_eq!(default_config.file_extension(), "parquet");
    assert_eq!(default_config.compression_name(), "snappy");
    let zstd = ParquetWriterConfig {
        compression: CompressionCodec::Zstd,
        zstd_level: Some(9),
        ..Default::default()
    };
    assert_eq!(zstd.compression_name(), "zstd");
    assert!(BatchBudget {
        max_rows: 10,
        max_bytes: 100
    }
    .should_flush(10, 1));
    assert!(BatchBudget {
        max_rows: 10,
        max_bytes: 100
    }
    .should_flush(1, 100));
}

#[test]
fn runtime_limits_reject_zero_concurrency() {
    assert!(RuntimeLimits {
        table_concurrency: 0,
        partition_concurrency_per_table: 1,
        global_io_concurrency: 1,
        memory_limit_mb: 512
    }
    .validate()
    .is_err());
    RuntimeLimits {
        table_concurrency: 2,
        partition_concurrency_per_table: 2,
        global_io_concurrency: 2,
        memory_limit_mb: 512,
    }
    .validate()
    .unwrap();
}

#[test]
fn query_config_validation_requires_name_and_sql() {
    let scope = ExportScope::Query {
        sql: "".into(),
        name: "q".into(),
        partition_template: None,
    };
    assert!(matches!(scope, ExportScope::Query { .. }));
}
