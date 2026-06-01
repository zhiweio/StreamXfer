use streamxfer_core::arrow_pipeline::{ArrowSchema, BatchBudget, RecordBatchMeta};
use streamxfer_core::schema::mapping::{sql_type_to_arrow, ArrowType, SqlColumn};

// ============================================================
// SQL type to Arrow type mapping - comprehensive coverage
// ============================================================

#[test]
fn maps_bit_to_boolean() {
    assert_eq!(sql_type_to_arrow("bit", None, None), ArrowType::Boolean);
}

#[test]
fn maps_tinyint_to_uint8() {
    assert_eq!(sql_type_to_arrow("tinyint", None, None), ArrowType::UInt8);
}

#[test]
fn maps_smallint_to_int16() {
    assert_eq!(sql_type_to_arrow("smallint", None, None), ArrowType::Int16);
}

#[test]
fn maps_int_to_int32() {
    assert_eq!(sql_type_to_arrow("int", None, None), ArrowType::Int32);
}

#[test]
fn maps_bigint_to_int64() {
    assert_eq!(sql_type_to_arrow("bigint", None, None), ArrowType::Int64);
}

#[test]
fn maps_real_to_float32() {
    assert_eq!(sql_type_to_arrow("real", None, None), ArrowType::Float32);
}

#[test]
fn maps_float_to_float64() {
    assert_eq!(sql_type_to_arrow("float", None, None), ArrowType::Float64);
}

#[test]
fn maps_decimal_with_precision_and_scale() {
    assert_eq!(
        sql_type_to_arrow("decimal", Some(18), Some(2)),
        ArrowType::Decimal128 {
            precision: 18,
            scale: 2
        }
    );
}

#[test]
fn maps_decimal_without_precision_uses_defaults() {
    assert_eq!(
        sql_type_to_arrow("decimal", None, None),
        ArrowType::Decimal128 {
            precision: 38,
            scale: 10
        }
    );
}

#[test]
fn maps_numeric_to_decimal() {
    assert_eq!(
        sql_type_to_arrow("numeric", Some(10), Some(4)),
        ArrowType::Decimal128 {
            precision: 10,
            scale: 4
        }
    );
}

#[test]
fn maps_money_to_decimal() {
    assert_eq!(
        sql_type_to_arrow("money", None, None),
        ArrowType::Decimal128 {
            precision: 38,
            scale: 10
        }
    );
}

#[test]
fn maps_smallmoney_to_decimal() {
    assert_eq!(
        sql_type_to_arrow("smallmoney", Some(10), Some(4)),
        ArrowType::Decimal128 {
            precision: 10,
            scale: 4
        }
    );
}

#[test]
fn maps_date_to_date32() {
    assert_eq!(sql_type_to_arrow("date", None, None), ArrowType::Date32);
}

#[test]
fn maps_datetime_to_timestamp_nanos() {
    assert_eq!(
        sql_type_to_arrow("datetime", None, None),
        ArrowType::TimestampNanos
    );
}

#[test]
fn maps_datetime2_to_timestamp_nanos() {
    assert_eq!(
        sql_type_to_arrow("datetime2", None, None),
        ArrowType::TimestampNanos
    );
}

#[test]
fn maps_smalldatetime_to_timestamp_nanos() {
    assert_eq!(
        sql_type_to_arrow("smalldatetime", None, None),
        ArrowType::TimestampNanos
    );
}

#[test]
fn maps_datetimeoffset_to_timestamp_nanos() {
    assert_eq!(
        sql_type_to_arrow("datetimeoffset", None, None),
        ArrowType::TimestampNanos
    );
}

#[test]
fn maps_time_to_timestamp_nanos() {
    assert_eq!(
        sql_type_to_arrow("time", None, None),
        ArrowType::TimestampNanos
    );
}

#[test]
fn maps_binary_to_binary() {
    assert_eq!(sql_type_to_arrow("binary", None, None), ArrowType::Binary);
}

#[test]
fn maps_varbinary_to_binary() {
    assert_eq!(
        sql_type_to_arrow("varbinary", None, None),
        ArrowType::Binary
    );
}

#[test]
fn maps_image_to_binary() {
    assert_eq!(sql_type_to_arrow("image", None, None), ArrowType::Binary);
}

#[test]
fn maps_timestamp_to_binary() {
    assert_eq!(
        sql_type_to_arrow("timestamp", None, None),
        ArrowType::Binary
    );
}

#[test]
fn maps_rowversion_to_binary() {
    assert_eq!(
        sql_type_to_arrow("rowversion", None, None),
        ArrowType::Binary
    );
}

#[test]
fn maps_varchar_to_utf8() {
    assert_eq!(sql_type_to_arrow("varchar", None, None), ArrowType::Utf8);
}

#[test]
fn maps_nvarchar_to_utf8() {
    assert_eq!(sql_type_to_arrow("nvarchar", None, None), ArrowType::Utf8);
}

#[test]
fn maps_char_to_utf8() {
    assert_eq!(sql_type_to_arrow("char", None, None), ArrowType::Utf8);
}

#[test]
fn maps_nchar_to_utf8() {
    assert_eq!(sql_type_to_arrow("nchar", None, None), ArrowType::Utf8);
}

#[test]
fn maps_text_to_utf8() {
    assert_eq!(sql_type_to_arrow("text", None, None), ArrowType::Utf8);
}

#[test]
fn maps_ntext_to_utf8() {
    assert_eq!(sql_type_to_arrow("ntext", None, None), ArrowType::Utf8);
}

#[test]
fn maps_xml_to_utf8() {
    assert_eq!(sql_type_to_arrow("xml", None, None), ArrowType::Utf8);
}

#[test]
fn maps_uniqueidentifier_to_utf8() {
    assert_eq!(
        sql_type_to_arrow("uniqueidentifier", None, None),
        ArrowType::Utf8
    );
}

#[test]
fn maps_sql_variant_to_utf8() {
    assert_eq!(
        sql_type_to_arrow("sql_variant", None, None),
        ArrowType::Utf8
    );
}

#[test]
fn maps_unknown_type_to_utf8() {
    assert_eq!(sql_type_to_arrow("geometry", None, None), ArrowType::Utf8);
    assert_eq!(
        sql_type_to_arrow("hierarchyid", None, None),
        ArrowType::Utf8
    );
}

#[test]
fn mapping_is_case_insensitive() {
    assert_eq!(sql_type_to_arrow("INT", None, None), ArrowType::Int32);
    assert_eq!(sql_type_to_arrow("BigInt", None, None), ArrowType::Int64);
    assert_eq!(sql_type_to_arrow("NVARCHAR", None, None), ArrowType::Utf8);
    assert_eq!(
        sql_type_to_arrow("DateTime2", None, None),
        ArrowType::TimestampNanos
    );
}

// ============================================================
// SqlColumn.arrow_type()
// ============================================================

#[test]
fn sql_column_arrow_type_delegates_correctly() {
    let col = SqlColumn {
        name: "price".into(),
        sql_type: "decimal".into(),
        nullable: true,
        precision: Some(18),
        scale: Some(4),
    };
    assert_eq!(
        col.arrow_type(),
        ArrowType::Decimal128 {
            precision: 18,
            scale: 4
        }
    );
}

// ============================================================
// ArrowSchema
// ============================================================

#[test]
fn arrow_schema_from_empty_columns() {
    let schema = ArrowSchema::from_sql_columns(&[]);
    assert!(schema.fields.is_empty());
}

#[test]
fn arrow_schema_preserves_column_order() {
    let columns = vec![
        SqlColumn {
            name: "id".into(),
            sql_type: "int".into(),
            nullable: false,
            precision: None,
            scale: None,
        },
        SqlColumn {
            name: "name".into(),
            sql_type: "nvarchar".into(),
            nullable: true,
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
    let schema = ArrowSchema::from_sql_columns(&columns);
    assert_eq!(schema.fields.len(), 3);
    assert_eq!(schema.fields[0].name, "id");
    assert_eq!(schema.fields[0].data_type, ArrowType::Int32);
    assert!(!schema.fields[0].nullable);
    assert_eq!(schema.fields[1].name, "name");
    assert_eq!(schema.fields[1].data_type, ArrowType::Utf8);
    assert!(schema.fields[1].nullable);
    assert_eq!(schema.fields[2].name, "amount");
    assert_eq!(
        schema.fields[2].data_type,
        ArrowType::Decimal128 {
            precision: 18,
            scale: 2
        }
    );
}

// ============================================================
// BatchBudget
// ============================================================

#[test]
fn batch_budget_default_values() {
    let budget = BatchBudget::default();
    assert_eq!(budget.max_rows, 65_536);
    assert_eq!(budget.max_bytes, 128 * 1024 * 1024);
}

#[test]
fn batch_budget_flushes_on_row_limit() {
    let budget = BatchBudget {
        max_rows: 100,
        max_bytes: 1_000_000,
    };
    assert!(!budget.should_flush(99, 0));
    assert!(budget.should_flush(100, 0));
    assert!(budget.should_flush(101, 0));
}

#[test]
fn batch_budget_flushes_on_byte_limit() {
    let budget = BatchBudget {
        max_rows: 100,
        max_bytes: 1000,
    };
    assert!(!budget.should_flush(0, 999));
    assert!(budget.should_flush(0, 1000));
    assert!(budget.should_flush(0, 1001));
}

#[test]
fn batch_budget_flushes_when_both_exceeded() {
    let budget = BatchBudget {
        max_rows: 10,
        max_bytes: 100,
    };
    assert!(budget.should_flush(10, 100));
}

#[test]
fn batch_budget_does_not_flush_when_under_both_limits() {
    let budget = BatchBudget {
        max_rows: 10,
        max_bytes: 100,
    };
    assert!(!budget.should_flush(9, 99));
}

// ============================================================
// RecordBatchMeta
// ============================================================

#[test]
fn record_batch_meta_construction() {
    let schema = ArrowSchema::from_sql_columns(&[SqlColumn {
        name: "id".into(),
        sql_type: "int".into(),
        nullable: false,
        precision: None,
        scale: None,
    }]);
    let meta = RecordBatchMeta {
        schema: schema.clone(),
        rows: 1000,
        estimated_bytes: 4000,
    };
    assert_eq!(meta.rows, 1000);
    assert_eq!(meta.estimated_bytes, 4000);
    assert_eq!(meta.schema.fields.len(), 1);
}
