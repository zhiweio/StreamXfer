use streamxfer_core::config::TableRef;
use streamxfer_core::schema::mapping::SqlColumn;
use streamxfer_core::source::mssql::{table_columns_sql, table_select_sql, MssqlConnectionConfig};

// ============================================================
// MssqlConnectionConfig::from_url - comprehensive parsing
// ============================================================

#[test]
fn parses_standard_mssql_url() {
    let config = MssqlConnectionConfig::from_url("mssql://sa:pass@localhost:1433/mydb").unwrap();
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 1433);
    assert_eq!(config.database, "mydb");
    assert_eq!(config.username.as_deref(), Some("sa"));
    assert_eq!(config.password.as_deref(), Some("pass"));
}

#[test]
fn parses_legacy_pymssql_url() {
    let config = MssqlConnectionConfig::from_url(
        "mssql+pymssql:://admin:secret@host.example.com:1444/warehouse",
    )
    .unwrap();
    assert_eq!(config.host, "host.example.com");
    assert_eq!(config.port, 1444);
    assert_eq!(config.database, "warehouse");
    assert_eq!(config.username.as_deref(), Some("admin"));
    assert_eq!(config.password.as_deref(), Some("secret"));
}

#[test]
fn parses_url_with_default_port() {
    let config = MssqlConnectionConfig::from_url("mssql://sa:pass@myhost/db").unwrap();
    assert_eq!(config.host, "myhost");
    assert_eq!(config.port, 1433);
}

#[test]
fn parses_url_without_auth() {
    let config = MssqlConnectionConfig::from_url("mssql://myserver:5000/testdb").unwrap();
    assert_eq!(config.host, "myserver");
    assert_eq!(config.port, 5000);
    assert_eq!(config.database, "testdb");
    assert!(config.username.is_none());
    assert!(config.password.is_none());
}

#[test]
fn parses_url_with_special_chars_in_password() {
    let config = MssqlConnectionConfig::from_url("mssql://user:p@ss:w0rd@host/db").unwrap();
    assert_eq!(config.host, "host");
    assert_eq!(config.username.as_deref(), Some("user"));
    assert_eq!(config.password.as_deref(), Some("p@ss:w0rd"));
}

#[test]
fn rejects_non_mssql_scheme() {
    assert!(MssqlConnectionConfig::from_url("postgres://host/db").is_err());
    assert!(MssqlConnectionConfig::from_url("mysql://host/db").is_err());
    assert!(MssqlConnectionConfig::from_url("http://host/db").is_err());
}

#[test]
fn rejects_url_without_database() {
    assert!(MssqlConnectionConfig::from_url("mssql://host").is_err());
}

#[test]
fn rejects_url_with_empty_database() {
    assert!(MssqlConnectionConfig::from_url("mssql://host/").is_err());
}

#[test]
fn rejects_url_with_empty_host() {
    assert!(MssqlConnectionConfig::from_url("mssql://user:pass@/db").is_err());
}

#[test]
fn trust_cert_is_true_by_default() {
    let config = MssqlConnectionConfig::from_url("mssql://sa:pass@host/db").unwrap();
    assert!(config.trust_cert);
}

#[test]
fn tiberius_config_builds_without_panic() {
    let config = MssqlConnectionConfig::from_url("mssql://sa:pass@myhost:2000/testdb").unwrap();
    // tiberius Config uses setter-style API, so we just verify it doesn't panic
    let _tib = config.tiberius_config();
}

// ============================================================
// SQL generation
// ============================================================

#[test]
fn table_columns_sql_generates_correct_query() {
    let table = TableRef::new("dbo", "orders");
    let sql = table_columns_sql(&table);
    assert!(sql.contains("information_schema.columns"));
    assert!(sql.contains("table_schema = N'dbo'"));
    assert!(sql.contains("table_name = N'orders'"));
    assert!(sql.contains("order by ordinal_position"));
}

#[test]
fn table_columns_sql_escapes_single_quotes() {
    let table = TableRef::new("dbo", "it's_table");
    let sql = table_columns_sql(&table);
    assert!(sql.contains("table_name = N'it''s_table'"));
}

#[test]
fn table_select_sql_with_empty_columns_uses_star() {
    let table = TableRef::new("dbo", "orders");
    let sql = table_select_sql(&table, &[], None);
    assert_eq!(sql, "select * from [dbo].[orders]");
}

#[test]
fn table_select_sql_with_columns_projects() {
    let table = TableRef::new("dbo", "orders");
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
    ];
    let sql = table_select_sql(&table, &columns, None);
    assert_eq!(sql, "select [id], [name] from [dbo].[orders]");
}

#[test]
fn table_select_sql_with_predicate() {
    let table = TableRef::new("sales", "items");
    let sql = table_select_sql(&table, &[], Some("price > 100"));
    assert_eq!(sql, "select * from [sales].[items] where price > 100");
}

#[test]
fn table_select_sql_with_columns_and_predicate() {
    let table = TableRef::new("dbo", "orders");
    let columns = vec![SqlColumn {
        name: "amount".into(),
        sql_type: "decimal".into(),
        nullable: false,
        precision: Some(18),
        scale: Some(2),
    }];
    let sql = table_select_sql(&table, &columns, Some("amount > 0"));
    assert_eq!(sql, "select [amount] from [dbo].[orders] where amount > 0");
}

#[test]
fn table_select_sql_quotes_column_names_with_spaces() {
    let table = TableRef::new("dbo", "orders");
    let columns = vec![SqlColumn {
        name: "order id".into(),
        sql_type: "int".into(),
        nullable: false,
        precision: None,
        scale: None,
    }];
    let sql = table_select_sql(&table, &columns, None);
    assert_eq!(sql, "select [order id] from [dbo].[orders]");
}
