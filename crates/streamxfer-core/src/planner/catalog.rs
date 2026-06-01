use crate::config::TableRef;
use crate::error::{Result, StreamXferError};
use async_trait::async_trait;

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn list_schema_tables(&self, schema: &str) -> Result<Vec<TableRef>>;
    async fn list_database_tables(&self) -> Result<Vec<TableRef>>;
}

#[derive(Debug, Clone, Default)]
pub struct StaticCatalog {
    tables: Vec<TableRef>,
}

impl StaticCatalog {
    pub fn new(tables: Vec<TableRef>) -> Self {
        Self { tables }
    }
}

#[async_trait]
impl Catalog for StaticCatalog {
    async fn list_schema_tables(&self, schema: &str) -> Result<Vec<TableRef>> {
        Ok(self
            .tables
            .iter()
            .filter(|table| table.schema.eq_ignore_ascii_case(schema))
            .cloned()
            .collect())
    }
    async fn list_database_tables(&self) -> Result<Vec<TableRef>> {
        Ok(self.tables.clone())
    }
}

pub fn list_tables_sql(schema: Option<&str>) -> String {
    match schema {
        Some(_) => "select table_schema, table_name from information_schema.tables where table_type = 'BASE TABLE' and table_schema = @P1 order by table_schema, table_name".into(),
        None => "select table_schema, table_name from information_schema.tables where table_type = 'BASE TABLE' order by table_schema, table_name".into(),
    }
}

pub fn ensure_non_empty(scope: &str, tables: &[TableRef]) -> Result<()> {
    if tables.is_empty() {
        return Err(StreamXferError::Catalog(format!(
            "no exportable tables found for {scope}"
        )));
    }
    Ok(())
}
