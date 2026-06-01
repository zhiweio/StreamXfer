use crate::error::{Result, StreamXferError};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    Parquet,
    Csv,
    Tsv,
    Json,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Parquet
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionCodec {
    None,
    Snappy,
    Zstd,
    Gzip,
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::Snappy
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsistencyMode {
    None,
    SnapshotTransaction,
    DatabaseSnapshot,
    HighWatermark,
}

impl Default for ConsistencyMode {
    fn default() -> Self {
        Self::SnapshotTransaction
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub database: Option<String>,
    pub schema: String,
    pub table: String,
}

impl TableRef {
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: None,
            schema: schema.into(),
            table: table.into(),
        }
    }
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }
    pub fn sql_name(&self) -> String {
        match &self.database {
            Some(database) => format!(
                "{}.{}.{}",
                quote_ident(database),
                quote_ident(&self.schema),
                quote_ident(&self.table)
            ),
            None => format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table)),
        }
    }
    pub fn path_name(&self) -> String {
        match &self.database {
            Some(database) => format!(
                "{}/{}/{}",
                sanitize_path(database),
                sanitize_path(&self.schema),
                sanitize_path(&self.table)
            ),
            None => format!(
                "{}/{}",
                sanitize_path(&self.schema),
                sanitize_path(&self.table)
            ),
        }
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.database {
            Some(database) => write!(f, "{}.{}.{}", database, self.schema, self.table),
            None => write!(f, "{}.{}", self.schema, self.table),
        }
    }
}

impl FromStr for TableRef {
    type Err = StreamXferError;
    fn from_str(input: &str) -> Result<Self> {
        let mut s = input.trim().to_string();
        if s.is_empty() {
            return Err(StreamXferError::InvalidIdentifier(input.to_string()));
        }
        s = s.replace(['[', ']', '"'], "");
        let parts: Vec<_> = s.split('.').filter(|part| !part.is_empty()).collect();
        match parts.as_slice() {
            [schema, table] => Ok(TableRef::new(*schema, *table)),
            [database, schema, table] => {
                Ok(TableRef::new(*schema, *table).with_database(*database))
            }
            [table] => Ok(TableRef::new("dbo", *table)),
            _ => Err(StreamXferError::InvalidIdentifier(input.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExportScope {
    Table {
        table: TableRef,
        predicate: Option<String>,
    },
    Query {
        sql: String,
        name: String,
        partition_template: Option<String>,
    },
    Schema {
        schema: String,
        include: Vec<String>,
        exclude: Vec<String>,
    },
    Database {
        include: Vec<String>,
        exclude: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    pub connection_url: String,
    pub scope: ExportScope,
    pub target: String,
    #[serde(default)]
    pub format: OutputFormat,
    #[serde(default)]
    pub compression: CompressionCodec,
    #[serde(default)]
    pub consistency: ConsistencyMode,
    #[serde(default = "default_target_file_size")]
    pub target_file_size: u64,
    /// None means not explicitly set by user — file splitting uses target_file_size only.
    #[serde(default)]
    pub max_rows_per_file: Option<usize>,
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    #[serde(default = "default_memory_limit_mb")]
    pub memory_limit_mb: usize,
    #[serde(default = "default_table_concurrency")]
    pub table_concurrency: usize,
    #[serde(default = "default_partition_concurrency")]
    pub partition_concurrency_per_table: usize,
    #[serde(default = "default_global_io_concurrency")]
    pub global_io_concurrency: usize,
    pub checkpoint_dir: Option<String>,
    #[serde(default)]
    pub resume: bool,
}

impl ExportConfig {
    pub fn validate(&self) -> Result<()> {
        if self.connection_url.trim().is_empty() {
            return Err(StreamXferError::InvalidConfig(
                "connection_url is required".into(),
            ));
        }
        if self.target.trim().is_empty() {
            return Err(StreamXferError::InvalidConfig("target is required".into()));
        }
        if self.table_concurrency == 0
            || self.partition_concurrency_per_table == 0
            || self.global_io_concurrency == 0
        {
            return Err(StreamXferError::InvalidConfig(
                "concurrency values must be greater than zero".into(),
            ));
        }
        if self.memory_limit_mb < 64 {
            return Err(StreamXferError::InvalidConfig(
                "memory_limit_mb must be at least 64".into(),
            ));
        }
        match &self.scope {
            ExportScope::Query { name, sql, .. }
                if name.trim().is_empty() || sql.trim().is_empty() =>
            {
                Err(StreamXferError::InvalidConfig(
                    "query scope requires both name and sql".into(),
                ))
            }
            ExportScope::Schema { schema, .. } if schema.trim().is_empty() => Err(
                StreamXferError::InvalidConfig("schema scope requires schema".into()),
            ),
            _ => Ok(()),
        }
    }
    pub fn normalized_connection_url(&self) -> String {
        self.connection_url.replace("mssql+pymssql:://", "mssql://")
    }
}

pub fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

pub fn sanitize_path(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => ch,
        })
        .collect()
}

fn default_target_file_size() -> u64 {
    256 * 1024 * 1024
}
fn default_batch_rows() -> usize {
    65_536
}
fn default_memory_limit_mb() -> usize {
    512
}
fn default_table_concurrency() -> usize {
    4
}
fn default_partition_concurrency() -> usize {
    4
}
fn default_global_io_concurrency() -> usize {
    16
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_legacy_table_names() {
        assert_eq!(
            "dbo.orders".parse::<TableRef>().unwrap().sql_name(),
            "[dbo].[orders]"
        );
        assert_eq!(
            "[sales].[orders]".parse::<TableRef>().unwrap().path_name(),
            "sales/orders"
        );
    }
}
