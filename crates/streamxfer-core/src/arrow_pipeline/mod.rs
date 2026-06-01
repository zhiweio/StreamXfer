use crate::schema::mapping::{ArrowType, SqlColumn};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchBudget {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for BatchBudget {
    fn default() -> Self {
        Self {
            max_rows: 65_536,
            max_bytes: 128 * 1024 * 1024,
        }
    }
}

impl BatchBudget {
    pub fn should_flush(&self, rows: usize, bytes: usize) -> bool {
        rows >= self.max_rows || bytes >= self.max_bytes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrowField {
    pub name: String,
    pub data_type: ArrowType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrowSchema {
    pub fields: Vec<ArrowField>,
}

impl ArrowSchema {
    pub fn from_sql_columns(columns: &[SqlColumn]) -> Self {
        Self {
            fields: columns
                .iter()
                .map(|column| ArrowField {
                    name: column.name.clone(),
                    data_type: column.arrow_type(),
                    nullable: column.nullable,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordBatchMeta {
    pub schema: ArrowSchema,
    pub rows: usize,
    pub estimated_bytes: usize,
}
