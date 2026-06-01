use crate::error::{Result, StreamXferError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeLimits {
    pub table_concurrency: usize,
    pub partition_concurrency_per_table: usize,
    pub global_io_concurrency: usize,
    pub memory_limit_mb: usize,
}
impl RuntimeLimits {
    pub fn validate(&self) -> Result<()> {
        if self.table_concurrency == 0
            || self.partition_concurrency_per_table == 0
            || self.global_io_concurrency == 0
        {
            return Err(StreamXferError::InvalidConfig(
                "runtime concurrency must be non-zero".into(),
            ));
        }
        let worst_case_tasks = self.table_concurrency * self.partition_concurrency_per_table;
        if worst_case_tasks > self.global_io_concurrency * 4 {
            tracing::warn!(
                worst_case_tasks,
                global_io_concurrency = self.global_io_concurrency,
                "table and partition concurrency may exceed io budget"
            );
        }
        Ok(())
    }
}
