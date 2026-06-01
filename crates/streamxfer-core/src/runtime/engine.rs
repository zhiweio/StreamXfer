use crate::config::ExportConfig;
use crate::error::Result;
use crate::planner::catalog::Catalog;
use crate::planner::scope::{ExportTask, ScopePlanner};
use crate::runtime::limits::RuntimeLimits;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSummary {
    pub planned_tasks: usize,
    pub completed_tasks: usize,
    pub skipped_tasks: usize,
    pub bytes_written: u64,
}
#[derive(Debug, Default)]
pub struct ExecutionEngine {
    planner: ScopePlanner,
}
impl ExecutionEngine {
    pub fn new() -> Self {
        Self {
            planner: ScopePlanner,
        }
    }
    pub async fn plan<C: Catalog>(
        &self,
        config: &ExportConfig,
        catalog: &C,
    ) -> Result<Vec<ExportTask>> {
        self.planner.plan(config, catalog).await
    }
    pub async fn dry_run<C: Catalog>(
        &self,
        config: &ExportConfig,
        catalog: &C,
    ) -> Result<ExecutionSummary> {
        RuntimeLimits {
            table_concurrency: config.table_concurrency,
            partition_concurrency_per_table: config.partition_concurrency_per_table,
            global_io_concurrency: config.global_io_concurrency,
            memory_limit_mb: config.memory_limit_mb,
        }
        .validate()?;
        let tasks = self.plan(config, catalog).await?;
        Ok(ExecutionSummary {
            planned_tasks: tasks.len(),
            completed_tasks: 0,
            skipped_tasks: 0,
            bytes_written: 0,
        })
    }
}
