use crate::config::{sanitize_path, ExportConfig, ExportScope, TableRef};
use crate::error::{Result, StreamXferError};
use crate::planner::catalog::{ensure_non_empty, Catalog};
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskKind {
    Table,
    Query,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportTask {
    pub id: String,
    pub kind: TaskKind,
    pub table: Option<TableRef>,
    pub query_name: Option<String>,
    pub sql: String,
    pub target_prefix: String,
    pub partition_hint: Option<String>,
}

#[derive(Debug, Default)]
pub struct ScopePlanner;

impl ScopePlanner {
    pub async fn plan<C: Catalog>(
        &self,
        config: &ExportConfig,
        catalog: &C,
    ) -> Result<Vec<ExportTask>> {
        config.validate()?;
        match &config.scope {
            ExportScope::Table { table, predicate } => Ok(vec![table_task(
                table.clone(),
                predicate.clone(),
                &config.target,
            )]),
            ExportScope::Query {
                sql,
                name,
                partition_template,
            } => Ok(vec![query_task(
                name,
                sql,
                partition_template.clone(),
                &config.target,
            )]),
            ExportScope::Schema {
                schema,
                include,
                exclude,
            } => {
                let tables =
                    filter_tables(catalog.list_schema_tables(schema).await?, include, exclude)?;
                ensure_non_empty(schema, &tables)?;
                Ok(tables
                    .into_iter()
                    .map(|table| table_task(table, None, &config.target))
                    .collect())
            }
            ExportScope::Database { include, exclude } => {
                let tables =
                    filter_tables(catalog.list_database_tables().await?, include, exclude)?;
                ensure_non_empty("database", &tables)?;
                Ok(tables
                    .into_iter()
                    .map(|table| table_task(table, None, &config.target))
                    .collect())
            }
        }
    }
}

fn table_task(table: TableRef, predicate: Option<String>, target_template: &str) -> ExportTask {
    let sql = match predicate {
        Some(predicate) => format!("select * from {} where {predicate}", table.sql_name()),
        None => format!("select * from {}", table.sql_name()),
    };
    let target_prefix = render_target_template(target_template, Some(&table), None, None, None);
    let id = format!("table:{}", table.path_name());
    ExportTask {
        id,
        kind: TaskKind::Table,
        table: Some(table),
        query_name: None,
        sql,
        target_prefix,
        partition_hint: None,
    }
}

fn query_task(
    name: &str,
    sql: &str,
    partition_hint: Option<String>,
    target_template: &str,
) -> ExportTask {
    ExportTask {
        id: format!("query:{}", sanitize_path(name)),
        kind: TaskKind::Query,
        table: None,
        query_name: Some(name.to_string()),
        sql: sql.to_string(),
        target_prefix: render_target_template(target_template, None, Some(name), None, None),
        partition_hint,
    }
}

pub fn render_target_template(
    template: &str,
    table: Option<&TableRef>,
    query: Option<&str>,
    partition: Option<&str>,
    file_index: Option<usize>,
) -> String {
    let mut rendered = template.to_string();
    if let Some(table) = table {
        rendered = rendered.replace("{database}", table.database.as_deref().unwrap_or("default"));
        rendered = rendered.replace("{schema}", &sanitize_path(&table.schema));
        rendered = rendered.replace("{table}", &sanitize_path(&table.table));
    }
    if let Some(query) = query {
        rendered = rendered.replace("{query}", &sanitize_path(query));
    }
    if let Some(partition) = partition {
        rendered = rendered.replace("{partition}", &sanitize_path(partition));
    }
    if let Some(file_index) = file_index {
        rendered = rendered.replace("{file_index}", &format!("{file_index:08}"));
    }
    rendered
}

fn filter_tables(
    tables: Vec<TableRef>,
    include: &[String],
    exclude: &[String],
) -> Result<Vec<TableRef>> {
    let include_set = build_globset(include)?;
    let exclude_set = build_globset(exclude)?;
    Ok(tables
        .into_iter()
        .filter(|table| {
            let name = table.to_string();
            (include.is_empty() || include_set.is_match(&name)) && !exclude_set.is_match(&name)
        })
        .collect())
}

fn build_globset(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            Glob::new(pattern).map_err(|err| StreamXferError::InvalidConfig(err.to_string()))?,
        );
    }
    builder
        .build()
        .map_err(|err| StreamXferError::InvalidConfig(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompressionCodec, ConsistencyMode, OutputFormat};
    use crate::planner::catalog::StaticCatalog;
    fn config(scope: ExportScope) -> ExportConfig {
        ExportConfig {
            connection_url: "mssql://u:p@host/db".into(),
            scope,
            target: "s3://lake/raw/{schema}/{table}/".into(),
            format: OutputFormat::Parquet,
            compression: CompressionCodec::Snappy,
            consistency: ConsistencyMode::SnapshotTransaction,
            target_file_size: 128,
            batch_rows: 10,
            memory_limit_mb: 512,
            table_concurrency: 2,
            partition_concurrency_per_table: 2,
            global_io_concurrency: 4,
            checkpoint_dir: None,
            resume: false,
        }
    }
    #[tokio::test]
    async fn schema_scope_expands_tables_with_separate_targets() {
        let catalog = StaticCatalog::new(vec![
            TableRef::new("dbo", "a"),
            TableRef::new("dbo", "b"),
            TableRef::new("sales", "c"),
        ]);
        let planner = ScopePlanner;
        let tasks = planner
            .plan(
                &config(ExportScope::Schema {
                    schema: "dbo".into(),
                    include: vec![],
                    exclude: vec!["*.b".into()],
                }),
                &catalog,
            )
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].target_prefix, "s3://lake/raw/dbo/a/");
    }
}
