use anyhow::Result;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use streamxfer_core::config::{
    CompressionCodec, ConsistencyMode, ExportConfig, ExportScope, OutputFormat, TableRef,
};
use streamxfer_core::planner::catalog::StaticCatalog;
use streamxfer_core::{ExecutionEngine, TaskExecutor};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "stx",
    version,
    about = "Stream SQL Server data to local or object storage"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, global = true, default_value_t = false)]
    dry_run: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    Table(TableArgs),
    Query(QueryArgs),
    Schema(SchemaArgs),
    Database(DatabaseArgs),
}

#[derive(Debug, Args)]
struct CommonArgs {
    connection_url: String,
    target: String,
    #[arg(long, value_enum, default_value_t = FormatArg::Parquet)]
    format: FormatArg,
    #[arg(long, short = 'C', value_enum, default_value_t = CompressionArg::Snappy)]
    compression: CompressionArg,
    /// Target file size (e.g. 256m, 1g). Files are split when approaching this size.
    #[arg(long, value_parser = parse_size)]
    target_file_size: Option<u64>,
    /// Maximum rows per output file. Conflicts with --target-file-size if both are set explicitly.
    #[arg(long)]
    max_rows_per_file: Option<usize>,
    #[arg(long, default_value_t = 512)]
    memory_limit_mb: usize,
    #[arg(long, default_value_t = 4)]
    table_concurrency: usize,
    #[arg(long, default_value_t = 4)]
    partition_concurrency_per_table: usize,
    #[arg(long, default_value_t = 16)]
    max_concurrency: usize,
    #[arg(long)]
    checkpoint_dir: Option<String>,
    #[arg(long, default_value_t = false)]
    resume: bool,
}

#[derive(Debug, Args)]
struct TableArgs {
    #[command(flatten)]
    common: CommonArgs,
    table: String,
    #[arg(long)]
    r#where: Option<String>,
}
#[derive(Debug, Args)]
struct QueryArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, conflicts_with = "query_file")]
    query: Option<String>,
    #[arg(long)]
    query_file: Option<std::path::PathBuf>,
    #[arg(long)]
    query_name: String,
    #[arg(long)]
    partition_predicate_template: Option<String>,
}
#[derive(Debug, Args)]
struct SchemaArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long)]
    schema: String,
    #[arg(long = "include-table")]
    include: Vec<String>,
    #[arg(long = "exclude-table")]
    exclude: Vec<String>,
}
#[derive(Debug, Args)]
struct DatabaseArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long = "include-table")]
    include: Vec<String>,
    #[arg(long = "exclude-table")]
    exclude: Vec<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum FormatArg {
    Parquet,
    Csv,
    Tsv,
    Json,
}
#[derive(Debug, Clone, Copy, ValueEnum)]
enum CompressionArg {
    None,
    Snappy,
    Zstd,
    Gzip,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    let Some(command) = cli.command else {
        Cli::command().print_help()?;
        println!();
        return Ok(());
    };
    let config = command.into_config().await?;
    let engine = ExecutionEngine::new();
    let catalog = StaticCatalog::default();
    let tasks = engine.plan(&config, &catalog).await?;

    if cli.dry_run {
        println!("{}", serde_json::to_string_pretty(&tasks)?);
        return Ok(());
    }

    println!("Exporting {} task(s)...", tasks.len());

    let executor = TaskExecutor::new(config);
    let result = executor.execute_tasks(tasks).await?;

    println!(
        "\n✅ Export complete: {} task(s), {} rows, {} file(s), {} bytes written",
        result.completed_tasks, result.rows_exported, result.files_written, result.bytes_written,
    );
    if result.failed_tasks > 0 {
        eprintln!("⚠️  {} task(s) failed:", result.failed_tasks);
        for err in &result.errors {
            eprintln!("  - {err}");
        }
        std::process::exit(1);
    }
    Ok(())
}

impl Command {
    async fn into_config(self) -> Result<ExportConfig> {
        match self {
            Command::Table(args) => {
                let table: TableRef = args.table.parse()?;
                Ok(config(
                    args.common,
                    ExportScope::Table {
                        table,
                        predicate: args.r#where,
                    },
                ))
            }
            Command::Query(args) => {
                let sql = match (args.query, args.query_file) {
                    (Some(query), None) => query,
                    (None, Some(path)) => tokio::fs::read_to_string(path).await?,
                    _ => anyhow::bail!("query scope requires --query or --query-file"),
                };
                Ok(config(
                    args.common,
                    ExportScope::Query {
                        sql,
                        name: args.query_name,
                        partition_template: args.partition_predicate_template,
                    },
                ))
            }
            Command::Schema(args) => Ok(config(
                args.common,
                ExportScope::Schema {
                    schema: args.schema,
                    include: args.include,
                    exclude: args.exclude,
                },
            )),
            Command::Database(args) => Ok(config(
                args.common,
                ExportScope::Database {
                    include: args.include,
                    exclude: args.exclude,
                },
            )),
        }
    }
}

fn config(common: CommonArgs, scope: ExportScope) -> ExportConfig {
    const DEFAULT_FILE_SIZE: u64 = 256 * 1024 * 1024; // 256MB
    const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
    const WARN_FILE_SIZE: u64 = 512 * 1024 * 1024; // 512MB

    let user_set_file_size = common.target_file_size.is_some();
    let user_set_rows = common.max_rows_per_file.is_some();

    // Resolve target_file_size with validation
    let mut target_file_size = common.target_file_size.unwrap_or(DEFAULT_FILE_SIZE);
    if target_file_size > MAX_FILE_SIZE {
        tracing::warn!(
            requested = target_file_size,
            fallback = DEFAULT_FILE_SIZE,
            "target_file_size exceeds 1GB maximum, falling back to 256MB"
        );
        target_file_size = DEFAULT_FILE_SIZE;
    } else if target_file_size > WARN_FILE_SIZE {
        tracing::warn!(
            size_mb = target_file_size / (1024 * 1024),
            "target_file_size > 512MB — large files may impact downstream query performance"
        );
    }

    // Resolve max_rows_per_file: if both are explicitly set, warn and use file size only
    let max_rows_per_file = if user_set_file_size && user_set_rows {
        tracing::warn!(
            "Both --target-file-size and --max-rows-per-file specified; \
             using --target-file-size only, ignoring --max-rows-per-file"
        );
        None
    } else {
        common.max_rows_per_file
    };

    ExportConfig {
        connection_url: common.connection_url,
        scope,
        target: common.target,
        format: match common.format {
            FormatArg::Parquet => OutputFormat::Parquet,
            FormatArg::Csv => OutputFormat::Csv,
            FormatArg::Tsv => OutputFormat::Tsv,
            FormatArg::Json => OutputFormat::Json,
        },
        compression: match common.compression {
            CompressionArg::None => CompressionCodec::None,
            CompressionArg::Snappy => CompressionCodec::Snappy,
            CompressionArg::Zstd => CompressionCodec::Zstd,
            CompressionArg::Gzip => CompressionCodec::Gzip,
        },
        consistency: ConsistencyMode::SnapshotTransaction,
        target_file_size,
        max_rows_per_file,
        batch_rows: 65_536,
        memory_limit_mb: common.memory_limit_mb,
        table_concurrency: common.table_concurrency,
        partition_concurrency_per_table: common.partition_concurrency_per_table,
        global_io_concurrency: common.max_concurrency,
        checkpoint_dir: common.checkpoint_dir,
        resume: common.resume,
    }
}

/// Parse human-readable size strings like "256m", "1g", "512k", "1048576"
fn parse_size(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return Err("empty size value".to_string());
    }
    let (num_str, multiplier) = if s.ends_with("gb") || s.ends_with('g') {
        let num = s.trim_end_matches("gb").trim_end_matches('g');
        (num, 1024u64 * 1024 * 1024)
    } else if s.ends_with("mb") || s.ends_with('m') {
        let num = s.trim_end_matches("mb").trim_end_matches('m');
        (num, 1024u64 * 1024)
    } else if s.ends_with("kb") || s.ends_with('k') {
        let num = s.trim_end_matches("kb").trim_end_matches('k');
        (num, 1024u64)
    } else {
        (s.as_str(), 1u64)
    };
    let num: f64 = num_str.parse().map_err(|_| format!("invalid size: {s}"))?;
    Ok((num * multiplier as f64) as u64)
}
