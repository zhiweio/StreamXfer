# Configuration

StreamXfer is configured via CLI arguments. Internally, all arguments are assembled into an `ExportConfig` struct that drives the execution engine.

## Export Configuration

The full configuration model includes:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | String | — | SQL Server connection URL |
| `scope` | ExportScope | — | What to export (table, query, schema, database) |
| `target` | String | — | Output path with optional template variables |
| `format` | OutputFormat | `parquet` | Output file format |
| `compression` | CompressionCodec | `snappy` | Compression algorithm |
| `consistency` | ConsistencyMode | `snapshot_transaction` | Read consistency guarantee |
| `target_file_size` | u64 | `256 MB` | Target size per output file |
| `batch_rows` | usize | `65,536` | Rows per batch in processing pipeline |
| `memory_limit_mb` | usize | `512` | Memory budget (minimum 64 MB) |
| `table_concurrency` | usize | `4` | Parallel table exports |
| `partition_concurrency_per_table` | usize | `4` | Parallel partitions per table |
| `global_io_concurrency` | usize | `16` | Global I/O parallelism cap |
| `checkpoint_dir` | Option | None | Path for checkpoint store |
| `resume` | bool | `false` | Whether to resume from checkpoint |

## Output Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| `parquet` | `.parquet` | Columnar format, best for analytics workloads |
| `csv` | `.csv` | Comma-separated values |
| `tsv` | `.tsv` | Tab-separated values |
| `json` | `.json` | Newline-delimited JSON |

!!! tip "Recommended Format"
    Parquet is the default and recommended format. It provides excellent compression ratios, preserves schema/type information, and is natively supported by most analytics tools (Spark, DuckDB, Polars, Redshift Spectrum, Athena, etc.).

## Compression Codecs

| Codec | Description | Use Case |
|-------|-------------|----------|
| `none` | No compression | Maximum write speed, largest files |
| `snappy` | Fast compression, moderate ratio | **Default.** Good balance of speed and size |
| `zstd` | High compression ratio | Minimize storage cost |
| `gzip` | Wide compatibility | When downstream requires gzip |

## Consistency Modes

StreamXfer supports multiple read consistency strategies:

| Mode | Description |
|------|-------------|
| `snapshot_transaction` | **Default.** Uses a snapshot transaction for read consistency |
| `database_snapshot` | Creates a database snapshot (requires permissions) |
| `high_watermark` | Tracks a high watermark column for incremental exports |
| `none` | No consistency guarantee (fastest) |

## Concurrency Tuning

StreamXfer uses a hierarchical concurrency model:

```
Global I/O Concurrency (--max-concurrency)
├── Table Concurrency (--table-concurrency)
│   ├── Partition Concurrency (--partition-concurrency-per-table)
│   └── Partition Concurrency
└── Table Concurrency
    ├── Partition Concurrency
    └── Partition Concurrency
```

!!! warning "Memory Budget"
    The worst-case concurrent tasks = `table_concurrency × partition_concurrency_per_table`. Ensure your `memory_limit_mb` can accommodate this level of parallelism. A warning is emitted if the worst-case exceeds `global_io_concurrency × 4`.

### Recommended Settings

| Scenario | `table_concurrency` | `partition_concurrency_per_table` | `max_concurrency` | `memory_limit_mb` |
|----------|--------------------|------------------------------------|-------------------|-------------------|
| Single large table | 1 | 8 | 16 | 1024 |
| Many small tables | 8 | 2 | 16 | 512 |
| Balanced | 4 | 4 | 16 | 512 |
| Memory-constrained | 2 | 2 | 8 | 256 |

## SQL Server Type Mapping

StreamXfer maps SQL Server types to Arrow types for accurate schema preservation:

| SQL Server Type | Arrow Type |
|-----------------|------------|
| `bit` | Boolean |
| `tinyint` | UInt8 |
| `smallint` | Int16 |
| `int` | Int32 |
| `bigint` | Int64 |
| `real` | Float32 |
| `float` | Float64 |
| `decimal`, `numeric`, `money`, `smallmoney` | Decimal128(precision, scale) |
| `date` | Date32 |
| `datetime`, `datetime2`, `smalldatetime`, `datetimeoffset`, `time` | TimestampNanos |
| `binary`, `varbinary`, `image`, `timestamp`, `rowversion` | Binary |
| `char`, `nchar`, `varchar`, `nvarchar`, `text`, `ntext`, `xml`, `uniqueidentifier` | Utf8 |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Log level filter (e.g., `info`, `debug`, `streamxfer_core=debug`) |
