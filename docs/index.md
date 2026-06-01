# StreamXfer

**High-performance SQL Server data export tool built in Rust.**

StreamXfer streams data directly from SQL Server via the native TDS protocol and writes to local or cloud storage (S3, GCS, Azure Blob) in Parquet, CSV, TSV, or JSON format.

---

## Key Features

- :zap: **Native TDS Protocol** — Connects directly to SQL Server via [tiberius](https://github.com/steffenede/tiberius), no BCP or ODBC required
- :package: **Multiple Output Formats** — Parquet (default), CSV, TSV, JSON
- :file_folder: **Flexible Storage Targets** — Local filesystem, Amazon S3, Google Cloud Storage, Azure Blob Storage
- :repeat: **Checkpoint & Resume** — Resumable exports with RocksDB-backed checkpoint store
- :rocket: **Concurrent Execution** — Table-level, partition-level, and global I/O concurrency controls
- :compression: **Compression** — Snappy (default), Zstd, Gzip with format-aware validation
- :scissors: **Smart File Splitting** — Split output by target file size (default 256 MB) or row count
- :shield: **Consistency Modes** — Snapshot transactions, database snapshots, high watermark
- :bulb: **Smart Planning** — Export single tables, custom queries, entire schemas, or full databases
- :mag: **Glob Filters** — Include/exclude tables with glob patterns
- :card_index_dividers: **Path Templates** — Dynamic output paths with `{database}`, `{schema}`, `{table}` variables
- :snake: **Python Bindings** — Use StreamXfer from Python via PyO3

## Architecture

```mermaid
graph LR
    A[SQL Server] -->|TDS Protocol| B[StreamXfer Core]
    B -->|Arrow RecordBatch| C{Output Format}
    C -->|Parquet| D[Storage Sink]
    C -->|CSV/TSV/JSON| D
    D --> E[Local FS]
    D --> F[S3]
    D --> G[GCS]
    D --> H[Azure Blob]
```

## Quick Example

```bash
# Export a single table to local Parquet files
stx table 'mssql://user:pass@host:1433/mydb' ./output/ dbo.orders

# Export an entire schema to S3
stx schema 'mssql://user:pass@host:1433/mydb' s3://bucket/export/ --schema sales

# Export with custom query
stx query 'mssql://user:pass@host:1433/mydb' ./output/ \
    --query "SELECT * FROM orders WHERE year = 2024" \
    --query-name orders_2024
```

## Project Structure

```
StreamXfer/
├── crates/
│   ├── streamxfer-core/    # Core library: planner, runtime, source, sink
│   ├── streamxfer-cli/     # CLI binary (stx)
│   └── streamxfer-py/      # Python bindings via PyO3
├── tests/                  # Integration tests
├── docs/                   # Documentation (this site)
└── Cargo.toml              # Workspace configuration
```
