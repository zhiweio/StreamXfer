# StreamXfer

[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](https://choosealicense.com/licenses/gpl-3.0/)

**High-performance SQL Server data export tool built in Rust.**

StreamXfer streams data directly from SQL Server via the native TDS protocol and writes to local or cloud storage in Parquet, CSV, TSV, or JSON format — with concurrent execution, checkpointing, and Python bindings.

## Features

- ⚡ **Native TDS Protocol** — Connects directly via [tiberius](https://github.com/steffenede/tiberius), no BCP or ODBC required
- 📦 **Multiple Output Formats** — Parquet (default), CSV, TSV, JSON
- 📂 **Flexible Storage Targets** — Local filesystem, Amazon S3, Google Cloud Storage, Azure Blob Storage
- 🔄 **Checkpoint & Resume** — Resumable exports with RocksDB-backed checkpoint store
- 🚀 **Concurrent Execution** — Table-level, partition-level, and global I/O concurrency controls
- 🗜️ **Compression** — Snappy (default), Zstd, Gzip with format-aware validation
- ✂️ **Smart File Splitting** — Split output by target file size (default 256 MB) or row count
- 🛡️ **Consistency Modes** — Snapshot transactions, database snapshots, high watermark
- 💡 **Smart Planning** — Export single tables, custom queries, entire schemas, or full databases
- 🔍 **Glob Filters** — Include/exclude tables with glob patterns
- 🗂️ **Path Templates** — Dynamic output paths with `{database}`, `{schema}`, `{table}` variables
- 🐍 **Python Bindings** — Use StreamXfer from Python via PyO3

## Installation

**Prerequisites:** Rust 1.70+ ([install](https://rustup.rs/))

```bash
# Build from source
git clone https://github.com/zhiweio/StreamXfer.git && cd StreamXfer
cargo build --release

# Install to ~/.cargo/bin
cargo install --path crates/streamxfer-cli
```

## Quick Start

```bash
# Export a single table to Parquet
stx table 'mssql://user:pass@host:1433/mydb' ./output/ dbo.orders

# Export with WHERE filter
stx table 'mssql://user:pass@host:1433/mydb' ./output/ dbo.orders \
    --where "created_at >= '2024-01-01'"

# Export entire schema to S3
stx schema 'mssql://user:pass@host:1433/mydb' s3://bucket/raw/ --schema sales

# Export custom query
stx query 'mssql://user:pass@host:1433/mydb' ./output/ \
    --query "SELECT * FROM orders WHERE year = 2024" \
    --query-name orders_2024

# Export full database with filters
stx database 'mssql://user:pass@host:1433/mydb' s3://bucket/full/ \
    --exclude-table "*.tmp_*" --exclude-table "*.bak_*"

# Dry run (preview plan)
stx --dry-run schema 'mssql://user:pass@host:1433/mydb' ./out/ --schema dbo
```

## CLI Subcommands

| Command | Description |
|---------|-------------|
| `stx table` | Export a single table |
| `stx query` | Export results of a SQL query |
| `stx schema` | Export all tables in a schema |
| `stx database` | Export all tables in the database |

### Common Options

| Option | Default | Description |
|--------|---------|-------------|
| `--format` | `parquet` | Output format: `parquet`, `csv`, `tsv`, `json` |
| `-C, --compression` | `snappy` | Codec: `none`, `snappy`, `zstd`, `gzip` |
| `--target-file-size` | `256m` | Target size per output file (e.g. `128m`, `1g`) |
| `--max-rows-per-file` | — | Max rows per file (optional, conflicts with `--target-file-size`) |
| `--memory-limit-mb` | `512` | Memory budget (min 64 MB) |
| `--table-concurrency` | `4` | Parallel table exports |
| `--partition-concurrency-per-table` | `4` | Parallel partitions per table |
| `--max-concurrency` | `16` | Global I/O concurrency cap |
| `--checkpoint-dir` | — | Path for checkpoint state |
| `--resume` | `false` | Resume from last checkpoint |
| `--dry-run` | — | Preview plan without executing |

## Python Usage

```bash
pip install maturin
cd crates/streamxfer-py && maturin develop --release
```

```python
import json
import streamxfer_rust

config = {
    "connection_url": "mssql://user:pass@host:1433/mydb",
    "scope": {"type": "table", "table": {"schema": "dbo", "table": "orders"}, "predicate": None},
    "target": "./output/{schema}/{table}/",
    "format": "parquet",
    "compression": "snappy",
    "consistency": "snapshot_transaction",
    "target_file_size": 268435456,
    "batch_rows": 65536,
    "memory_limit_mb": 512,
    "table_concurrency": 4,
    "partition_concurrency_per_table": 4,
    "global_io_concurrency": 16,
}

tasks = json.loads(streamxfer_rust.plan(json.dumps(config)))
print(f"Planned {len(tasks)} tasks")
```

## Architecture

```
SQL Server ──TDS──▶ StreamXfer Core ──Arrow──▶ Format Encoder ──▶ Storage Sink
                         │                                            │
                    Planner + Runtime                          Local / S3 / GCS / Azure
                    Checkpoint Store
```

**Workspace structure:**

| Crate | Description |
|-------|-------------|
| `streamxfer-core` | Core library: planner, runtime, source, sink, checkpoint |
| `streamxfer-cli` | CLI binary (`stx`) |
| `streamxfer-py` | Python bindings via PyO3 |

## Documentation

Full documentation: [https://zhiweio.github.io/StreamXfer/](https://zhiweio.github.io/StreamXfer/)

Build docs locally:

```bash
uvx --with mkdocs-material mkdocs serve
```

## Development

```bash
cargo check --workspace      # Check compilation
cargo test --workspace       # Run all tests
cargo fmt --all --check      # Check formatting
cargo clippy --workspace     # Lint
```

### Local Test Environment

Docker Compose provides a ready-to-use test environment with SQL Server 2022, MinIO (S3-compatible), and a data seeder that generates ~30 million rows:

```bash
# Start SQL Server + MinIO
docker compose up -d

# Seed ~30M rows of production-realistic test data
docker compose --profile seed up data-seeder

# Run stx against local environment
stx table 'mssql://sa:StreamXfer@2024!@localhost:1433/streamxfer_test' \
    ./output/ dbo.orders --format parquet

```

The test dataset covers 8 tables across 3 schemas with diverse SQL types (datetime2, money, decimal, binary, uniqueidentifier, nvarchar(max) JSON, etc.).

See [full documentation](https://zhiweio.github.io/StreamXfer/development/local-testing/) for details.

### Docker Build

```bash
# Build the stx image (multi-stage, local source)
docker build -t streamxfer:local .

# Or via compose profile
docker compose --profile app build stx
```

## License

[GPL-3.0](https://choosealicense.com/licenses/gpl-3.0/)

## Author

[@zhiweio](https://github.com/zhiweio)
