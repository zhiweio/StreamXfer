# CLI Reference

StreamXfer provides the `stx` command-line tool with four subcommands for different export scopes.

## Global Options

| Option | Description |
|--------|-------------|
| `--dry-run` | Preview execution plan without running the export |
| `--version` | Print version information |
| `--help` | Print help information |

## Common Options

All subcommands share these options:

| Option | Default | Description |
|--------|---------|-------------|
| `--format` | `parquet` | Output format: `parquet`, `csv`, `tsv`, `json` |
| `-C, --compression` | `snappy` | Compression codec: `none`, `snappy`, `zstd`, `gzip` |
| `--memory-limit-mb` | `512` | Memory budget in MB (minimum 64) |
| `--table-concurrency` | `4` | Number of tables exported in parallel |
| `--partition-concurrency-per-table` | `4` | Partitions per table exported in parallel |
| `--max-concurrency` | `16` | Global I/O concurrency limit |
| `--checkpoint-dir` | None | Directory for checkpoint state (enables resume) |
| `--resume` | `false` | Resume from last checkpoint |

## `stx table`

Export a single table.

```
stx table <CONNECTION_URL> <TARGET> <TABLE> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `CONNECTION_URL` | SQL Server connection URL |
| `TARGET` | Output path (local or cloud URI) |
| `TABLE` | Table reference (e.g., `dbo.orders`, `[sales].[items]`) |

**Table-specific options:**

| Option | Description |
|--------|-------------|
| `--where` | WHERE clause to filter rows |

**Examples:**

```bash
stx table 'mssql://user:pass@host/db' ./output/ dbo.orders
stx table 'mssql://user:pass@host/db' s3://bucket/path/ dbo.orders --where "id > 1000"
stx table 'mssql://user:pass@host/db' ./output/ warehouse.dbo.orders --format csv
```

## `stx query`

Export results of a custom SQL query.

```
stx query <CONNECTION_URL> <TARGET> --query-name <NAME> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `CONNECTION_URL` | SQL Server connection URL |
| `TARGET` | Output path (local or cloud URI) |

**Query-specific options:**

| Option | Description |
|--------|-------------|
| `--query` | SQL query string (conflicts with `--query-file`) |
| `--query-file` | Path to SQL file (conflicts with `--query`) |
| `--query-name` | **Required.** Logical name for the query (used in output path) |
| `--partition-predicate-template` | Template for partition predicates |

**Examples:**

```bash
stx query 'mssql://user:pass@host/db' ./output/ \
    --query "SELECT * FROM orders WHERE year = 2024" \
    --query-name orders_2024

stx query 'mssql://user:pass@host/db' s3://bucket/reports/ \
    --query-file ./queries/monthly.sql \
    --query-name monthly_report
```

## `stx schema`

Export all tables in a schema.

```
stx schema <CONNECTION_URL> <TARGET> --schema <SCHEMA> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `CONNECTION_URL` | SQL Server connection URL |
| `TARGET` | Output path (local or cloud URI) |

**Schema-specific options:**

| Option | Description |
|--------|-------------|
| `--schema` | **Required.** Schema name to export |
| `--include-table` | Glob pattern to include tables (repeatable) |
| `--exclude-table` | Glob pattern to exclude tables (repeatable) |

**Examples:**

```bash
stx schema 'mssql://user:pass@host/db' s3://bucket/raw/ --schema dbo
stx schema 'mssql://user:pass@host/db' ./output/ --schema sales \
    --exclude-table "*.tmp_*" --exclude-table "*.bak_*"
```

## `stx database`

Export all user tables in the database.

```
stx database <CONNECTION_URL> <TARGET> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `CONNECTION_URL` | SQL Server connection URL |
| `TARGET` | Output path (local or cloud URI) |

**Database-specific options:**

| Option | Description |
|--------|-------------|
| `--include-table` | Glob pattern to include tables (repeatable) |
| `--exclude-table` | Glob pattern to exclude tables (repeatable) |

**Examples:**

```bash
stx database 'mssql://user:pass@host/db' s3://bucket/full/
stx database 'mssql://user:pass@host/db' ./output/ \
    --include-table "dbo.*" --include-table "sales.*"
```

## Table Reference Format

Tables can be specified in multiple formats:

| Format | Parsed As |
|--------|-----------|
| `orders` | `dbo.orders` (defaults to dbo schema) |
| `dbo.orders` | `dbo.orders` |
| `warehouse.dbo.orders` | `warehouse.dbo.orders` (3-part name) |
| `[dbo].[orders]` | `dbo.orders` (brackets stripped) |

## Target Path Templates

The target path supports template variables that are replaced at runtime:

| Variable | Description |
|----------|-------------|
| `{database}` | Database name (or "default" if not specified) |
| `{schema}` | Schema name |
| `{table}` | Table name |
| `{query}` | Query name |
| `{partition}` | Partition identifier |
| `{file_index}` | Zero-padded file index (8 digits) |

**Example:**

```bash
stx schema 'mssql://user:pass@host/db' \
    's3://lake/raw/{database}/{schema}/{table}/' \
    --schema sales
# Writes to: s3://lake/raw/default/sales/orders/, s3://lake/raw/default/sales/items/, ...
```

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Runtime error (connection failure, I/O error, etc.) |
| `2` | Invalid arguments or configuration |
