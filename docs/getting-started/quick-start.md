# Quick Start

This guide demonstrates the most common StreamXfer workflows.

## Connection URL Format

StreamXfer uses a standard connection URL format:

```
mssql://username:password@host:port/database
```

The default port is `1433` if omitted.

## Export a Single Table

Export the `dbo.orders` table to Parquet files on local disk:

```bash
stx table 'mssql://sa:password@localhost:1433/mydb' ./output/ dbo.orders
```

The output is written to `./output/dbo/orders/` with auto-generated part files.

## Export with Filtering

Add a WHERE clause to filter rows:

```bash
stx table 'mssql://sa:password@localhost:1433/mydb' ./output/ dbo.orders \
    --where "created_at >= '2024-01-01'"
```

## Export a Custom Query

Run an arbitrary SQL query and export results:

```bash
stx query 'mssql://sa:password@localhost:1433/mydb' ./output/ \
    --query "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id" \
    --query-name orders_with_customers
```

You can also load SQL from a file:

```bash
stx query 'mssql://sa:password@localhost:1433/mydb' ./output/ \
    --query-file ./queries/report.sql \
    --query-name monthly_report
```

## Export an Entire Schema

Export all tables in the `sales` schema:

```bash
stx schema 'mssql://sa:password@localhost:1433/mydb' s3://my-bucket/export/ \
    --schema sales
```

Use glob patterns to include or exclude tables:

```bash
stx schema 'mssql://sa:password@localhost:1433/mydb' s3://my-bucket/export/ \
    --schema sales \
    --exclude-table "*.tmp_*" \
    --exclude-table "*.log_*"
```

## Export an Entire Database

Export all user tables across all schemas:

```bash
stx database 'mssql://sa:password@localhost:1433/mydb' s3://my-bucket/full-export/ \
    --include-table "dbo.*" \
    --include-table "sales.*"
```

## Choose Output Format

```bash
# CSV output
stx table 'mssql://sa:password@localhost:1433/mydb' ./output/ dbo.orders --format csv

# JSON output with Zstd compression
stx table 'mssql://sa:password@localhost:1433/mydb' ./output/ dbo.orders \
    --format json --compression zstd
```

## Dry Run

Use `--dry-run` to preview the execution plan without actually running the export:

```bash
stx --dry-run table 'mssql://sa:password@localhost:1433/mydb' ./output/ dbo.orders
```

This prints the planned tasks as JSON.

## Logging

Control log verbosity via the `RUST_LOG` environment variable:

```bash
# Show info-level logs
RUST_LOG=info stx table 'mssql://...' ./output/ dbo.orders

# Debug-level for troubleshooting
RUST_LOG=debug stx table 'mssql://...' ./output/ dbo.orders

# Filter to specific modules
RUST_LOG=streamxfer_core=debug stx table 'mssql://...' ./output/ dbo.orders
```
