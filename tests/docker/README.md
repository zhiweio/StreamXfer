# StreamXfer – Local Test Environment

This directory contains everything needed to spin up a complete local testing
environment with **SQL Server 2022** and **MinIO** (S3-compatible storage),
plus a **T-SQL-based data seeder** that generates ~30 million rows.

## Services

| Service | Port | Purpose |
|---------|------|---------|
| SQL Server 2022 Developer | 1433 | Source database |
| MinIO S3 API | 9000 | Output object storage |
| MinIO Console | 9001 | Web UI – http://localhost:9001 |
| `data-seeder` | — | One-shot seeder (profile `seed`) |
| `stx` | — | StreamXfer CLI (profile `app`) |

## Quick Start

### 1 – Start the core stack

```bash
# From the repository root
docker compose up -d

# Wait for SQL Server to be healthy (~30 s)
docker compose ps
```

### 2 – Seed test data (~30M rows)

The seeder uses **pure T-SQL set-based generation** (`sys.all_objects CROSS JOIN`
with `CHECKSUM(NEWID())`) — no Rust binary needed. Estimated time: **10–30 min**
depending on Docker resource limits.

```bash
# Run the seeder (opt-in via the 'seed' profile)
docker compose --profile seed up data-seeder

# To re-seed (drop & recreate):
docker compose --profile seed run --rm data-seeder
```

Progress is printed to stdout in real time. The seeder creates the
`streamxfer_test` database with these tables:

| Table | Schema | Rows | Key types |
|-------|--------|------|-----------|
| `customers` | dbo | 500,000 | nvarchar, date, decimal, money, bit, nvarchar(max) |
| `products` | dbo | 100,000 | money, smallmoney, float, decimal, bit, nvarchar(max) |
| `orders` | dbo | 2,000,000 | datetime2, date, money, smallmoney |
| `order_items` | dbo | 10,000,000 | smallint, decimal(5,2), char(4) |
| `events` | dbo | 10,000,000 | tinyint, bigint, uniqueidentifier, varbinary, bit |
| `measurements` | dbo | 5,000,000 | real, float, decimal, varbinary(256), binary(16) |
| `transactions` | sales | 2,000,000 | decimal(18,4), char(3), char(6), datetime2 |
| `employees` | hr | 50,000 | self-ref manager_id, date, money, nvarchar(max) |
| **Total** | | **~30M** | |

#### Manual seeding via sqlcmd (alternative)

```bash
# Schema setup
docker compose exec sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "StreamXfer@2024!" -No -t 0 -b \
  -i /dev/stdin < tests/docker/sql/00_setup.sql

# Data generation
docker compose exec sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "StreamXfer@2024!" -No -t 0 -b \
  -d streamxfer_test \
  -i /dev/stdin < tests/docker/sql/01_seed.sql
```

### 3 – Run StreamXfer against the local environment

**MinIO S3 environment variables:**

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_REGION=us-east-1
```

**Export a table to local filesystem:**

```bash
stx table \
  --url "mssql://sa:StreamXfer@2024!@localhost:1433/streamxfer_test" \
  --table dbo.events \
  --output /tmp/out/ \
  --format parquet
```

**Export to MinIO (S3-compatible):**

```bash
stx table \
  --url "mssql://sa:StreamXfer@2024!@localhost:1433/streamxfer_test" \
  --table dbo.measurements \
  --output "s3://streamxfer-output/measurements/" \
  --format parquet \
  --compression zstd
```

**Run stx inside Docker (built from local source):**

```bash
# Build and run stx via the 'app' compose profile
docker compose --profile app run --rm stx \
  table \
  --url "mssql://sa:StreamXfer@2024!@sqlserver:1433/streamxfer_test" \
  --table dbo.order_items \
  --output "s3://streamxfer-output/order_items/" \
  --format parquet
```

### 4 – Export the entire database

```bash
stx database \
  --url "mssql://sa:StreamXfer@2024!@localhost:1433/streamxfer_test" \
  --output /tmp/streamxfer-out/ \
  --format parquet \
  --compression snappy \
  --table-concurrency 4
```

## Credentials

| Service | User | Password |
|---------|------|----------|
| SQL Server | `sa` | `StreamXfer@2024!` |
| MinIO | `minioadmin` | `minioadmin123` |

## Re-seeding

To reset and re-seed:

```bash
# Drop the test database
docker compose exec sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "StreamXfer@2024!" -No \
  -Q "DROP DATABASE IF EXISTS streamxfer_test"

# Re-run the seeder
docker compose --profile seed run --rm data-seeder
```

## Teardown

```bash
# Stop containers, keep volumes
docker compose stop

# Stop and remove everything (including data volumes)
docker compose down -v
```

## SQL Script Files

| File | Purpose |
|------|---------|
| `sql/00_setup.sql` | CREATE DATABASE, schemas, 8 tables (idempotent) |
| `sql/01_seed.sql` | T-SQL set-based generation of ~30M rows |
| `sql/run-seeder.sh` | Shell wrapper executed inside the seeder container |

## Legacy Rust Seeder (data-seeder/)

The `data-seeder/` directory contains an earlier Rust implementation using
`tiberius` + `fake-rs` that generated ~14,300 rows via row-by-row inserts.
It is kept for reference. For production-scale seeding use the T-SQL scripts above.

