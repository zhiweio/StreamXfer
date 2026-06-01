# Local Testing Environment

StreamXfer provides a **Docker Compose** stack for local development and testing with SQL Server 2022, MinIO (S3-compatible storage), and a T-SQL-based data seeder that generates ~30 million rows.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (macOS/Windows) or Docker Engine (Linux)
- Docker Compose v2+
- At least **8 GB RAM** allocated to Docker (for SQL Server + 29M row generation)

!!! note "Apple Silicon (ARM64)"
    SQL Server only provides `linux/amd64` images. On Apple Silicon Macs, Docker Desktop runs these via Rosetta 2 emulation. The `docker-compose.yml` includes `platform: linux/amd64` to handle this automatically.

## Architecture

```mermaid
graph LR
    subgraph Docker Compose
        SQL[SQL Server 2022<br/>:1433]
        MINIO[MinIO S3<br/>:9000 / :9001]
        INIT[minio-init<br/>bucket setup]
        SEED[data-seeder<br/>~30M rows]
        STX[stx CLI<br/>local build]
    end

    INIT -->|creates buckets| MINIO
    SEED -->|sqlcmd| SQL
    STX -->|TDS| SQL
    STX -->|S3 API| MINIO
```

## Services

| Service | Profile | Port | Description |
|---------|---------|------|-------------|
| `sqlserver` | *(default)* | 1433 | SQL Server 2022 Developer Edition |
| `minio` | *(default)* | 9000, 9001 | S3 API + Web Console |
| `minio-init` | *(default)* | — | Creates test buckets on startup |
| `data-seeder` | `seed` | — | T-SQL seeder (~30M rows) |
| `stx` | `app` | — | StreamXfer CLI (built from local source) |

## Quick Start

### 1. Start core services

```bash
# From the repository root
docker compose up -d

# Verify SQL Server is healthy
docker compose ps
```

Wait until `streamxfer-sqlserver` shows `healthy` status (~30 seconds).

### 2. Seed test data

```bash
# Run the seeder (first time or re-seed)
docker compose --profile seed up data-seeder
```

This runs `sqlcmd` inside a SQL Server container to execute pure T-SQL set-based generation. Estimated time: **10–30 minutes** depending on Docker resource allocation.

!!! tip "Progress monitoring"
    The seeder prints real-time progress to stdout:
    ```
    [1/8] dbo.customers (500K)...
      customers: 500000 rows, 12 sec
    [6/8] dbo.order_items (10M, 5 x 2M batches)...
      order_items: 2000000 / 10000000 rows
      order_items: 4000000 / 10000000 rows
      ...
    ```

### 3. Set up S3 credentials

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_REGION=us-east-1
```

### 4. Run StreamXfer

=== "Native (cargo build)"

    ```bash
    stx table 'mssql://sa:StreamXfer@2024!@localhost:1433/streamxfer_test' \
        ./output/ dbo.events \
        --format parquet --compression zstd
    ```

=== "Docker (local build)"

    ```bash
    docker compose --profile app run --rm stx \
        table 'mssql://sa:StreamXfer@2024!@sqlserver:1433/streamxfer_test' \
        s3://streamxfer-output/events/ dbo.events \
        --format parquet
    ```

## Test Dataset

The seeder creates the `streamxfer_test` database with 3 schemas and 8 tables:

| Table | Schema | Rows | Key SQL Types |
|-------|--------|------|---------------|
| `customers` | dbo | 500,000 | nvarchar, date, decimal(18,2), money, bit, bigint, uniqueidentifier |
| `products` | dbo | 100,000 | money, smallmoney, real, decimal(3,2), nvarchar(max) JSON |
| `orders` | dbo | 2,000,000 | datetime2(3), date, money, smallmoney, varchar |
| `order_items` | dbo | 10,000,000 | bigint identity, smallint, decimal(5,2), char(4), money |
| `events` | dbo | 10,000,000 | bigint identity, tinyint, uniqueidentifier, nvarchar(max) JSON, bit |
| `measurements` | dbo | 5,000,000 | real, float, decimal(9,6), varbinary(256), binary(16), smallint, tinyint |
| `transactions` | sales | 2,000,000 | decimal(18,4), char(3), char(6), uniqueidentifier, nvarchar(max) JSON |
| `employees` | hr | 50,000 | self-ref INT FK, date, money, decimal(5,2), tinyint, nvarchar(max) |
| **Total** | | **~30M** | |

### Data characteristics

- **NULLable columns** — all tables have columns with realistic NULL distributions (5–80%)
- **Wide rows** — `customers` and `events` have 15+ columns each
- **Binary data** — `measurements` includes `varbinary(256)` and `binary(16)` random payloads
- **JSON payloads** — `products.specifications`, `events.payload`, `transactions.metadata`
- **Self-referencing FK** — `hr.employees.manager_id` references `hr.employees.emp_id`
- **Mixed precision** — REAL, FLOAT, DECIMAL(5,2), DECIMAL(9,6), DECIMAL(18,4)

## Credentials

| Service | User | Password |
|---------|------|----------|
| SQL Server | `sa` | `StreamXfer@2024!` |
| MinIO S3 | `minioadmin` | `minioadmin123` |

**Connection strings:**

```bash
# SQL Server (mssql URL format)
mssql://sa:StreamXfer@2024!@localhost:1433/streamxfer_test

# MinIO S3 endpoint
http://localhost:9000
```

**MinIO Web Console:** [http://localhost:9001](http://localhost:9001)

## Re-seeding

To drop and re-create all test data:

```bash
# Drop the database
docker compose exec sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "StreamXfer@2024!" -No \
  -Q "DROP DATABASE IF EXISTS streamxfer_test"

# Re-run the seeder
docker compose --profile seed run --rm data-seeder
```

## Teardown

```bash
# Stop containers (keep data volumes)
docker compose stop

# Remove everything including volumes
docker compose down -v
```

## Docker Build (stx image)

The `Dockerfile` at the repository root builds the `stx` binary from local source using a multi-stage build:

```
┌─────────────────────────────────────┐
│  Stage 1: rust:1.82-slim (builder)  │
│  • COPY Cargo.toml + crates/        │
│  • cargo build --release            │
│  • strip binary                     │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│  Stage 2: debian:bookworm-slim      │
│  • COPY stx binary from builder     │
│  • Non-root user                    │
│  • ENTRYPOINT ["stx"]              │
└─────────────────────────────────────┘
```

Build manually:

```bash
docker build -t streamxfer:local .
```

Or via compose:

```bash
docker compose --profile app build stx
```

## SQL Script Files

| File | Lines | Purpose |
|------|-------|---------|
| `tests/docker/sql/00_setup.sql` | ~240 | CREATE DATABASE + schemas + 8 tables (idempotent) |
| `tests/docker/sql/01_seed.sql` | ~640 | T-SQL set-based seeder with progress reporting |
| `tests/docker/sql/run-seeder.sh` | ~20 | Shell wrapper for Docker entrypoint |

### Seeding technique

The seeder uses **pure T-SQL set-based generation** for maximum throughput:

```sql
-- Generate a tally table from system catalog cross join (~16M rows available)
;WITH Tally(rn) AS (
    SELECT TOP(2000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
    FROM sys.all_objects a, sys.all_objects b
)
INSERT INTO dbo.orders (...)
SELECT
    -- Random values via CHECKSUM(NEWID())
    CHOOSE((CHECKSUM(NEWID()) & 0x7FFFFFFF) % 5 + 1,
        'PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED'),
    ...
FROM Tally;
```

For the largest tables (10M+ rows), a `WHILE` loop inserts in 2M-row batches to manage transaction log growth.
