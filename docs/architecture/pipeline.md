# Data Pipeline

StreamXfer implements a streaming data pipeline from SQL Server to storage targets.

## Pipeline Stages

```mermaid
graph LR
    A[SQL Server<br/>TDS Protocol] -->|Row Stream| B[Schema Discovery]
    B -->|SqlColumn metadata| C[Arrow Conversion]
    C -->|RecordBatch| D[Format Encoder]
    D -->|Bytes| E[Compression]
    E -->|Compressed Bytes| F[Storage Sink]
    F -->|Atomic Write| G[Target Files]
```

### Stage 1: Source (TDS Connection)

StreamXfer connects to SQL Server using the [tiberius](https://github.com/steffenede/tiberius) TDS client library. The connection configuration is parsed from a standard URL:

```
mssql://username:password@host:port/database
```

The source module generates appropriate SQL based on the export scope:

- **Table:** `SELECT * FROM [schema].[table] [WHERE predicate]`
- **Query:** User-provided SQL
- **Schema/Database:** Discovered via `INFORMATION_SCHEMA.TABLES`

### Stage 2: Schema Discovery

Before data transfer, StreamXfer queries `INFORMATION_SCHEMA.COLUMNS` to retrieve column metadata:

- Column name and ordinal position
- Data type
- Nullability
- Numeric precision and scale

### Stage 3: Arrow Type Mapping

SQL Server types are mapped to Apache Arrow types for efficient in-memory representation:

```
SQL Server          →  Arrow Type
────────────────────────────────────
bit                 →  Boolean
tinyint             →  UInt8
smallint            →  Int16
int                 →  Int32
bigint              →  Int64
real                →  Float32
float               →  Float64
decimal(p,s)        →  Decimal128(p, s)
date                →  Date32
datetime/datetime2  →  TimestampNanos
varbinary           →  Binary
varchar/nvarchar    →  Utf8
```

### Stage 4: Format Encoding

Arrow record batches are encoded into the target format:

| Format | Encoding Strategy |
|--------|-------------------|
| Parquet | Columnar encoding with row groups aligned to batch boundaries |
| CSV | Row-by-row text encoding with RFC 4180 escaping |
| TSV | Tab-delimited text encoding |
| JSON | Newline-delimited JSON (one JSON object per row) |

### Stage 5: Compression

The encoded bytes are compressed using the configured codec:

| Codec | Algorithm | Typical Ratio |
|-------|-----------|---------------|
| Snappy | Block compression | 2-4x |
| Zstd | Dictionary compression | 4-8x |
| Gzip | DEFLATE | 3-6x |

For Parquet format, compression is applied at the column chunk level (built into Parquet encoding). For other formats, compression wraps the entire output stream.

### Stage 6: Storage Sink

Compressed bytes are written to the target storage:

- **Local:** Atomic write via temp file + rename
- **Cloud:** Multipart upload with commit/abort

## File Splitting

Output is split into multiple files based on `target_file_size` (default 256 MB). Each file is named with a zero-padded index:

```
target/schema/table/part-00000000.parquet
target/schema/table/part-00000001.parquet
target/schema/table/part-00000002.parquet
```

## Partitioned Execution

For large tables, StreamXfer can partition the export into parallel streams:

```mermaid
graph TB
    T[Table: dbo.orders] --> P1[Partition 0<br/>WHERE id BETWEEN 0 AND 999999]
    T --> P2[Partition 1<br/>WHERE id BETWEEN 1000000 AND 1999999]
    T --> P3[Partition 2<br/>WHERE id BETWEEN 2000000 AND 2999999]
    P1 --> S1[part-00000000.parquet]
    P2 --> S2[part-00000001.parquet]
    P3 --> S3[part-00000002.parquet]
```

Partition strategies:

- **None** — Single stream (default)
- **Range** — Split by column value range
- **PredicateList** — Explicit list of WHERE predicates

## Concurrency Model

```mermaid
graph TB
    G[Global I/O Semaphore<br/>max_concurrency=16]
    G --> T1[Table 1]
    G --> T2[Table 2]
    G --> T3[Table 3]
    G --> T4[Table 4]
    T1 --> P1A[Partition A]
    T1 --> P1B[Partition B]
    T2 --> P2A[Partition A]
    T2 --> P2B[Partition B]
```

The runtime enforces:

1. **Table concurrency** — Max tables processed simultaneously
2. **Partition concurrency per table** — Max partitions per table simultaneously
3. **Global I/O concurrency** — Hard cap on total concurrent I/O operations

This prevents resource exhaustion while maximizing throughput.
