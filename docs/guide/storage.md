# Storage Targets

StreamXfer supports writing to local filesystems and cloud object stores.

## Local Filesystem

Write to any local directory:

```bash
stx table 'mssql://user:pass@host/db' /data/exports/ dbo.orders
stx table 'mssql://user:pass@host/db' ./relative/path/ dbo.orders
stx table 'mssql://user:pass@host/db' file:///absolute/path/ dbo.orders
```

**Behavior:**

- Directories are created automatically
- Files are written atomically (write to `.tmp`, then rename)
- Safe for concurrent readers

## Amazon S3

```bash
stx table 'mssql://user:pass@host/db' s3://my-bucket/prefix/ dbo.orders
```

Authentication uses the standard AWS credential chain (environment variables, `~/.aws/credentials`, IAM role, etc.).

## Google Cloud Storage

```bash
stx table 'mssql://user:pass@host/db' gs://my-bucket/prefix/ dbo.orders
```

Authentication uses Application Default Credentials.

## Azure Blob Storage

```bash
stx table 'mssql://user:pass@host/db' az://container/prefix/ dbo.orders
# or
stx table 'mssql://user:pass@host/db' abfs://container/prefix/ dbo.orders
```

## URI Format

| Scheme | Target |
|--------|--------|
| `/path` or `./path` or `file://path` | Local filesystem |
| `s3://bucket/prefix` | Amazon S3 |
| `gs://bucket/prefix` | Google Cloud Storage |
| `az://container/prefix` or `abfs://container/prefix` | Azure Blob Storage |

## Output Directory Structure

StreamXfer organizes output files using the target path template:

```
target/{schema}/{table}/part-00000000.parquet
target/{schema}/{table}/part-00000001.parquet
...
```

With template variables:

```bash
# Template: s3://lake/raw/{database}/{schema}/{table}/
# Result:   s3://lake/raw/mydb/dbo/orders/part-00000000.parquet
#           s3://lake/raw/mydb/sales/items/part-00000000.parquet
```

## Atomic Writes

StreamXfer guarantees atomic writes for all storage targets:

- **Local:** Write to a temporary file (`.filename.tmp`), then `rename()` to final path
- **Cloud:** Uses multipart upload with commit/abort semantics

This ensures readers never see partial files.
