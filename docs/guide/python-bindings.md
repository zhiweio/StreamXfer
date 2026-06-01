# Python Bindings

StreamXfer provides Python bindings via [PyO3](https://pyo3.rs/), exposing the core planning and export functionality to Python programs.

## Installation

Build and install using [maturin](https://github.com/PyO3/maturin):

```bash
pip install maturin
cd crates/streamxfer-py
maturin develop --release
```

This installs the `streamxfer_rust` module into your active Python environment.

## API Reference

### `streamxfer_rust.plan(config_json: str) -> str`

Generate an execution plan from an export configuration.

**Parameters:**

- `config_json` — JSON string representing the export configuration

**Returns:** JSON string containing the list of planned export tasks.

**Example:**

```python
import json
import streamxfer_rust

config = {
    "connection_url": "mssql://user:pass@host:1433/mydb",
    "scope": {
        "type": "table",
        "table": {"schema": "dbo", "table": "orders"},
        "predicate": None
    },
    "target": "s3://bucket/raw/{schema}/{table}/",
    "format": "parquet",
    "compression": "snappy",
    "consistency": "snapshot_transaction",
    "target_file_size": 268435456,
    "batch_rows": 65536,
    "memory_limit_mb": 512,
    "table_concurrency": 4,
    "partition_concurrency_per_table": 4,
    "global_io_concurrency": 16,
    "checkpoint_dir": None,
    "resume": False
}

tasks = json.loads(streamxfer_rust.plan(json.dumps(config)))
print(f"Planned {len(tasks)} export tasks")
for task in tasks:
    print(f"  - {task['id']}: {task['target_prefix']}")
```

### `streamxfer_rust.export(config_json: str) -> str`

Execute a dry-run export and return the execution summary.

**Parameters:**

- `config_json` — JSON string representing the export configuration

**Returns:** JSON string containing the execution summary.

**Example:**

```python
import json
import streamxfer_rust

config = {
    "connection_url": "mssql://user:pass@host:1433/mydb",
    "scope": {
        "type": "schema",
        "schema": "dbo",
        "include": [],
        "exclude": ["*.tmp_*"]
    },
    "target": "./output/{schema}/{table}/",
    "format": "parquet",
    "compression": "zstd",
    "consistency": "snapshot_transaction",
    "target_file_size": 268435456,
    "batch_rows": 65536,
    "memory_limit_mb": 1024,
    "table_concurrency": 4,
    "partition_concurrency_per_table": 4,
    "global_io_concurrency": 16,
    "checkpoint_dir": "/tmp/checkpoints",
    "resume": False
}

summary = json.loads(streamxfer_rust.export(json.dumps(config)))
print(f"Planned: {summary['planned_tasks']}")
print(f"Completed: {summary['completed_tasks']}")
print(f"Bytes written: {summary['bytes_written']}")
```

## Configuration JSON Schema

The configuration JSON follows the same structure as the internal `ExportConfig`:

```json
{
  "connection_url": "mssql://user:pass@host:port/database",
  "scope": { ... },
  "target": "s3://bucket/prefix/{schema}/{table}/",
  "format": "parquet | csv | tsv | json",
  "compression": "none | snappy | zstd | gzip",
  "consistency": "none | snapshot_transaction | database_snapshot | high_watermark",
  "target_file_size": 268435456,
  "batch_rows": 65536,
  "memory_limit_mb": 512,
  "table_concurrency": 4,
  "partition_concurrency_per_table": 4,
  "global_io_concurrency": 16,
  "checkpoint_dir": "/path/to/checkpoints",
  "resume": false
}
```

### Scope Variants

=== "Table"
    ```json
    {
      "type": "table",
      "table": {"schema": "dbo", "table": "orders", "database": null},
      "predicate": "id > 1000"
    }
    ```

=== "Query"
    ```json
    {
      "type": "query",
      "sql": "SELECT * FROM orders WHERE year = 2024",
      "name": "orders_2024",
      "partition_template": null
    }
    ```

=== "Schema"
    ```json
    {
      "type": "schema",
      "schema": "dbo",
      "include": ["dbo.order*"],
      "exclude": ["*.tmp_*"]
    }
    ```

=== "Database"
    ```json
    {
      "type": "database",
      "include": [],
      "exclude": ["*.sys_*"]
    }
    ```
