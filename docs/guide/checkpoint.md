# Checkpoint & Resume

StreamXfer supports checkpointing to enable resumable exports. If an export is interrupted (network failure, process crash, etc.), it can be resumed from the last committed checkpoint.

## Enabling Checkpoints

Provide a checkpoint directory:

```bash
stx table 'mssql://user:pass@host/db' s3://bucket/data/ dbo.orders \
    --checkpoint-dir /tmp/stx-checkpoints
```

## Resuming an Export

To resume a previously interrupted export:

```bash
stx table 'mssql://user:pass@host/db' s3://bucket/data/ dbo.orders \
    --checkpoint-dir /tmp/stx-checkpoints \
    --resume
```

Already-committed partitions are skipped automatically.

## How It Works

```mermaid
stateDiagram-v2
    [*] --> Planned
    Planned --> Running
    Running --> Uploaded
    Uploaded --> Committed
    Running --> Failed
    Failed --> Running: retry
    Committed --> [*]
```

Each export task partition goes through these states:

| State | Description |
|-------|-------------|
| `Planned` | Task is queued for execution |
| `Running` | Data is being read and processed |
| `Uploaded` | Output file has been written to storage |
| `Committed` | Checkpoint recorded; task is complete |
| `Failed` | An error occurred (eligible for retry) |

## Checkpoint Key

Each checkpoint record is uniquely identified by:

- Job ID
- Database name
- Schema name
- Table name (or query name)
- Partition ID
- File index

This allows fine-grained resume at the individual file level.

## Checkpoint Stores

### In-Memory (Default)

Used when no `--checkpoint-dir` is provided. State is lost when the process exits.

### RocksDB (Persistent)

When `--checkpoint-dir` is specified and the `rocksdb-checkpoint` feature is enabled, StreamXfer uses RocksDB for durable checkpoint storage.

Build with RocksDB support:

```bash
cargo build --release --features streamxfer-core/rocksdb-checkpoint
```

## Best Practices

!!! tip "Production Exports"
    Always use `--checkpoint-dir` for large exports. This protects against network interruptions and allows you to safely resume without re-exporting data.

!!! warning "Checkpoint Directory"
    Use a persistent local directory for the checkpoint store. Do not use `/tmp` for production workloads as it may be cleared on reboot.
