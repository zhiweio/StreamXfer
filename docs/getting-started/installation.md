# Installation

## Prerequisites

- **Rust toolchain** (1.70+): [Install Rust](https://rustup.rs/)
- **SQL Server** instance accessible over the network

No external tools like BCP or ODBC drivers are required — StreamXfer connects directly via the TDS protocol.

## Install from Source

```bash
git clone https://github.com/zhiweio/StreamXfer.git
cd StreamXfer
cargo build --release
```

The binary is located at `target/release/stx`.

### Install to PATH

```bash
cargo install --path crates/streamxfer-cli
```

This installs the `stx` binary to `~/.cargo/bin/`.

## Build with RocksDB Checkpoint Support

To enable persistent checkpointing with RocksDB:

```bash
cargo build --release --features streamxfer-core/rocksdb-checkpoint
```

## Python Bindings

The Python bindings require [maturin](https://github.com/PyO3/maturin):

```bash
pip install maturin
cd crates/streamxfer-py
maturin develop --release
```

This builds and installs the `streamxfer_rust` Python module in your current environment.

## Verify Installation

```bash
stx --version
stx --help
```

## Docker

StreamXfer provides a multi-stage Dockerfile for containerized deployment:

```bash
docker build -t streamxfer:local .

# Run directly
docker run --rm streamxfer:local --help
docker run --rm streamxfer:local table 'mssql://user:pass@host:1433/db' /data/ dbo.orders
```

Or use `docker compose` with the `app` profile to build and run against the local test environment:

```bash
docker compose --profile app run --rm stx \
    table 'mssql://sa:StreamXfer@2024!@sqlserver:1433/streamxfer_test' \
    s3://streamxfer-output/orders/ dbo.orders --format parquet
```

See [Local Testing](../development/local-testing.md) for the full Docker Compose setup.
