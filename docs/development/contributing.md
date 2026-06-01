# Contributing

## Development Setup

```bash
git clone https://github.com/zhiweio/StreamXfer.git
cd StreamXfer
cargo build
```

### Prerequisites

- Rust 1.70+ (install via [rustup](https://rustup.rs/))
- For RocksDB feature: C++ compiler and `libclang`
- For integration testing: Docker Desktop (see [Local Testing](local-testing.md))

## Project Structure

```
crates/
├── streamxfer-core/     # Core library
│   ├── src/
│   │   ├── config.rs           # Configuration & validation
│   │   ├── error.rs            # Error types
│   │   ├── planner/            # Task planning
│   │   │   ├── catalog.rs      # Table discovery
│   │   │   ├── partition.rs    # Partition strategies
│   │   │   └── scope.rs        # Scope expansion
│   │   ├── runtime/            # Execution engine
│   │   │   ├── engine.rs       # Orchestration
│   │   │   └── limits.rs       # Concurrency limits
│   │   ├── source/mssql.rs     # SQL Server source
│   │   ├── schema/mapping.rs   # Type mapping
│   │   ├── sink/storage.rs     # Storage targets
│   │   └── checkpoint/         # Resumable state
│   └── tests/                  # Integration tests
├── streamxfer-cli/             # CLI binary (stx)
│   └── src/main.rs
└── streamxfer-py/              # Python bindings
    └── src/lib.rs
```

## Common Commands

```bash
# Check compilation
cargo check --workspace

# Run all tests
cargo test --workspace

# Format code
cargo fmt --all

# Check formatting
cargo fmt --all --check

# Run lints
cargo clippy --workspace

# Build release binary
cargo build --release

# Build with RocksDB checkpoint support
cargo build --release --features streamxfer-core/rocksdb-checkpoint
```

## Testing

Tests are organized by module:

- `crates/streamxfer-core/src/*/` — Unit tests in each module (via `#[cfg(test)]`)
- `crates/streamxfer-core/tests/` — Integration tests:
    - `export_planning.rs` — End-to-end planning tests
    - `export_scopes.rs` — Scope expansion tests
    - `source_schema_runtime.rs` — Source and schema tests
    - `storage_checkpoint.rs` — Storage and checkpoint tests

Run a specific test:

```bash
cargo test -p streamxfer-core export_planning
```

### Local Test Environment

For full end-to-end testing against a real SQL Server with production-scale data:

```bash
# Start SQL Server + MinIO
docker compose up -d

# Seed ~30M rows of test data
docker compose --profile seed up data-seeder

# Run tests against local SQL Server
export MSSQL_URL="mssql://sa:StreamXfer@2024!@localhost:1433/streamxfer_test"
cargo test --workspace
```

See [Local Testing](local-testing.md) for complete setup instructions.

## Adding a New Output Format

1. Add variant to `OutputFormat` in `config.rs`
2. Implement encoder in a new module under `crates/streamxfer-core/src/`
3. Wire it into the execution pipeline in `runtime/engine.rs`
4. Add CLI argument variant in `crates/streamxfer-cli/src/main.rs`
5. Add tests

## Adding a New Storage Backend

1. Implement the `StorageSink` trait from `sink/storage.rs`:
   ```rust
   #[async_trait]
   pub trait StorageSink: Send + Sync {
       async fn put_atomic(&self, path: &str, bytes: Bytes) -> Result<PutResult>;
       async fn head(&self, path: &str) -> Result<Option<PutResult>>;
   }
   ```
2. Add URI parsing support in `StorageUri::parse()`
3. Register in `default_sink_from_uri()`

## Code Style

- Follow standard Rust conventions (`cargo fmt`)
- Use `thiserror` for error types
- Use `tracing` for structured logging (not `println!`)
- Prefer `async` interfaces for I/O operations
- Write tests for all public APIs

## Commit Messages

Use conventional commit format:

```
feat: add Azure Blob Storage sink
fix: handle nullable decimal columns correctly
docs: update CLI reference for new --where option
test: add schema export integration test
```
