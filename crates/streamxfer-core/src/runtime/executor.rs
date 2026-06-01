use crate::config::{CompressionCodec, ExportConfig, OutputFormat};
use crate::error::{Result, StreamXferError};
use crate::planner::scope::ExportTask;
use crate::sink::storage::StorageUri;
use crate::source::mssql::MssqlConnectionConfig;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tiberius::{Client, ColumnType, Row};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::{info, warn};

pub struct TaskExecutor {
    config: ExportConfig,
}

impl TaskExecutor {
    pub fn new(config: ExportConfig) -> Self {
        Self { config }
    }

    pub async fn execute_tasks(&self, tasks: Vec<ExportTask>) -> Result<ExecutionResult> {
        let mut result = ExecutionResult::default();
        for task in &tasks {
            info!(task_id = %task.id, sql = %task.sql, "executing task");
            match self.execute_single_task(task).await {
                Ok(task_result) => {
                    result.rows_exported += task_result.rows;
                    result.bytes_written += task_result.bytes;
                    result.files_written += task_result.files;
                    result.completed_tasks += 1;
                    info!(
                        task_id = %task.id,
                        rows = task_result.rows,
                        bytes = task_result.bytes,
                        files = task_result.files,
                        "task complete"
                    );
                }
                Err(e) => {
                    warn!(task_id = %task.id, error = %e, "task failed");
                    result.failed_tasks += 1;
                    result.errors.push(format!("{}: {}", task.id, e));
                }
            }
        }
        Ok(result)
    }

    async fn execute_single_task(&self, task: &ExportTask) -> Result<TaskResult> {
        let conn_config = MssqlConnectionConfig::from_url(&self.config.connection_url)?;
        let mut client = connect(&conn_config).await?;

        // Execute the query
        let stream = client
            .simple_query(&task.sql)
            .await
            .map_err(|e| StreamXferError::Source(format!("query execution failed: {e}")))?;

        let result_set = stream
            .into_first_result()
            .await
            .map_err(|e| StreamXferError::Source(format!("failed to read result set: {e}")))?;

        if result_set.is_empty() {
            info!(task_id = %task.id, "query returned no rows");
            return Ok(TaskResult {
                rows: 0,
                bytes: 0,
                files: 0,
            });
        }

        // Build Arrow schema from first row's column metadata
        let schema = build_schema_from_rows(&result_set)?;

        let batch_size = self.config.batch_rows;
        let max_rows_per_file = self.config.max_rows_per_file;
        let target_file_size = self.config.target_file_size;
        let mut all_rows_written = 0usize;
        let mut all_bytes_written = 0u64;
        let mut file_index = 0usize;

        let target_uri = StorageUri::parse(&task.target_prefix)?;
        let sink = create_sink(&target_uri).await?;

        // Resolve compression once per task (emits warning only once)
        let effective_compression =
            resolve_compression(&self.config.format, &self.config.compression);

        // Accumulate batches into files, splitting by target_file_size (primary)
        // or max_rows_per_file (if explicitly set)
        let mut offset = 0;
        let mut file_batches: Vec<RecordBatch> = Vec::new();
        let mut file_rows = 0usize;
        let mut file_size_estimate = 0u64;

        while offset < result_set.len() {
            let end = (offset + batch_size).min(result_set.len());
            let chunk = &result_set[offset..end];
            let batch = rows_to_record_batch(chunk, &schema)?;
            let batch_rows_count = batch.num_rows();
            offset = end;

            // Estimate batch size by encoding it
            let batch_bytes =
                write_batch_to_format(&batch, &self.config.format, &effective_compression)?;
            let batch_len = batch_bytes.len() as u64;

            // Check if adding this batch would exceed limits
            let would_exceed_size =
                file_size_estimate + batch_len > target_file_size && !file_batches.is_empty();
            let would_exceed_rows = match max_rows_per_file {
                Some(max) => file_rows + batch_rows_count > max,
                None => false,
            };

            if would_exceed_rows || would_exceed_size {
                // Flush current file
                let file_data = merge_batches_to_format(
                    &file_batches,
                    &self.config.format,
                    &effective_compression,
                )?;
                let file_name =
                    format_file_name(file_index, &self.config.format, &effective_compression);
                let path = build_output_path(&target_uri, &file_name);
                let written = file_data.len() as u64;
                sink.put(&path.into(), file_data.into())
                    .await
                    .map_err(|e| StreamXferError::Storage(format!("write failed: {e}")))?;

                all_rows_written += file_rows;
                all_bytes_written += written;
                file_index += 1;

                // Reset accumulators
                file_batches.clear();
                file_rows = 0;
                file_size_estimate = 0;
            }

            file_batches.push(batch);
            file_rows += batch_rows_count;
            file_size_estimate += batch_len;
        }

        // Flush remaining batches
        if !file_batches.is_empty() {
            let file_data = merge_batches_to_format(
                &file_batches,
                &self.config.format,
                &effective_compression,
            )?;
            let file_name =
                format_file_name(file_index, &self.config.format, &effective_compression);
            let path = build_output_path(&target_uri, &file_name);
            let written = file_data.len() as u64;
            sink.put(&path.into(), file_data.into())
                .await
                .map_err(|e| StreamXferError::Storage(format!("write failed: {e}")))?;

            all_rows_written += file_rows;
            all_bytes_written += written;
            file_index += 1;
        }

        Ok(TaskResult {
            rows: all_rows_written,
            bytes: all_bytes_written,
            files: file_index,
        })
    }
}

async fn connect(
    config: &MssqlConnectionConfig,
) -> Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    let tib_config = config.tiberius_config();
    let addr = format!("{}:{}", config.host, config.port);
    let tcp = TcpStream::connect(&addr)
        .await
        .map_err(|e| StreamXferError::Source(format!("TCP connect to {addr} failed: {e}")))?;
    tcp.set_nodelay(true).ok();
    let client = Client::connect(tib_config, tcp.compat_write())
        .await
        .map_err(|e| StreamXferError::Source(format!("TDS connect failed: {e}")))?;
    Ok(client)
}

fn build_schema_from_rows(rows: &[Row]) -> Result<Arc<Schema>> {
    let first = rows
        .first()
        .ok_or_else(|| StreamXferError::Source("no rows to infer schema".into()))?;
    let columns = first.columns();
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let name = col.name().to_string();
            let dt = tiberius_type_to_arrow(col.column_type());
            Field::new(name, dt, true)
        })
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

fn tiberius_type_to_arrow(col_type: ColumnType) -> DataType {
    match col_type {
        ColumnType::Bit => DataType::Boolean,
        ColumnType::Int1 => DataType::UInt8,
        ColumnType::Int2 => DataType::Int16,
        ColumnType::Int4 => DataType::Int32,
        ColumnType::Int8 | ColumnType::Intn => DataType::Int64,
        ColumnType::Float4 => DataType::Float32,
        ColumnType::Float8 | ColumnType::Floatn => DataType::Float64,
        ColumnType::Money | ColumnType::Money4 => DataType::Float64,
        ColumnType::Numericn | ColumnType::Decimaln => DataType::Utf8,
        ColumnType::Datetime
        | ColumnType::Datetime2
        | ColumnType::Datetime4
        | ColumnType::Datetimen
        | ColumnType::DatetimeOffsetn => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::Daten => DataType::Date32,
        ColumnType::Timen => DataType::Utf8,
        ColumnType::Bitn => DataType::Boolean,
        ColumnType::BigBinary | ColumnType::BigVarBin => DataType::Binary,
        _ => DataType::Utf8,
    }
}

fn rows_to_record_batch(rows: &[Row], schema: &Arc<Schema>) -> Result<RecordBatch> {
    let num_cols = schema.fields().len();
    let num_rows = rows.len();

    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let field = schema.field(col_idx);
        let array: Arc<dyn Array> = match field.data_type() {
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in rows {
                    match row.try_get::<bool, _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::UInt8 => {
                let mut builder = UInt8Builder::with_capacity(num_rows);
                for row in rows {
                    match row.try_get::<u8, _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int16 => {
                let mut builder = Int16Builder::with_capacity(num_rows);
                for row in rows {
                    match row.try_get::<i16, _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(num_rows);
                for row in rows {
                    match row.try_get::<i32, _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in rows {
                    match row.try_get::<i64, _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float32 => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for row in rows {
                    match row.try_get::<f32, _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in rows {
                    match row.try_get::<f64, _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Date32 => {
                let mut builder = Date32Builder::with_capacity(num_rows);
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                for row in rows {
                    match row.try_get::<NaiveDate, _>(col_idx) {
                        Ok(Some(d)) => {
                            let days = (d - epoch).num_days() as i32;
                            builder.append_value(days);
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                for row in rows {
                    match row.try_get::<NaiveDateTime, _>(col_idx) {
                        Ok(Some(dt)) => {
                            let micros = (dt - epoch).num_microseconds().unwrap_or(0);
                            builder.append_value(micros);
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Binary => {
                let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 32);
                for row in rows {
                    match row.try_get::<&[u8], _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // Default: convert everything to string
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
                for row in rows {
                    match row.try_get::<&str, _>(col_idx) {
                        Ok(Some(v)) => builder.append_value(v),
                        _ => {
                            // Try other string-convertible types
                            let val = extract_as_string(row, col_idx);
                            match val {
                                Some(s) => builder.append_value(&s),
                                None => builder.append_null(),
                            }
                        }
                    }
                }
                Arc::new(builder.finish())
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| StreamXferError::Source(format!("failed to build RecordBatch: {e}")))
}

fn extract_as_string(row: &Row, idx: usize) -> Option<String> {
    // Try numeric types to string
    if let Ok(Some(v)) = row.try_get::<i32, _>(idx) {
        return Some(v.to_string());
    }
    if let Ok(Some(v)) = row.try_get::<i64, _>(idx) {
        return Some(v.to_string());
    }
    if let Ok(Some(v)) = row.try_get::<f64, _>(idx) {
        return Some(v.to_string());
    }
    if let Ok(Some(v)) = row.try_get::<bool, _>(idx) {
        return Some(v.to_string());
    }
    None
}

fn write_batch_to_format(
    batch: &RecordBatch,
    format: &OutputFormat,
    compression: &CompressionCodec,
) -> Result<Bytes> {
    match format {
        OutputFormat::Parquet => write_parquet(batch, compression),
        OutputFormat::Csv => write_csv(batch, compression),
        OutputFormat::Tsv => write_tsv(batch, compression),
        OutputFormat::Json => write_json(batch, compression),
    }
}

/// Write multiple batches into a single file output.
fn merge_batches_to_format(
    batches: &[RecordBatch],
    format: &OutputFormat,
    compression: &CompressionCodec,
) -> Result<Bytes> {
    match format {
        OutputFormat::Parquet => write_parquet_multi(batches, compression),
        OutputFormat::Csv => write_csv_multi(batches, compression),
        OutputFormat::Tsv => write_tsv_multi(batches, compression),
        OutputFormat::Json => write_json_multi(batches, compression),
    }
}

/// Validate format+compression compatibility and fallback if needed.
fn resolve_compression(format: &OutputFormat, codec: &CompressionCodec) -> CompressionCodec {
    match format {
        OutputFormat::Parquet => match codec {
            CompressionCodec::Gzip => {
                warn!("Parquet does not support gzip compression, falling back to snappy");
                CompressionCodec::Snappy
            }
            _ => *codec,
        },
        OutputFormat::Csv | OutputFormat::Tsv | OutputFormat::Json => match codec {
            CompressionCodec::Snappy => {
                warn!(
                    format = ?format,
                    "CSV/TSV/JSON do not support snappy compression, falling back to gzip"
                );
                CompressionCodec::Gzip
            }
            _ => *codec,
        },
    }
}

fn write_parquet(batch: &RecordBatch, compression: &CompressionCodec) -> Result<Bytes> {
    let comp = match compression {
        CompressionCodec::None => Compression::UNCOMPRESSED,
        CompressionCodec::Snappy => Compression::SNAPPY,
        CompressionCodec::Zstd => Compression::ZSTD(Default::default()),
        CompressionCodec::Gzip => Compression::UNCOMPRESSED, // shouldn't happen after resolve
    };
    let props = WriterProperties::builder().set_compression(comp).build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
        .map_err(|e| StreamXferError::Storage(format!("parquet writer init: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| StreamXferError::Storage(format!("parquet write: {e}")))?;
    writer
        .close()
        .map_err(|e| StreamXferError::Storage(format!("parquet close: {e}")))?;
    Ok(Bytes::from(buf))
}

fn write_csv(batch: &RecordBatch, compression: &CompressionCodec) -> Result<Bytes> {
    let raw = write_csv_raw(batch, b',')?;
    compress_bytes(&raw, compression)
}

fn write_tsv(batch: &RecordBatch, compression: &CompressionCodec) -> Result<Bytes> {
    let raw = write_csv_raw(batch, b'\t')?;
    compress_bytes(&raw, compression)
}

fn write_csv_raw(batch: &RecordBatch, delimiter: u8) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = arrow::csv::WriterBuilder::new()
        .with_header(true)
        .with_delimiter(delimiter)
        .build(&mut buf);
    writer
        .write(batch)
        .map_err(|e| StreamXferError::Storage(format!("csv write: {e}")))?;
    drop(writer);
    Ok(buf)
}

fn write_json(batch: &RecordBatch, compression: &CompressionCodec) -> Result<Bytes> {
    let mut buf = Vec::new();
    let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
    writer
        .write(batch)
        .map_err(|e| StreamXferError::Storage(format!("json write: {e}")))?;
    writer
        .finish()
        .map_err(|e| StreamXferError::Storage(format!("json finish: {e}")))?;
    compress_bytes(&buf, compression)
}

fn compress_bytes(data: &[u8], compression: &CompressionCodec) -> Result<Bytes> {
    use flate2::write::GzEncoder;
    use std::io::Write;

    match compression {
        CompressionCodec::None => Ok(Bytes::from(data.to_vec())),
        CompressionCodec::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder
                .write_all(data)
                .map_err(|e| StreamXferError::Storage(format!("gzip compress: {e}")))?;
            let compressed = encoder
                .finish()
                .map_err(|e| StreamXferError::Storage(format!("gzip finish: {e}")))?;
            Ok(Bytes::from(compressed))
        }
        CompressionCodec::Zstd => {
            let compressed = zstd::encode_all(data, 3)
                .map_err(|e| StreamXferError::Storage(format!("zstd compress: {e}")))?;
            Ok(Bytes::from(compressed))
        }
        CompressionCodec::Snappy => {
            // Shouldn't reach here for text formats after resolve_compression
            Ok(Bytes::from(data.to_vec()))
        }
    }
}

fn write_parquet_multi(batches: &[RecordBatch], compression: &CompressionCodec) -> Result<Bytes> {
    let comp = match compression {
        CompressionCodec::None => Compression::UNCOMPRESSED,
        CompressionCodec::Snappy => Compression::SNAPPY,
        CompressionCodec::Zstd => Compression::ZSTD(Default::default()),
        CompressionCodec::Gzip => Compression::UNCOMPRESSED,
    };
    let props = WriterProperties::builder().set_compression(comp).build();
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
        .map_err(|e| StreamXferError::Storage(format!("parquet writer init: {e}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| StreamXferError::Storage(format!("parquet write: {e}")))?;
    }
    writer
        .close()
        .map_err(|e| StreamXferError::Storage(format!("parquet close: {e}")))?;
    Ok(Bytes::from(buf))
}

fn write_csv_multi(batches: &[RecordBatch], compression: &CompressionCodec) -> Result<Bytes> {
    write_delimited_multi(batches, b',', compression)
}

fn write_tsv_multi(batches: &[RecordBatch], compression: &CompressionCodec) -> Result<Bytes> {
    write_delimited_multi(batches, b'\t', compression)
}

fn write_delimited_multi(
    batches: &[RecordBatch],
    delimiter: u8,
    compression: &CompressionCodec,
) -> Result<Bytes> {
    let mut buf = Vec::new();
    // Write first batch with header
    if let Some((first, rest)) = batches.split_first() {
        let mut writer = arrow::csv::WriterBuilder::new()
            .with_header(true)
            .with_delimiter(delimiter)
            .build(&mut buf);
        writer
            .write(first)
            .map_err(|e| StreamXferError::Storage(format!("csv write: {e}")))?;
        drop(writer);
        // Write remaining batches without header
        for batch in rest {
            let mut writer = arrow::csv::WriterBuilder::new()
                .with_header(false)
                .with_delimiter(delimiter)
                .build(&mut buf);
            writer
                .write(batch)
                .map_err(|e| StreamXferError::Storage(format!("csv write: {e}")))?;
            drop(writer);
        }
    }
    compress_bytes(&buf, compression)
}

fn write_json_multi(batches: &[RecordBatch], compression: &CompressionCodec) -> Result<Bytes> {
    let mut buf = Vec::new();
    let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| StreamXferError::Storage(format!("json write: {e}")))?;
    }
    writer
        .finish()
        .map_err(|e| StreamXferError::Storage(format!("json finish: {e}")))?;
    compress_bytes(&buf, compression)
}

async fn create_sink(uri: &StorageUri) -> Result<Arc<dyn ObjectStore>> {
    use crate::sink::storage::StorageKind;
    match &uri.kind {
        StorageKind::Local => {
            let path = &uri.prefix;
            tokio::fs::create_dir_all(path)
                .await
                .map_err(|e| StreamXferError::Storage(format!("create dir {path}: {e}")))?;
            Ok(Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(path)
                    .map_err(|e| StreamXferError::Storage(format!("local fs: {e}")))?,
            ))
        }
        StorageKind::S3 => {
            let bucket = uri.bucket_or_container.as_deref().unwrap_or_default();
            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

            // Support env vars for S3 config (including MinIO)
            if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
                builder = builder.with_endpoint(&endpoint).with_allow_http(true);
            }
            if let Ok(region) = std::env::var("AWS_REGION") {
                builder = builder.with_region(&region);
            } else {
                builder = builder.with_region("us-east-1");
            }
            if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID") {
                builder = builder.with_access_key_id(&key);
            }
            if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                builder = builder.with_secret_access_key(&secret);
            }
            // Force path-style for MinIO
            if std::env::var("AWS_ENDPOINT_URL").is_ok() {
                builder = builder.with_virtual_hosted_style_request(false);
            }

            let store = builder
                .build()
                .map_err(|e| StreamXferError::Storage(format!("S3 client: {e}")))?;
            Ok(Arc::new(store))
        }
        StorageKind::Gcs => Err(StreamXferError::Storage(
            "GCS sink not yet implemented".into(),
        )),
        StorageKind::Azure => Err(StreamXferError::Storage(
            "Azure sink not yet implemented".into(),
        )),
    }
}

fn format_file_name(
    file_index: usize,
    format: &OutputFormat,
    compression: &CompressionCodec,
) -> String {
    let ext = match format {
        OutputFormat::Parquet => "parquet",
        OutputFormat::Csv => "csv",
        OutputFormat::Tsv => "tsv",
        OutputFormat::Json => "jsonl",
    };
    let comp_ext = match (format, compression) {
        (OutputFormat::Parquet, _) => "", // parquet handles compression internally
        (_, CompressionCodec::None) => "",
        (_, CompressionCodec::Gzip) => ".gz",
        (_, CompressionCodec::Zstd) => ".zst",
        (_, CompressionCodec::Snappy) => "", // shouldn't happen after resolve
    };
    format!("part-{file_index:05}.{ext}{comp_ext}")
}

fn build_output_path(uri: &StorageUri, file_name: &str) -> String {
    let prefix = uri.prefix.trim_end_matches('/');
    if prefix.is_empty() {
        file_name.to_string()
    } else {
        format!("{prefix}/{file_name}")
    }
}

#[derive(Debug, Default)]
pub struct ExecutionResult {
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub rows_exported: usize,
    pub bytes_written: u64,
    pub files_written: usize,
    pub errors: Vec<String>,
}

#[derive(Debug)]
struct TaskResult {
    rows: usize,
    bytes: u64,
    files: usize,
}
