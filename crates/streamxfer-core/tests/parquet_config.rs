use streamxfer_core::config::CompressionCodec;
use streamxfer_core::parquet_pipeline::ParquetWriterConfig;

// ============================================================
// ParquetWriterConfig
// ============================================================

#[test]
fn parquet_config_defaults() {
    let config = ParquetWriterConfig::default();
    assert_eq!(config.compression, CompressionCodec::Snappy);
    assert!(config.zstd_level.is_none());
    assert_eq!(config.max_row_group_size, 128 * 1024 * 1024);
    assert_eq!(config.target_file_size, 256 * 1024 * 1024);
    assert!(config.dictionary_enabled);
}

#[test]
fn parquet_config_file_extension() {
    let config = ParquetWriterConfig::default();
    assert_eq!(config.file_extension(), "parquet");
}

#[test]
fn parquet_config_compression_name_none() {
    let config = ParquetWriterConfig {
        compression: CompressionCodec::None,
        ..Default::default()
    };
    assert_eq!(config.compression_name(), "none");
}

#[test]
fn parquet_config_compression_name_snappy() {
    let config = ParquetWriterConfig {
        compression: CompressionCodec::Snappy,
        ..Default::default()
    };
    assert_eq!(config.compression_name(), "snappy");
}

#[test]
fn parquet_config_compression_name_zstd() {
    let config = ParquetWriterConfig {
        compression: CompressionCodec::Zstd,
        zstd_level: Some(3),
        ..Default::default()
    };
    assert_eq!(config.compression_name(), "zstd");
}

#[test]
fn parquet_config_compression_name_gzip() {
    let config = ParquetWriterConfig {
        compression: CompressionCodec::Gzip,
        ..Default::default()
    };
    assert_eq!(config.compression_name(), "gzip");
}

#[test]
fn parquet_config_custom_values() {
    let config = ParquetWriterConfig {
        compression: CompressionCodec::Zstd,
        zstd_level: Some(9),
        max_row_group_size: 64 * 1024 * 1024,
        target_file_size: 512 * 1024 * 1024,
        dictionary_enabled: false,
    };
    assert_eq!(config.zstd_level, Some(9));
    assert_eq!(config.max_row_group_size, 64 * 1024 * 1024);
    assert_eq!(config.target_file_size, 512 * 1024 * 1024);
    assert!(!config.dictionary_enabled);
}

#[test]
fn parquet_config_serializes_and_deserializes() {
    let config = ParquetWriterConfig {
        compression: CompressionCodec::Zstd,
        zstd_level: Some(5),
        max_row_group_size: 100,
        target_file_size: 200,
        dictionary_enabled: false,
    };
    let json = serde_json::to_string(&config).unwrap();
    let deserialized: ParquetWriterConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.compression, CompressionCodec::Zstd);
    assert_eq!(deserialized.zstd_level, Some(5));
    assert_eq!(deserialized.max_row_group_size, 100);
    assert_eq!(deserialized.target_file_size, 200);
    assert!(!deserialized.dictionary_enabled);
}
