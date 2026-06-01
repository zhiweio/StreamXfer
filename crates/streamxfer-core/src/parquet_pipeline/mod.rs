use crate::config::CompressionCodec;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetWriterConfig {
    pub compression: CompressionCodec,
    pub zstd_level: Option<i32>,
    pub max_row_group_size: usize,
    pub target_file_size: u64,
    pub dictionary_enabled: bool,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            compression: CompressionCodec::Snappy,
            zstd_level: None,
            max_row_group_size: 128 * 1024 * 1024,
            target_file_size: 256 * 1024 * 1024,
            dictionary_enabled: true,
        }
    }
}

impl ParquetWriterConfig {
    pub fn file_extension(&self) -> &'static str {
        "parquet"
    }
    pub fn compression_name(&self) -> &'static str {
        match self.compression {
            CompressionCodec::None => "none",
            CompressionCodec::Snappy => "snappy",
            CompressionCodec::Zstd => "zstd",
            CompressionCodec::Gzip => "gzip",
        }
    }
}
