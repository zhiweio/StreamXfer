use thiserror::Error;

pub type Result<T> = std::result::Result<T, StreamXferError>;

#[derive(Debug, Error)]
pub enum StreamXferError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid SQL Server identifier: {0}")]
    InvalidIdentifier(String),
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("checkpoint error: {0}")]
    Checkpoint(String),
    #[error("source error: {0}")]
    Source(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
