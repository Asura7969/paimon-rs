use datafusion_sql::sqlparser::parser::ParserError;
use parquet::errors::ParquetError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PaimonError {
    #[error("parquet error")]
    ParquetError(#[from] ParquetError),

    #[error("datafusion error")]
    DatafusionError(#[from] datafusion_common::DataFusionError),

    #[error("serde_json error")]
    SerdeJsonError(#[from] serde_json::error::Error),

    #[error("std::io error")]
    StdIoError(#[from] std::io::Error),

    #[error("apache_avro error")]
    AvroError(#[from] apache_avro::Error),

    #[error("apache arrow error")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("parser error")]
    ParserError(#[from] ParserError),
}
