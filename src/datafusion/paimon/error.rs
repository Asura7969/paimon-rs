use datafusion_sql::sqlparser::parser::ParserError;
use object_store::Error as ObjectStoreError;
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

    #[error("Generic DeltaTable error: {0}")]
    Generic(String),

    #[error("Paimon-rs must be build with feature '{feature}' to support loading from: {url}.")]
    MissingFeature {
        /// Name of the missing feature
        feature: &'static str,
        /// Storage location url
        url: String,
    },

    #[error("Failed to read paimon log object: {}", .source)]
    ObjectStore {
        /// Storage error details when reading the delta log object failed.
        #[from]
        source: ObjectStoreError,
    },
}
