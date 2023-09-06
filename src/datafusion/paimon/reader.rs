use std::ops::Range;
use std::sync::{Arc, Mutex};

use super::error::PaimonError;
use super::json_util::record_batches_to_json_rows;
use super::manifest::ManifestEntry;
use super::{add_system_fields, PaimonSchema};
use crate::datafusion::paimon::ManifestFileMeta;
use apache_avro::{from_value, Reader as AvroReader};
use bytes::Bytes;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{AvroExec, FileScanConfig, ParquetExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Statistics;
use datafusion_expr::Expr;
use futures::future::{BoxFuture, FutureExt};
use futures::TryStreamExt;
use nom::AsBytes;
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectMeta};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::footer::parse_metadata;
use parquet::file::metadata::ParquetMetaData;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
pub enum FileFormat {
    #[allow(dead_code)]
    Parquet,
    #[allow(dead_code)]
    Avro,
    #[allow(dead_code)]
    Orc,
}

impl From<&String> for FileFormat {
    fn from(value: &String) -> Self {
        match value.as_str() {
            "parquet" => FileFormat::Parquet,
            "orc" => FileFormat::Orc,
            _ => FileFormat::Avro,
        }
    }
}

pub async fn manifest_list(
    storage: &Arc<DynObjectStore>,
    location: &Path,
    format: &FileFormat,
) -> Result<Vec<ManifestFileMeta>, PaimonError> {
    match format {
        FileFormat::Avro => read_avro_bytes::<ManifestFileMeta>(storage, location).await,
        FileFormat::Parquet => read_parquet_bytes::<ManifestFileMeta>(storage, location).await,
        FileFormat::Orc => unimplemented!(),
    }
}

#[allow(dead_code)]
pub async fn manifest(
    storage: &Arc<DynObjectStore>,
    location: &Path,
    format: &FileFormat,
) -> Result<Vec<ManifestEntry>, PaimonError> {
    match format {
        FileFormat::Avro => read_avro_bytes::<ManifestEntry>(storage, location).await,
        FileFormat::Parquet => read_parquet_bytes::<ManifestEntry>(storage, location).await,
        FileFormat::Orc => unimplemented!(),
    }
}

#[derive(Clone)]
struct ParquetBytesReader {
    data: Bytes,
    metadata: Arc<ParquetMetaData>,
    requests: Arc<Mutex<Vec<Range<usize>>>>,
}

impl AsyncFileReader for ParquetBytesReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.requests.lock().unwrap().push(range.clone());
        futures::future::ready(Ok(self.data.slice(range))).boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        futures::future::ready(Ok(self.metadata.clone())).boxed()
    }
}

async fn read_parquet_bytes<T>(
    storage: &Arc<DynObjectStore>,
    location: &Path,
) -> Result<Vec<T>, PaimonError>
where
    T: Serialize + for<'a> Deserialize<'a> + for<'a> From<&'a Map<String, Value>>,
{
    let bytes = storage.get(location).await?.bytes().await?;
    let metadata = parse_metadata(&bytes)?;
    let metadata = Arc::new(metadata);

    let async_reader = ParquetBytesReader {
        data: bytes,
        metadata,
        requests: Default::default(),
    };

    // let _requests = async_reader.requests.clone();
    let builder = ParquetRecordBatchStreamBuilder::new(async_reader)
        .await
        .unwrap();

    let mask = ProjectionMask::leaves(builder.parquet_schema(), 0..builder.schema().fields.len());
    let stream = builder
        .with_projection(mask.clone())
        .with_batch_size(1024)
        .build()?;

    let batches: Vec<_> = stream.try_collect().await?;

    let json_rows = batches
        .iter()
        .flat_map(|batch| record_batches_to_json_rows(&[batch]).unwrap())
        .collect::<Vec<Map<String, Value>>>();

    let list = json_rows.iter().map(|row| row.into()).collect::<Vec<T>>();

    Ok(list)
}

async fn read_avro_bytes<T: Serialize + for<'a> Deserialize<'a>>(
    storage: &Arc<DynObjectStore>,
    location: &Path,
) -> Result<Vec<T>, PaimonError> {
    let bytes = storage.get(location).await?.bytes().await?;
    // let r: fs::File = fs::File::open(path)?;
    let reader = AvroReader::new(bytes.as_bytes())?;
    // let writer_schema = reader.writer_schema().clone();
    // println!("schema: {:?}", writer_schema);

    let mut manifestlist: Vec<T> = Vec::new();

    for value in reader {
        let record = value?;
        // println!("{:?}", record);
        let meta: T = from_value::<T>(&record)?;
        manifestlist.push(meta);
    }

    // let serialized = serde_json::to_string(&manifestlist).unwrap();
    // println!("{}", serialized);

    Ok(manifestlist)
}

#[allow(dead_code)]
pub fn generate_execution_plan(
    url: &ListingTableUrl,
    entries: &[ManifestEntry],
    file_schema: &mut PaimonSchema,
    projection: Option<Vec<usize>>,
    _filters: &[Expr],
    _limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>, PaimonError> {
    let file_groups = entries
        .iter()
        .filter(|m| m.kind == 0 && m.file.is_some())
        .map(|e| {
            let p: Option<ObjectMeta> = e.to_object_meta(url);
            p
        })
        .filter(|o| o.is_some())
        .map(|o| Into::into(o.unwrap()))
        .collect::<Vec<PartitionedFile>>();

    let file_format = file_schema.get_file_format();
    let file_schema = add_system_fields(file_schema)?;
    // Create a async parquet reader builder with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024

    let base_config = FileScanConfig {
        object_store_url: url.object_store(),
        file_groups: vec![file_groups],
        file_schema,
        statistics: Statistics::default(),
        projection,
        limit: None,
        // TODO: fill partition column
        table_partition_cols: vec![],
        output_ordering: vec![],
        infinite_source: false,
    };

    match file_format {
        FileFormat::Parquet => Ok(Arc::new(ParquetExec::new(base_config, None, None))),
        FileFormat::Avro => Ok(Arc::new(AvroExec::new(base_config))),
        FileFormat::Orc => Err(PaimonError::Generic(
            "No support orc file format data".to_string(),
        )),
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod tests {

    use super::*;
    use crate::datafusion::paimon::{
        error::PaimonError, exec::MergeExec, manifest::ManifestEntry, snapshot::SnapshotManager,
        test_local_store, test_paimonm_table_path, PartitionKeys, PrimaryKeys, WriteMode,
    };
    use arrow::util::pretty::print_batches as arrow_print_batches;
    use datafusion::{
        common::{config::ConfigExtension, extensions_options},
        config::ConfigOptions,
        datasource::listing::ListingTableUrl,
        execution::{context::SessionState, runtime_env::RuntimeEnv, TaskContext},
        prelude::SessionConfig,
    };
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    use futures::TryStreamExt;
    use parquet::arrow::{
        arrow_reader::{ArrowPredicateFn, RowFilter},
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    };
    use std::{collections::HashMap, sync::Arc, time::SystemTime};
    use tokio::fs::File;

    #[tokio::test]
    async fn read_parquet_manifest_test() -> Result<(), PaimonError> {
        let (_url, storage) = test_local_store("avro_parquet_table").await;
        let manifest = read_parquet_bytes::<ManifestEntry>(
            &storage,
            &Path::from_iter(vec![
                "manifest",
                "manifest-90cbba21-37fe-4e9e-ba86-71e680b87955-0",
            ]),
        )
        .await?;

        let serialized = serde_json::to_string(&manifest).unwrap();
        println!("{}", serialized);

        Ok(())
    }

    #[tokio::test]
    async fn read_avro_manifest_test() -> Result<(), PaimonError> {
        let (_url, storage) = test_local_store("ods_mysql_paimon_points_5").await;
        let manifest = read_avro_bytes::<ManifestEntry>(
            &storage,
            &Path::from_iter(vec![
                "manifest",
                "manifest-5246a8f1-fdf4-4524-a2a2-fcd99dc08a1b-0",
            ]),
        )
        .await?;

        let serialized = serde_json::to_string(&manifest).unwrap();
        println!("{}", serialized);

        Ok(())
    }

    #[tokio::test]
    async fn async_read_parquet_files_test() -> Result<(), PaimonError> {
        let path = test_paimonm_table_path("many_pk_table");
        let path = format!(
            "{}/bucket-0/data-a53acd62-0d99-43d6-8ffe-a76ac3b719d9-1.parquet",
            path
        );

        let file = File::open(path).await.unwrap();

        // Create a async parquet reader builder with batch_size.
        // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
        let mut builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .with_batch_size(8192);

        let file_metadata = builder.metadata().file_metadata().clone();
        let mask = ProjectionMask::roots(
            file_metadata.schema_descr(),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        );
        // Set projection mask to read only root columns 1 and 2.
        builder = builder.with_projection(mask);

        // Highlight: set `RowFilter`, it'll push down filter predicates to skip IO and decode.
        // For more specific usage: please refer to https://github.com/apache/arrow-datafusion/blob/master/datafusion/core/src/physical_plan/file_format/parquet/row_filter.rs.

        // let filter = ArrowPredicateFn::new(
        //     ProjectionMask::roots(file_metadata.schema_descr(), [0]),
        //     |record_batch| arrow::compute::eq_dyn_scalar(record_batch.column(0), 1),
        // );
        // let row_filter = RowFilter::new(vec![Box::new(filter)]);
        // builder = builder.with_row_filter(row_filter);

        // Build a async parquet reader.
        let stream = builder.build().unwrap();

        let start = SystemTime::now();

        let result: Vec<arrow_array::RecordBatch> = stream.try_collect::<Vec<_>>().await?;

        println!("took: {} ms", start.elapsed().unwrap().as_millis());

        arrow_print_batches(&result).unwrap();

        Ok(())
    }
}
