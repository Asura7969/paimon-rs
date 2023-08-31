use std::sync::Arc;

use apache_avro::{from_value, Reader as AvroReader};
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion_common::Statistics;
use datafusion_expr::Expr;
use nom::AsBytes;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::datafusion::paimon::ManifestFileMeta;

use super::error::PaimonError;
use super::manifest::ManifestEntry;
use super::{add_system_fields, PaimonSchema};
use object_store::{DynObjectStore, ObjectMeta};
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
        FileFormat::Avro => read_avro::<ManifestFileMeta>(storage, location).await,
        FileFormat::Parquet => unimplemented!(),
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
        FileFormat::Avro => read_avro::<ManifestEntry>(storage, location).await,
        FileFormat::Parquet => unimplemented!(),
        FileFormat::Orc => unimplemented!(),
    }
}

async fn read_avro<T: Serialize + for<'a> Deserialize<'a>>(
    storage: &Arc<DynObjectStore>,
    location: &Path,
) -> Result<Vec<T>, PaimonError> {
    // TODO: remote read, such as OSS, HDFS, etc.
    let bytes = storage.get(location).await.unwrap().bytes().await.unwrap();
    // let r: fs::File = fs::File::open(path)?;
    let reader = AvroReader::new(bytes.as_bytes())?;
    // let writer_schema = reader.writer_schema().clone();
    // println!("schema: {:?}", writer_schema);

    let mut manifestlist: Vec<T> = Vec::new();

    for value in reader {
        let record = value.unwrap();
        // println!("{:?}", record);
        let meta: T = from_value::<T>(&record)?;
        manifestlist.push(meta);
    }

    // let serialized = serde_json::to_string(&manifestlist).unwrap();
    // println!("{}", serialized);

    Ok(manifestlist)
}

#[allow(dead_code)]
pub fn read_parquet(
    url: &ListingTableUrl,
    entries: &[ManifestEntry],
    file_schema: &mut PaimonSchema,
    projection: Option<Vec<usize>>,
    _filters: &[Expr],
    _limit: Option<usize>,
) -> Result<ParquetExec, PaimonError> {
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
    let file_schema = add_system_fields(file_schema.arrow_schema())?;
    // Create a async parquet reader builder with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024

    // schema_coercion.rs
    let parquet_exec: ParquetExec = ParquetExec::new(
        FileScanConfig {
            object_store_url: url.object_store(),
            file_groups: vec![file_groups],
            file_schema,
            statistics: Statistics::default(),
            projection,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
            infinite_source: false,
        },
        None,
        None,
    );

    Ok(parquet_exec)
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
    async fn read_manifest_test() -> Result<(), PaimonError> {
        let (_url, storage) = test_local_store("ods_mysql_paimon_points_5").await;
        let manifest = read_avro::<ManifestEntry>(
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
        let path = format!("{}/bucket-0/data-a53acd62-0d99-43d6-8ffe-a76ac3b719d9-1.parquet", path);

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
