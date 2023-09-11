use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    datasource::{listing::ListingTableUrl, provider::TableProviderFactory, TableProvider},
    execution::context::SessionState,
};
use datafusion_cli::object_storage::{get_oss_object_store_builder, get_s3_object_store_builder};
use datafusion_common::DataFusionError;
use datafusion_expr::CreateExternalTable;
#[cfg(feature = "hdfs")]
use datafusion_objectstore_hdfs::object_store::hdfs::HadoopFileSystem;
use object_store::{local::LocalFileSystem, ObjectStore};
use url::Url;

use super::paimon::{
    error::PaimonError,
    table::{open_table, open_table_with_storage_options},
};

pub struct PaimonTableFactory {}

#[async_trait]
impl TableProviderFactory for PaimonTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        create_external_table(state, cmd).await?;

        let location = cmd.to_owned().location;

        let provider = if cmd.options.is_empty() {
            open_table(state, location).await?
        } else {
            open_table_with_storage_options(state, location, cmd.to_owned().options).await?
        };
        Ok(Arc::new(provider))
    }
}

async fn create_external_table(
    state: &SessionState,
    cmd: &CreateExternalTable,
) -> datafusion::error::Result<()> {
    let table_path: ListingTableUrl = ListingTableUrl::parse(&cmd.location)?;
    let scheme = table_path.scheme();
    let url: &Url = table_path.as_ref();

    // registering the cloud object store dynamically using cmd.options
    let store = match scheme {
        "s3" => {
            let builder = get_s3_object_store_builder(url, cmd).await?;
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }
        "oss" => {
            let builder = get_oss_object_store_builder(url, cmd)?;
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }
        "hdfs" => try_hdfs(url).expect("don't create hdfs object store"),
        "file" => {
            #[cfg(windows)]
            {
                let loacl = (&table_path.prefix().as_ref()).to_string();
                Arc::new(LocalFileSystem::new_with_prefix(Path::new(loacl.as_str()))?)
            }

            #[cfg(unix)]
            {
                let loacl = format!("/{}", &table_path.prefix().as_ref());
                Arc::new(LocalFileSystem::new_with_prefix(Path::new(loacl.as_str()))?)
            }
        }
        _ => {
            // for other types, try to get from the object_store_registry
            state
                .runtime_env()
                .object_store_registry
                .get_store(url)
                .map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Unsupported object store scheme: {}",
                        scheme
                    ))
                })?
        }
    };

    state.runtime_env().register_object_store(url, store);

    Ok(())
}

#[cfg(feature = "hdfs")]
fn try_hdfs(url: &Url) -> Result<Arc<dyn ObjectStore>, PaimonError> {
    Ok(Arc::new(
        HadoopFileSystem::new(url.as_ref()).ok_or_else(|| {
            PaimonError::Generic(format!(
                "failed to create HadoopFileSystem for {}",
                url.as_ref()
            ))
        })? as Arc<dyn ObjectStore>,
    ))
}

#[cfg(not(feature = "hdfs"))]
fn try_hdfs(url: &Url) -> Result<Arc<dyn ObjectStore>, PaimonError> {
    Err(PaimonError::MissingFeature {
        feature: "hdfs",
        url: url.as_ref().into(),
    })
}

#[allow(unused_imports)]
#[cfg(test)]
mod tests {
    use crate::datafusion::{
        context_with_delta_table_factory,
        paimon::{error::PaimonError, test_paimonm_table_path},
    };
    use arrow::util::pretty::print_batches as arrow_print_batches;

    #[tokio::test]
    async fn test_datafusion_sql_registration() -> Result<(), PaimonError> {
        let ctx = context_with_delta_table_factory();
        let d = test_paimonm_table_path("ods_mysql_paimon_points_5");

        println!("d: {}", d);

        let sql = format!(
            "CREATE EXTERNAL TABLE ods_mysql_paimon_points_5 STORED AS PAIMON OPTIONS ('scan.snapshot-id' '5') LOCATION '{}'",
            d.as_str()
        );

        println!("sql: {}", sql);

        let _ = ctx
            .sql(sql.as_str())
            .await
            .expect("Failed to register table!");

        let batches = ctx
            .sql("SELECT point_id,address FROM ods_mysql_paimon_points_5")
            .await?
            .collect()
            .await?;

        // arrow_print_batches(&batches).unwrap();
        assert!(arrow_print_batches(&batches).is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_read_avro_table() -> Result<(), PaimonError> {
        let ctx = context_with_delta_table_factory();
        let d = test_paimonm_table_path("avro_parquet_table");

        let sql = format!(
            "CREATE EXTERNAL TABLE avro_parquet_table STORED AS PAIMON OPTIONS ('scan.snapshot-id' '1') LOCATION '{}'",
            d.as_str()
        );

        let _ = ctx
            .sql(sql.as_str())
            .await
            .expect("Failed to register table!");

        let batches = ctx
            .sql("SELECT point_id,address,create_time FROM avro_parquet_table limit 3")
            .await?
            .collect()
            .await?;

        // arrow_print_batches(&batches).unwrap();
        assert!(arrow_print_batches(&batches).is_ok());
        Ok(())
    }
}
