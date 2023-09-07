use datafusion::{datasource::listing::ListingTableUrl, error::Result};
use datafusion_common::DataFusionError;
use object_store::DynObjectStore;
use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef, datasource::TableProvider, execution::context::SessionState,
    physical_plan::ExecutionPlan,
};
use datafusion_expr::{Expr, TableType};

use crate::datafusion::{
    builder::PaimonTableBuilder,
    paimon::{exec::MergeExec, reader::generate_execution_plan},
};

use super::{snapshot::Snapshot, PaimonSchema};

#[allow(dead_code)]
pub struct PaimonProvider {
    pub table_path: ListingTableUrl,
    pub(crate) snapshot: Snapshot,
    pub(crate) storage: Arc<DynObjectStore>,
    // TODO: state 更合适，此处为临时处理方案
    pub schema: Option<PaimonSchema>,
}

impl PaimonProvider {
    pub async fn load(&mut self) -> datafusion::error::Result<()> {
        let schema = self
            .snapshot
            .get_schema(&self.storage)
            .await
            .map_err(|e| DataFusionError::NotImplemented(e.to_string()))?;
        self.schema = Some(schema);
        Ok(())
    }
}

#[async_trait]
impl TableProvider for PaimonProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut schema = self.schema.clone().expect("Load method not called ...");
        Arc::new(schema.arrow_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut paimon_schema = self.schema.clone().unwrap();
        let entries = self.snapshot.all(&self.storage).await.unwrap();

        let new_projection = if let Some(idxes) = projection {
            // 表的主键个数 + seq_num + RowKind
            let pk_len = paimon_schema.primary_keys.len() + 2;
            if pk_len == 2 {
                // 没有主键  append only表
                Some(idxes.clone())
            } else {
                let mut with_sys_column = idxes
                    .iter()
                    .map(|id: &usize| id + pk_len)
                    .collect::<Vec<_>>();

                for id in 0..pk_len {
                    with_sys_column.push(id);
                }
                with_sys_column.sort();
                Some(with_sys_column)
            }
        } else {
            None
        };

        let exec = generate_execution_plan(
            &self.table_path,
            &entries,
            &mut paimon_schema,
            new_projection,
            filters,
            limit,
        )
        .unwrap();

        Ok(Arc::new(MergeExec::new(paimon_schema, exec, limit)))
    }
}

pub async fn open_table(
    state: &SessionState,
    table_uri: impl AsRef<str>,
) -> datafusion::error::Result<PaimonProvider> {
    let table = PaimonTableBuilder::from_uri(table_uri, state.clone())
        .load()
        .await?;
    Ok(table)
}

pub async fn open_table_with_storage_options(
    state: &SessionState,
    table_uri: impl AsRef<str>,
    storage_options: HashMap<String, String>,
) -> datafusion::error::Result<PaimonProvider> {
    let table = PaimonTableBuilder::from_uri(table_uri, state.clone())
        .with_storage_options(storage_options)
        .load()
        .await?;
    Ok(table)
}

#[allow(dead_code)]
pub async fn open_table_with_version(
    _table_uri: impl AsRef<str>,
    _tag: impl AsRef<str>,
) -> datafusion::error::Result<PaimonProvider> {
    todo!()
}

#[allow(unused_imports)]
#[cfg(test)]
mod tests {
    use arrow::util::pretty::print_batches as arrow_print_batches;
    use bytes::Bytes;
    use datafusion::{
        datasource::provider_as_source,
        prelude::{SessionConfig, SessionContext},
    };
    use datafusion_expr::LogicalPlan;
    use datafusion_sql::{
        planner::{ParserOptions, SqlToRel},
        sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser},
    };
    use object_store::{local::LocalFileSystem, ObjectStore};

    use super::*;
    use std::{
        collections::hash_map::Entry,
        path::{Path, PathBuf},
        str::from_utf8,
    };

    use crate::datafusion::{
        dialect::PaimonDialect,
        paimon::{
            error::PaimonError, test_local_store, test_paimonm_table_path, PartitionKeys,
            PrimaryKeys, WriteMode,
        },
    };

    fn get_local_file_system(url: &ListingTableUrl) -> String {
        #[cfg(windows)]
        {
            format!("{}", &url.prefix().as_ref())
        }

        #[cfg(unix)]
        {
            format!("/{}", &url.prefix().as_ref())
        }
    }

    #[tokio::test]
    async fn object_store_test() -> Result<(), PaimonError> {
        let path = "ods_mysql_paimon_points_5";

        let path = test_paimonm_table_path(path);
        let url = ListingTableUrl::parse(path.as_str())?;
        let local = get_local_file_system(&url);

        let store = LocalFileSystem::new_with_prefix(Path::new(local.as_str())).unwrap();

        let _expected_data = r#"
        {
          "version" : 3,
          "id" : 5,
          "schemaId" : 0,
          "baseManifestList" : "manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-12",
          "deltaManifestList" : "manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-13",
          "changelogManifestList" : null,
          "commitUser" : "e75f405b-210d-4d84-b350-ec445fed9530",
          "commitIdentifier" : 6,
          "commitKind" : "APPEND",
          "timeMillis" : 1691031342569,
          "logOffsets" : { },
          "totalRecordCount" : 9,
          "deltaRecordCount" : 0,
          "changelogRecordCount" : 0,
          "watermark" : -9223372036854775808
        }"#;

        let location = object_store::path::Path::from("snapshot/snapshot-5");
        let read_data = store.get(&location).await.unwrap().bytes().await.unwrap();
        let _d = String::from_utf8_lossy(read_data.split_at(read_data.len()).0);
        // assert_eq!(d.to_string().as_str(), expected_data);
        Ok(())
    }
}
