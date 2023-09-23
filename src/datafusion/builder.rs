use chrono::{DateTime, Utc};
use datafusion::{datasource::listing::ListingTableUrl, execution::context::SessionState};
use std::{collections::HashMap, sync::Arc};
use url::Url;

use object_store::DynObjectStore;

use super::paimon::{snapshot::SnapshotManager, table::PaimonProvider};

pub const SCAN_SNAPSHOT_ID: &str = "scan.snapshot-id";

#[derive(Debug)]
pub struct PaimonTableLoadOptions {
    /// table root uri
    pub table_uri: String,
    /// backend to access storage system
    pub storage_backend: Option<(Arc<DynObjectStore>, Url)>,

    pub options: HashMap<String, String>,
}

impl PaimonTableLoadOptions {
    /// create default table load options for a table uri
    pub fn new(table_uri: impl Into<String>) -> Self {
        Self {
            table_uri: table_uri.into(),
            storage_backend: None,
            options: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct PaimonTableBuilder {
    options: PaimonTableLoadOptions,
    storage_options: Option<HashMap<String, String>>,
    state: SessionState,
}

#[allow(dead_code)]
#[allow(unused_mut)]
impl PaimonTableBuilder {
    pub fn from_uri(table_uri: impl AsRef<str>, state: SessionState) -> Self {
        Self {
            options: PaimonTableLoadOptions::new(table_uri.as_ref()),
            storage_options: None,
            state,
        }
    }

    pub fn with_version(mut self, _version: i64) -> Self {
        todo!()
    }

    pub fn with_timestamp(mut self, _timestamp: DateTime<Utc>) -> Self {
        todo!()
    }

    pub fn with_storage_backend(mut self, storage: Arc<DynObjectStore>, location: Url) -> Self {
        self.options.storage_backend = Some((storage, location));
        self
    }

    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    pub async fn build(self) -> datafusion::error::Result<PaimonProvider> {
        // let config = DeltaTableConfig {
        //     require_tombstones: self.options.require_tombstones,
        //     require_files: self.options.require_files,
        // };
        let path = self.options.table_uri;
        let url = ListingTableUrl::parse(path)?;

        let storage = &self.state.runtime_env().object_store(url.clone())?;

        let manager = SnapshotManager::new(url.clone(), storage.clone());

        let snapshot = if self.options.options.contains_key(SCAN_SNAPSHOT_ID) {
            let id = self
                .options
                .options
                .get(SCAN_SNAPSHOT_ID)
                .map(|s| s.parse::<i64>().expect("snapshot id error"))
                .unwrap();
            manager
                .snapshot(id)
                .await
                .unwrap_or_else(|_| panic!("read snapshot failed, id: {}", id))
        } else {
            manager
                .latest_snapshot()
                .await
                .expect("not find latest snapshot")
        };

        Ok(PaimonProvider {
            table_path: url,
            snapshot,
            storage: storage.clone(),
            schema: None,
        })
    }

    /// Build the [`PaimonTable`] and load its state
    pub async fn load(self) -> datafusion::error::Result<PaimonProvider> {
        // let version = self.options.version.clone();
        let mut table = self.build().await?;
        // match version {
        //     DeltaVersion::Newest => table.load().await?,
        //     DeltaVersion::Version(v) => table.load_version(v).await?,
        //     DeltaVersion::Timestamp(ts) => table.load_with_datetime(ts).await?,
        // }
        table.load().await?;
        Ok(table)
    }
}
