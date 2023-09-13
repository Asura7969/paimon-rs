use datafusion::{datasource::listing::ListingTableUrl, execution::context::SessionState};
use datafusion_common::DataFusionError;
use datafusion_sql::sqlparser::parser::ParserError;
use std::{collections::HashMap, sync::Arc};
use url::Url;

use object_store::DynObjectStore;

use super::paimon::{snapshot::SnapshotManager, table::PaimonProvider};

pub const SCAN_SNAPSHOT_ID: &str = "scan.snapshot-id";
pub const CONSUMER_ID: &str = "consumer-id";
pub const SCAN_TAG_NAME: &str = "scan.tag-name";

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

    pub fn get_scan_mode(&self) -> ScanModeEnum {
        ScanModeEnum::from_option(&self.options)
    }
}

pub enum ScanModeEnum {
    SnapshotId(i64),
    TagName(String),
    ConsumerId(i64),
    Latest,
}

impl ScanModeEnum {
    pub fn from_option(option: &HashMap<String, String>) -> ScanModeEnum {
        if option.contains_key(SCAN_SNAPSHOT_ID) {
            let id = option
                .get(SCAN_SNAPSHOT_ID)
                .map(|s| s.parse::<i64>().expect("snapshot id error"))
                .unwrap();
            ScanModeEnum::SnapshotId(id)
        } else if option.contains_key(CONSUMER_ID) {
            let id = option
                .get(CONSUMER_ID)
                .map(|s| s.parse::<i64>().expect("snapshot id error"))
                .unwrap();
            ScanModeEnum::ConsumerId(id)
        } else if option.contains_key(SCAN_TAG_NAME) {
            let tag_name = option.get(SCAN_TAG_NAME).unwrap().clone();
            ScanModeEnum::TagName(tag_name)
        } else {
            ScanModeEnum::Latest
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

    pub fn with_storage_backend(mut self, storage: Arc<DynObjectStore>, location: Url) -> Self {
        self.options.storage_backend = Some((storage, location));
        self
    }

    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    fn throw_err(msg: String) -> DataFusionError {
        DataFusionError::SQL(ParserError::ParserError(msg))
    }

    pub async fn build(self) -> datafusion::error::Result<PaimonProvider> {
        let path = &self.options.table_uri;
        let url = ListingTableUrl::parse(path)?;

        let storage = &self.state.runtime_env().object_store(url.clone())?;

        let manager = SnapshotManager::new(url.clone(), storage.clone());

        let snapshot = match &self.options.get_scan_mode() {
            ScanModeEnum::SnapshotId(snapshot_id) => manager.snapshot(*snapshot_id).await,
            ScanModeEnum::ConsumerId(consumer_id) => manager.consumer(*consumer_id).await,
            ScanModeEnum::TagName(tag_name) => manager.tag(tag_name.as_str()).await,
            ScanModeEnum::Latest => manager.latest_snapshot().await,
        }
        .map_err(|err| Self::throw_err(err.to_string()))?;

        Ok(PaimonProvider {
            table_path: url,
            snapshot,
            storage: storage.clone(),
            schema: None,
        })
    }

    /// Build the [`PaimonTable`] and load its state
    pub async fn load(self) -> datafusion::error::Result<PaimonProvider> {
        let mut table = self.build().await?;
        table.load().await?;
        Ok(table)
    }
}
