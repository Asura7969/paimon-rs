use std::{collections::HashMap, sync::Arc};

use datafusion::datasource::listing::ListingTableUrl;
use futures::future::join_all;
use object_store::{path::Path, DynObjectStore};
use serde::{Deserialize, Serialize};

use super::{
    consumer::Consumer, error::PaimonError, manifest_list::ManifestFileMeta, reader::manifest_list,
    CommitKind, PaimonSchema,
};
use crate::datafusion::paimon::{manifest::ManifestEntry, utils::read_to_string};

#[allow(dead_code)]
const SNAPSHOT_PREFIX: &str = "snapshot-";
#[allow(dead_code)]
const TAG_PREFIX: &str = "tag-";
#[allow(dead_code)]
const CONSUMER_PREFIX: &str = "consumer-";
#[allow(dead_code)]
const EARLIEST: &str = "EARLIEST";
const LATEST: &str = "LATEST";

pub struct SnapshotState {
    pub sp: Snapshot,
    pub schema: PaimonSchema,
    pub all: Vec<ManifestEntry>,
    pub base: Vec<ManifestEntry>,
    pub delta: Vec<ManifestEntry>,
}

impl SnapshotState {}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Snapshot {
    version: Option<i32>,
    id: i64,
    #[serde(rename = "schemaId")]
    schema_id: i64,
    #[serde(rename = "baseManifestList")]
    base_manifest_list: String,
    #[serde(rename = "deltaManifestList")]
    delta_manifest_list: String,
    #[serde(rename = "changelogManifestList")]
    changelog_manifest_list: Option<String>,
    #[serde(rename = "indexManifest")]
    index_manifest: Option<String>,
    #[serde(rename = "commitUser")]
    commit_user: String,
    #[serde(rename = "commitIdentifier")]
    commit_identifier: i64,
    #[serde(rename = "commitKind")]
    commit_kind: CommitKind,
    #[serde(rename = "timeMillis")]
    time_millis: i64,
    #[serde(rename = "logOffsets")]
    log_offsets: HashMap<i32, i64>,
    #[serde(rename = "totalRecordCount")]
    total_record_count: Option<i64>,
    #[serde(rename = "deltaRecordCount")]
    delta_record_count: Option<i64>,
    #[serde(rename = "changelogRecordCount")]
    changelog_record_count: Option<i64>,
    watermark: Option<i64>,
}

impl Snapshot {
    pub async fn get_schema(
        &self,
        storage: &Arc<DynObjectStore>,
    ) -> Result<PaimonSchema, PaimonError> {
        let latest_schema_path = format!("/schema/schema-{}", self.schema_id);
        let schema_str = read_to_string(storage, &Path::from(latest_schema_path)).await?;
        let schema: PaimonSchema = serde_json::from_str(schema_str.as_str())?;
        Ok(schema)
    }

    #[allow(dead_code)]
    pub async fn all(
        &self,
        storage: &Arc<DynObjectStore>,
    ) -> Result<Vec<ManifestEntry>, PaimonError> {
        let path = format!("/manifest/{}", self.delta_manifest_list);
        let schema = self.get_schema(storage).await?;
        let format = &schema.get_manifest_format();
        let mut delta_file_meta = manifest_list(storage, &Path::from(path), format).await?;

        let path = format!("/manifest/{}", self.base_manifest_list);
        let mut base_file_meta = manifest_list(storage, &Path::from(path), format).await?;
        delta_file_meta.append(&mut base_file_meta);
        let entry = self
            .get_manifest_entry(delta_file_meta, storage, &schema)
            .await;
        Ok(entry)
    }

    #[allow(dead_code)]
    pub async fn base(
        &self,
        storage: &Arc<DynObjectStore>,
    ) -> Result<Vec<ManifestEntry>, PaimonError> {
        let path = format!("/manifest/{}", self.base_manifest_list);
        let schema = self.get_schema(storage).await?;
        let file_meta =
            manifest_list(storage, &Path::from(path), &schema.get_manifest_format()).await?;

        let entry = self.get_manifest_entry(file_meta, storage, &schema).await;
        Ok(entry)
    }

    #[allow(dead_code)]
    pub async fn delta(
        &self,
        storage: &Arc<DynObjectStore>,
    ) -> Result<Vec<ManifestEntry>, PaimonError> {
        let path = format!("/manifest/{}", self.delta_manifest_list);
        let schema = self.get_schema(storage).await?;
        let file_meta =
            manifest_list(storage, &Path::from(path), &schema.get_manifest_format()).await?;

        let entry = self.get_manifest_entry(file_meta, storage, &schema).await;
        Ok(entry)
    }

    async fn get_manifest_entry(
        &self,
        file_meta: Vec<ManifestFileMeta>,
        storage: &Arc<DynObjectStore>,
        schema: &PaimonSchema,
    ) -> Vec<ManifestEntry> {
        // let serialized = serde_json::to_string(&r).unwrap();
        // println!("{}", serialized);
        join_all(file_meta.iter().map(|e| {
            let _ = &e.file_name;
            // let err_msg = format!("read {}", file_name.as_str());
            // TODO: Custom error
            e.manifest(storage, schema)
        }))
        .await
        .into_iter()
        .flat_map(|r| r.unwrap())
        .collect::<Vec<ManifestEntry>>()
    }
}

pub struct SnapshotManager {
    #[allow(dead_code)]
    table_path: ListingTableUrl,
    storage: Arc<DynObjectStore>,
}

#[allow(dead_code)]
impl SnapshotManager {
    pub(crate) fn new(url: ListingTableUrl, storage: Arc<DynObjectStore>) -> SnapshotManager {
        Self {
            table_path: url,
            storage,
        }
    }

    fn snapshot_path(&self, snapshot_id: i64) -> String {
        format!("/snapshot/{}{}", SNAPSHOT_PREFIX, snapshot_id)
    }

    fn tag_path(&self, tag_name: &str) -> String {
        format!("/tag/{}{}", TAG_PREFIX, tag_name)
    }

    fn consumer_path(&self, consumer_id: i64) -> String {
        format!("/consumer/{}{}", CONSUMER_PREFIX, consumer_id)
    }

    pub(crate) async fn snapshot(&self, snapshot_id: i64) -> Result<Snapshot, PaimonError> {
        let path = self.snapshot_path(snapshot_id);
        let content = read_to_string(&self.storage, &Path::from(path)).await?;
        let s: Snapshot = serde_json::from_str(content.as_str())?;
        Ok(s)
    }

    pub(crate) async fn tag(&self, tag_name: &str) -> Result<Snapshot, PaimonError> {
        let path = self.tag_path(tag_name);
        let content = read_to_string(&self.storage, &Path::from(path)).await?;
        let s: Snapshot = serde_json::from_str(content.as_str())?;
        Ok(s)
    }

    pub(crate) async fn consumer(&self, consumer_id: i64) -> Result<Snapshot, PaimonError> {
        let path = self.consumer_path(consumer_id);
        let content = read_to_string(&self.storage, &Path::from(path)).await?;
        let consumer: Consumer = Consumer::from_json(&content)?;
        let s: Snapshot = self.snapshot(consumer.next_snapshot).await?;
        Ok(s)
    }

    async fn latest_snapshot_id(&self) -> Option<i64> {
        let latest_path = format!("/snapshot/{}", LATEST);
        let id_string = read_to_string(&self.storage, &Path::from(latest_path))
            .await
            .unwrap();
        match id_string.parse::<i64>() {
            core::result::Result::Ok(id) => Some(id),
            Err(_) => None,
        }
    }

    pub(crate) async fn latest_snapshot(&self) -> Result<Snapshot, PaimonError> {
        if let Some(id) = self.latest_snapshot_id().await {
            self.snapshot(id).await
        } else {
            Err(PaimonError::Generic(
                "Not found latest snapshot id".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::datafusion::paimon::test_local_store;

    use super::*;

    pub(crate) async fn get_latest_metedata_file(
        storage: &Arc<DynObjectStore>,
    ) -> Result<Snapshot, PaimonError> {
        let latest_path = "/snapshot/LATEST".to_string();
        let latest_num = read_to_string(storage, &Path::from(latest_path)).await?;

        let latest_path = format!("/snapshot/snapshot-{}", latest_num);

        let content = read_to_string(storage, &Path::from(latest_path)).await?;
        let snapshot = serde_json::from_str(content.as_str())?;
        Ok(snapshot)
    }
    #[tokio::test]
    async fn read_snapshot() -> Result<(), PaimonError> {
        let (_url, storage) = test_local_store("ods_mysql_paimon_points_5");

        let json = r#"
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
            }
            "#;
        let expected: Snapshot = serde_json::from_str(json).unwrap();
        let actual = get_latest_metedata_file(&storage).await?;

        assert_eq!(expected, actual);
        assert_eq!(expected.version, actual.version);
        assert_eq!(expected.id, actual.id);
        assert_eq!(expected.schema_id, actual.schema_id);
        assert_eq!(expected.base_manifest_list, actual.base_manifest_list);
        assert_eq!(expected.delta_manifest_list, actual.delta_manifest_list);
        assert_eq!(
            expected.changelog_manifest_list,
            actual.changelog_manifest_list
        );
        assert_eq!(expected.commit_user, actual.commit_user);
        assert_eq!(expected.commit_identifier, actual.commit_identifier);
        assert_eq!(actual.commit_kind, actual.commit_kind);
        assert_eq!(expected.time_millis, actual.time_millis);
        assert_eq!(expected.log_offsets, actual.log_offsets);
        assert_eq!(expected.total_record_count, actual.total_record_count);
        assert_eq!(expected.delta_record_count, actual.delta_record_count);
        assert_eq!(
            expected.changelog_record_count,
            actual.changelog_record_count
        );
        assert_eq!(expected.watermark, actual.watermark);

        let actual = actual.get_schema(&storage).await?;
        let schema_str = r#"
            {
                "id" : 0,
                "fields" : [ {
                "id" : 0,
                "name" : "point_id",
                "type" : "STRING NOT NULL"
                }, {
                "id" : 1,
                "name" : "version",
                "type" : "INT NOT NULL"
                }, {
                "id" : 2,
                "name" : "version_info",
                "type" : "STRING"
                }, {
                "id" : 3,
                "name" : "address",
                "type" : "STRING"
                }, {
                "id" : 4,
                "name" : "lon",
                "type" : "STRING"
                }, {
                "id" : 5,
                "name" : "lat",
                "type" : "STRING"
                }, {
                "id" : 6,
                "name" : "operator",
                "type" : "STRING"
                }, {
                "id" : 7,
                "name" : "orders",
                "type" : "INT"
                }, {
                "id" : 8,
                "name" : "battery",
                "type" : "STRING"
                }, {
                "id" : 9,
                "name" : "ac_guns",
                "type" : "INT"
                }, {
                "id" : 10,
                "name" : "pre_gun_charge",
                "type" : "STRING"
                } ],
                "highestFieldId" : 10,
                "partitionKeys" : [ ],
                "primaryKeys" : [ "point_id" ],
                "options" : {
                "bucket" : "2",
                "auto-create" : "true",
                "path" : "oss://strategy-map/paimon/default.db/ods_mysql_paimon_points_5",
                "changelog-producer" : "input",
                "manifest.format" : "avro",
                "file.format" : "parquet",
                "type" : "paimon"
                }
            }
        "#;
        let expected: PaimonSchema = serde_json::from_str(schema_str).unwrap();
        assert_eq!(expected, actual);

        Ok(())
    }
}
