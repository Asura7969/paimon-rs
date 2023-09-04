use std::sync::Arc;

use object_store::{path::Path, DynObjectStore};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{
    error::PaimonError, manifest::ManifestEntry, reader::manifest, PaimonSchema, PartitionStat,
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ManifestFileMeta {
    #[serde(rename = "_VERSION")]
    pub version: i32,
    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i64,
    #[serde(rename = "_NUM_ADDED_FILES")]
    pub num_added_files: i64,
    #[serde(rename = "_NUM_DELETED_FILES")]
    pub num_deleted_files: i64,
    #[serde(rename = "_PARTITION_STATS")]
    pub partition_stats: Option<PartitionStat>,
    #[serde(rename = "_SCHEMA_ID")]
    pub schema_id: i64,
}

impl ManifestFileMeta {
    pub async fn manifest(
        &self,
        storage: &Arc<DynObjectStore>,
        schema: &PaimonSchema,
    ) -> Result<Vec<ManifestEntry>, PaimonError> {
        let path = format!("/manifest/{}", self.file_name);
        manifest(storage, &Path::from(path), &schema.get_manifest_format()).await
    }
}

impl From<&Map<String, Value>> for ManifestFileMeta {
    fn from(map: &Map<String, Value>) -> Self {
        let serialized = serde_json::to_string(map).unwrap();
        let deserialized: ManifestFileMeta = serde_json::from_str(&serialized).unwrap();
        deserialized
    }
}
