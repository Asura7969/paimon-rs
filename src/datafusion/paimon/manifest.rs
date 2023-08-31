use chrono::{TimeZone, Utc};
use datafusion::datasource::listing::ListingTableUrl;
use serde::{Deserialize, Serialize};

use super::PartitionStat;
use object_store::path::Path;
use object_store::ObjectMeta;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ManifestEntry {
    #[serde(rename = "_KIND")]
    pub kind: i32,
    #[serde(rename = "_PARTITION", with = "serde_bytes")]
    pub partition: Vec<u8>,
    #[serde(rename = "_BUCKET")]
    pub bucket: i32,
    #[serde(rename = "_TOTAL_BUCKETS")]
    pub total_bucket: i32,
    #[serde(rename = "_FILE")]
    pub file: Option<DataFileMeta>,
}

impl ManifestEntry {
    pub fn to_object_meta(&self, _url: &ListingTableUrl) -> Option<ObjectMeta> {
        match &self.file {
            Some(file) => {
                let path = format!("/bucket-{}/{}", self.bucket, file.file_name);
                let creation_time = Utc.timestamp_opt(file.creation_time, 0).unwrap();
                let location = Path::from(path);
                Some(ObjectMeta {
                    location,
                    last_modified: creation_time,
                    size: file.file_size as usize,
                    e_tag: None,
                })
            }
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct DataFileMeta {
    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i64,
    #[serde(rename = "_ROW_COUNT")]
    pub row_count: i64,
    #[serde(rename = "_MIN_KEY", with = "serde_bytes")]
    pub min_key: Vec<u8>,
    #[serde(rename = "_MAX_KEY", with = "serde_bytes")]
    pub max_key: Vec<u8>,
    #[serde(rename = "_KEY_STATS")]
    pub key_stats: Option<PartitionStat>,
    #[serde(rename = "_VALUE_STATS")]
    pub value_stats: Option<PartitionStat>,
    #[serde(rename = "_MIN_SEQUENCE_NUMBER")]
    pub min_sequence_number: i64,
    #[serde(rename = "_MAX_SEQUENCE_NUMBER")]
    pub max_sequence_number: i64,
    #[serde(rename = "_SCHEMA_ID")]
    pub schema_id: i64,
    #[serde(rename = "_LEVEL")]
    pub level: i32,
    #[serde(rename = "_EXTRA_FILES")]
    pub extra_files: Vec<String>,
    #[serde(rename = "_CREATION_TIME")]
    pub creation_time: i64,
}

// impl Into<ObjectMeta> for DataFileMeta {
//     fn into(self) -> ObjectMeta {
//         let creation_time = Utc.timestamp_opt(self.creation_time, 0).unwrap();
//         let location = Path::from_filesystem_path(self.file_name).unwrap();
//         ObjectMeta {
//             location,
//             last_modified: creation_time,
//             size: self.file_size as usize,
//             e_tag: None,
//         }
//     }
// }
