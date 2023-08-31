use arrow_schema::DataType;
use datafusion::{
    arrow::datatypes::{Field as AField, Schema, SchemaRef},
    datasource::listing::ListingTableUrl,
};
use object_store::{local::LocalFileSystem, DynObjectStore};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, path::Path, sync::Arc};

use self::{error::PaimonError, manifest_list::ManifestFileMeta, reader::FileFormat, utils::from};

pub mod error;
mod example;
mod exec;
mod manifest;
mod manifest_list;
mod reader;
pub mod snapshot;
pub mod table;
mod utils;

#[allow(dead_code)]
pub struct PrimaryKeys(pub Vec<String>);
#[allow(dead_code)]
pub struct PartitionKeys(pub Vec<String>);

#[allow(dead_code)]
pub enum WriteMode {
    Appendonly,
    Changelog,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum CommitKind {
    #[serde(rename = "APPEND")]
    Append,
    #[serde(rename = "OVERWRITE")]
    Overwrite,
    #[serde(rename = "COMPACT")]
    Compact,
}

// #[allow(dead_code)]
// fn get_manifest_list(
//     table_path: &str,
//     file_name: &str,
//     format: &FileFormat,
// ) -> Result<Vec<ManifestFileMeta>, PaimonError> {
//     let path = format!("{}/manifest/{}", table_path, file_name);
//     manifest_list(path.as_str(), format)
// }

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct PaimonSchema {
    pub id: u64,
    pub fields: Vec<Field>,
    #[serde(rename = "highestFieldId")]
    pub highest_field_id: u32,
    #[serde(rename = "partitionKeys")]
    pub partition_keys: Vec<String>,
    #[serde(rename = "primaryKeys")]
    pub primary_keys: Vec<String>,
    pub options: HashMap<String, String>,
}

impl PaimonSchema {
    #[allow(dead_code)]
    pub fn get_manifest_format(&self) -> FileFormat {
        match self.options.get("manifest.format") {
            Some(format) => FileFormat::from(format),
            None => FileFormat::Avro,
        }
    }

    #[allow(dead_code)]
    pub fn get_file_format(&self) -> FileFormat {
        match self.options.get("file.format") {
            Some(format) => FileFormat::from(format),
            None => FileFormat::Orc,
        }
    }

    pub fn arrow_schema(&mut self) -> Schema {
        self.fields.sort_by(|a, b| a.id.cmp(&b.id));
        let fields = self
            .fields
            .iter()
            .map(|field| field.to_arrow_field())
            .collect::<Vec<AField>>();
        Schema::new(fields)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Field {
    id: u64,
    name: String,
    #[serde(rename = "type")]
    field_type: String,
}

impl Field {
    fn to_arrow_field(&self) -> AField {
        let (datatype, nullable) = from(self.field_type.as_str());
        AField::new(self.name.clone(), datatype, nullable)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct PartitionStat {
    #[serde(rename = "_MIN_VALUES", with = "serde_bytes")]
    min_values: Vec<u8>,
    #[serde(rename = "_MAX_VALUES", with = "serde_bytes")]
    max_values: Vec<u8>,
    #[serde(rename = "_NULL_COUNTS")]
    null_counts: Option<Vec<i64>>,
}

pub(crate) fn add_system_fields(schema: Schema) -> Result<SchemaRef, PaimonError> {
    Schema::try_merge(vec![
        Schema::new(vec![
            AField::new("_KEY_point_id", DataType::Utf8, false),
            AField::new("_SEQUENCE_NUMBER", DataType::UInt64, false),
            AField::new("_VALUE_KIND", DataType::Int8, false),
        ]),
        schema,
    ])
    .map(SchemaRef::new)
    .map_err(PaimonError::ArrowError)
}

#[allow(dead_code)]
pub(crate) fn test_paimonm_table_path(table_name: &str) -> String {
    let mut config_path = env::current_dir().unwrap();
    config_path.push("test");
    config_path.push("paimon/default.db");
    config_path.push(table_name);

    config_path.display().to_string()
}

#[allow(dead_code)]
pub(crate) async fn test_local_store(root_path: &str) -> (ListingTableUrl, Arc<DynObjectStore>) {
    // let path = "ods_mysql_paimon_points_5/snapshot/snapshot-5";

    let path = test_paimonm_table_path(root_path);
    let url = ListingTableUrl::parse(path.as_str()).unwrap();
    let store = LocalFileSystem::new_with_prefix(Path::new(path.as_str())).unwrap();
    (url, Arc::new(store))
}
