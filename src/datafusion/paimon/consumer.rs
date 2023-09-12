use serde::{Deserialize, Serialize};

use super::error::PaimonError;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Consumer {
    #[serde(rename = "nextSnapshot")]
    pub next_snapshot: i64,
}

impl Consumer {
    pub fn from_json(json_str: &str) -> Result<Consumer, PaimonError> {
        let consumer: Consumer = serde_json::from_str(json_str)?;
        Ok(consumer)
    }
}
