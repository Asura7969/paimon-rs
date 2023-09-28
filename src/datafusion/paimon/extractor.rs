use super::error::PaimonError;
use arrow_array::RecordBatch;

#[allow(dead_code)]
pub(crate) struct KeyAndBucketExtractor {
    record: Option<RecordBatch>,
    project_idx: Option<Vec<usize>>,
}

#[allow(dead_code)]
impl KeyAndBucketExtractor {
    pub(crate) fn new(indices: Option<Vec<usize>>) -> KeyAndBucketExtractor {
        Self {
            record: None,
            project_idx: indices,
        }
    }

    pub(crate) fn set_record(&mut self, record: RecordBatch) {
        self.record = Some(record);
    }

    pub(crate) fn bucket(&self) -> Result<i32, PaimonError> {
        match &self.record {
            Some(batch) => {
                if let Some(idx) = &self.project_idx {
                    let _o = batch.project(idx)?;

                    // let hash_result = murmur3_32(&mut Cursor::new("hello world"), 0);
                }
                todo!()
            }
            None => panic!("must set record first"),
        }
    }

    fn inner_bucket(&self, hash_code: i32, num_buckets: i32) -> i32 {
        (hash_code % num_buckets).abs()
    }
}


#[cfg(test)]
mod tests {

}
