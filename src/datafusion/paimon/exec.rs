use std::{any::Any, sync::Arc};

use ahash::{AHashMap, RandomState};
use arrow::compute::filter_record_batch;
use arrow_array::cast::downcast_array;
use arrow_array::{BooleanArray, Int8Array, RecordBatch, UInt64Array};
use arrow_select::concat::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::{
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    },
};
use datafusion_common::Result;
use datafusion_common::Statistics;
use itertools::Itertools;

use super::PaimonSchema;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MergeExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    paimon_schema: PaimonSchema,
    // merge_func: Fn<>
}

impl MergeExec {
    #[allow(dead_code)]
    pub fn new(paimon_schema: PaimonSchema, input: Arc<dyn ExecutionPlan>) -> Self {
        MergeExec {
            paimon_schema,
            input,
        }
    }
}

impl DisplayAs for MergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MergeExec")
            }
        }
    }
}
impl ExecutionPlan for MergeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.input.schema())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn output_partitioning(&self) -> Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Result::Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let captured_input = self.input.clone();
        let captured_schema = self.input.schema();

        Result::Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            futures::stream::once(merge_batch(
                captured_input,
                captured_schema,
                context,
                self.paimon_schema.clone(),
            )),
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        }
    }
}

async fn merge_batch(
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    context: Arc<TaskContext>,
    paimon_schema: PaimonSchema,
) -> Result<RecordBatch> {
    let records: Vec<RecordBatch> = collect(input, context.clone()).await?;
    let batch = concat_batches(&schema, records.iter())?;

    let pk = paimon_schema.primary_keys.clone();

    if pk.is_empty() {
        // Appendonly
        return Ok(batch);
    }
    // Changelog

    // let arr: Int8Array = downcast_array(batch.column(2).as_ref());
    // let filter = gt_scalar(&arr, 0i8)?;
    // let delete_or_update = filter_record_batch(&batch, &filter)?;
    // if delete_or_update.num_rows() == 0 {
    //     return Ok(batch);
    // }

    let map = hash_pk_for_batch(&batch, context, &pk)?;
    let count = batch.num_rows();

    let idx = map
        .into_values()
        .map(|(_, _, idx)| idx)
        .collect::<Vec<usize>>();
    let idx: Vec<_> = (0..count).map(|x| idx.contains(&x)).collect();
    let merge_filter: BooleanArray = BooleanArray::from(idx);

    let project_idx = delete_system_fields(pk.len() + 2, batch.schema().fields.len());
    let batch = filter_record_batch(&batch.project(&project_idx)?, &merge_filter)?;
    Ok(batch)
}

fn delete_system_fields(system_fields_len: usize, all_fields_count: usize) -> Vec<usize> {
    (0..all_fields_count)
        .filter(|i| *i >= system_fields_len)
        .collect_vec()
}

fn hash_pk_for_batch(
    batch: &RecordBatch,
    _ctx: Arc<TaskContext>,
    pks: &Vec<String>,
) -> Result<AHashMap<u64, (u64, i8, usize)>> {
    let pk_len = pks.len();

    let key = pks
        .iter()
        .enumerate()
        .map(|(idx, pk_name)| Column::new(pk_name.as_str(), idx))
        .collect::<Vec<_>>();
    let keys_values = key
        .iter()
        .map(|c| Ok(c.evaluate(batch)?.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let random_state = RandomState::with_seeds(0, 0, 0, 0);
    let mut batch_hashes = vec![0; batch.num_rows()];

    create_hashes(&keys_values, &random_state, &mut batch_hashes)?;

    let seq: UInt64Array = downcast_array(batch.column(pk_len));
    let kind: Int8Array = downcast_array(batch.column(pk_len + 1));

    let mut map: AHashMap<u64, (u64, i8, usize)> = AHashMap::new();
    for (idx, &hash) in batch_hashes.iter().enumerate() {
        let seq_v = seq.value(idx);
        let kind_v = kind.value(idx);

        // 0: APPEND
        // 1: UPDATE_BEFORE
        // 2: UPDATE_AFTER
        // 3: DELETE
        match kind_v {
            0..=2 => {
                map.insert(hash, (seq_v, kind_v, idx));
            }
            3 => {
                map.remove(&hash);
            }
            _ => panic!("unknown rowkind, maybe new kind"),
        };
    }

    Ok(map)
}
