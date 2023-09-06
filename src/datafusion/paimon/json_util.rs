/// copy from arrow_json, support binary parser
use arrow_array::cast::AsArray;
use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::display::ArrayFormatter;
use arrow_cast::display::FormatOptions;
use arrow_json::writer::array_to_json_array;
use arrow_json::JsonSerializable;
use arrow_schema::{ArrowError, DataType};
use serde_json::map::Map as JsonMap;
use serde_json::Value;
use std::iter;

macro_rules! set_column_by_array_type {
    ($cast_fn:ident, $col_name:ident, $rows:ident, $array:ident) => {
        let arr = $cast_fn($array);
        $rows
            .iter_mut()
            .zip(arr.iter())
            .for_each(|(row, maybe_value)| {
                if let Some(v) = maybe_value {
                    row.insert($col_name.to_string(), v.into());
                }
            });
    };
}

pub fn record_batches_to_json_rows(
    batches: &[&RecordBatch],
) -> Result<Vec<JsonMap<String, Value>>, ArrowError> {
    let mut rows: Vec<JsonMap<String, Value>> = iter::repeat(JsonMap::new())
        .take(batches.iter().map(|b| b.num_rows()).sum())
        .collect();

    if !rows.is_empty() {
        let schema = batches[0].schema();

        let mut base = 0;
        for batch in batches {
            let row_count = batch.num_rows();
            let row_slice = &mut rows[base..base + batch.num_rows()];
            for (j, col) in batch.columns().iter().enumerate() {
                let col_name = schema.field(j).name();
                set_column_for_json_rows(row_slice, col, col_name)?
            }
            base += row_count;
        }
    }

    Ok(rows)
}

pub fn as_binary_array(arr: &dyn Array) -> &BinaryArray {
    arr.as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Unable to downcast to StringArray")
}

fn set_column_for_json_rows(
    rows: &mut [JsonMap<String, Value>],
    array: &ArrayRef,
    col_name: &str,
) -> Result<(), ArrowError> {
    match array.data_type() {
        DataType::Int8 => {
            set_column_by_primitive_type::<Int8Type>(rows, array, col_name);
        }
        DataType::Int16 => {
            set_column_by_primitive_type::<Int16Type>(rows, array, col_name);
        }
        DataType::Int32 => {
            set_column_by_primitive_type::<Int32Type>(rows, array, col_name);
        }
        DataType::Int64 => {
            set_column_by_primitive_type::<Int64Type>(rows, array, col_name);
        }
        DataType::UInt8 => {
            set_column_by_primitive_type::<UInt8Type>(rows, array, col_name);
        }
        DataType::UInt16 => {
            set_column_by_primitive_type::<UInt16Type>(rows, array, col_name);
        }
        DataType::UInt32 => {
            set_column_by_primitive_type::<UInt32Type>(rows, array, col_name);
        }
        DataType::UInt64 => {
            set_column_by_primitive_type::<UInt64Type>(rows, array, col_name);
        }
        DataType::Float16 => {
            set_column_by_primitive_type::<Float16Type>(rows, array, col_name);
        }
        DataType::Float32 => {
            set_column_by_primitive_type::<Float32Type>(rows, array, col_name);
        }
        DataType::Float64 => {
            set_column_by_primitive_type::<Float64Type>(rows, array, col_name);
        }
        DataType::Null => {
            // when value is null, we simply skip setting the key
        }
        DataType::Boolean => {
            set_column_by_array_type!(as_boolean_array, col_name, rows, array);
        }
        DataType::Utf8 => {
            set_column_by_array_type!(as_string_array, col_name, rows, array);
        }
        DataType::LargeUtf8 => {
            set_column_by_array_type!(as_largestring_array, col_name, rows, array);
        }
        DataType::Binary => {
            set_column_by_array_type!(as_binary_array, col_name, rows, array);
        }
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            let options = FormatOptions::default();
            let formatter = ArrayFormatter::try_new(array.as_ref(), &options)?;
            let nulls = array.nulls();
            rows.iter_mut().enumerate().for_each(|(idx, row)| {
                if nulls.map(|x| x.is_valid(idx)).unwrap_or(true) {
                    row.insert(
                        col_name.to_string(),
                        formatter.value(idx).to_string().into(),
                    );
                }
            });
        }
        DataType::Struct(_) => {
            let inner_objs = struct_array_to_jsonmap_array(array.as_struct())?;
            rows.iter_mut().zip(inner_objs).for_each(|(row, obj)| {
                row.insert(col_name.to_string(), Value::Object(obj));
            });
        }
        DataType::List(_) => {
            let listarr = as_list_array(array);
            rows.iter_mut().zip(listarr.iter()).try_for_each(
                |(row, maybe_value)| -> Result<(), ArrowError> {
                    if let Some(v) = maybe_value {
                        row.insert(col_name.to_string(), Value::Array(array_to_json_array(&v)?));
                    }
                    Ok(())
                },
            )?;
        }
        DataType::LargeList(_) => {
            let listarr = as_large_list_array(array);
            rows.iter_mut().zip(listarr.iter()).try_for_each(
                |(row, maybe_value)| -> Result<(), ArrowError> {
                    if let Some(v) = maybe_value {
                        let val = array_to_json_array(&v)?;
                        row.insert(col_name.to_string(), Value::Array(val));
                    }
                    Ok(())
                },
            )?;
        }
        DataType::Dictionary(_, value_type) => {
            let hydrated = arrow_cast::cast::cast(&array, value_type)
                .expect("cannot cast dictionary to underlying values");
            set_column_for_json_rows(rows, &hydrated, col_name)?;
        }
        DataType::Map(_, _) => {
            let maparr = as_map_array(array);

            let keys = maparr.keys();
            let values = maparr.values();

            // Keys have to be strings to convert to json.
            if !matches!(keys.data_type(), DataType::Utf8) {
                return Err(ArrowError::JsonError(format!(
                    "data type {:?} not supported in nested map for json writer",
                    keys.data_type()
                )));
            }

            let keys = keys.as_string::<i32>();
            let values = array_to_json_array(values)?;

            let mut kv = keys.iter().zip(values);

            for (i, row) in rows.iter_mut().enumerate() {
                if maparr.is_null(i) {
                    row.insert(col_name.to_string(), serde_json::Value::Null);
                    continue;
                }

                let len = maparr.value_length(i) as usize;
                let mut obj = serde_json::Map::new();

                for (_, (k, v)) in (0..len).zip(&mut kv) {
                    obj.insert(k.expect("keys in a map should be non-null").to_string(), v);
                }

                row.insert(col_name.to_string(), serde_json::Value::Object(obj));
            }
        }
        _ => {
            return Err(ArrowError::JsonError(format!(
                "data type {:?} not supported in nested map for json writer",
                array.data_type()
            )))
        }
    }
    Ok(())
}

fn set_column_by_primitive_type<T>(
    rows: &mut [JsonMap<String, Value>],
    array: &ArrayRef,
    col_name: &str,
) where
    T: ArrowPrimitiveType,
    T::Native: JsonSerializable,
{
    let primitive_arr = array.as_primitive::<T>();

    rows.iter_mut()
        .zip(primitive_arr.iter())
        .for_each(|(row, maybe_value)| {
            // when value is null, we simply skip setting the key
            if let Some(j) = maybe_value.and_then(|v| v.into_json_value()) {
                row.insert(col_name.to_string(), j);
            }
        });
}

fn struct_array_to_jsonmap_array(
    array: &StructArray,
) -> Result<Vec<JsonMap<String, Value>>, ArrowError> {
    let inner_col_names = array.column_names();

    let mut inner_objs = iter::repeat(JsonMap::new())
        .take(array.len())
        .collect::<Vec<JsonMap<String, Value>>>();

    for (j, struct_col) in array.columns().iter().enumerate() {
        set_column_for_json_rows(&mut inner_objs, struct_col, inner_col_names[j])?
    }
    Ok(inner_objs)
}
