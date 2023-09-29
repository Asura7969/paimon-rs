use arrow::datatypes::ToByteSlice;
use arrow::row::{RowConverter, SortField};
use arrow_array::{
    cast::*, downcast_primitive_array, ArrayRef, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, RecordBatch,
};
use arrow_schema::Field;
use bytes::{Bytes, BytesMut};
use std::hash::Hasher;
use std::sync::Arc;

use chrono::Local;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

use crate::datafusion::paimon::error::PaimonError;
use nom::{bytes::complete::take_until, error::ErrorKind, IResult};
use object_store::DynObjectStore;
use parquet::data_type::AsBytes;

// https://github.com/apache/incubator-paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/data/BinaryRow.java
#[allow(unused)]
const HEADER_BITS: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];

pub(crate) async fn read_to_string(
    storage: &Arc<DynObjectStore>,
    location: &object_store::path::Path,
) -> Result<String, PaimonError> {
    let bytes = storage.get(location).await.unwrap().bytes().await.unwrap();
    // TODO: 简化方法
    let content = String::from_utf8_lossy(bytes.split_at(bytes.len()).0);
    Ok(content.to_string())
}

pub(crate) fn time_zone() -> String {
    Local::now().format("%:z").to_string()
}

pub(crate) fn from(value: &str) -> (DataType, bool) {
    let nullable = !value.ends_with("NOT NULL");
    let (datatype_str, tuple2) = match extract_num(value) {
        core::result::Result::Ok((input, num)) => (input, Some(num)),
        core::result::Result::Err(err) => {
            if let nom::Err::Error(v) = err {
                (v.input, None)
            } else {
                panic!("parse filed error")
            }
        }
    };
    let tuple2 = tuple2.map_or((i32::MAX, None), |v| v);
    let data_type = match datatype_str {
        "STRING" | "VARCHAR" | "CHAR" | "TEXT" => DataType::Utf8,
        "TINYINT" => DataType::Int8,
        "SMALLINT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" | "REAL" => DataType::Float32,
        "DECIMAL" => DataType::Decimal256(tuple2.0 as u8, tuple2.1.map_or(i8::MAX, |v| v as i8)),
        "BOOLEAN" => DataType::Boolean,
        "DOUBLE" => DataType::Float64,
        "VARBINARY" | "BYTES" | "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIME" => DataType::Time64(get_time_unit(tuple2.0)),
        "TIMESTAMP" => {
            let unit = get_time_unit(tuple2.0);
            if value.contains("WITH LOCAL TIME ZONE") {
                DataType::Timestamp(unit, Some(time_zone().into()))
            } else {
                DataType::Timestamp(unit, None)
            }
        }
        data => panic!("Not support datatype: {}", data),
    };
    (data_type, nullable)
}

fn get_time_unit(v: i32) -> TimeUnit {
    match v {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        7..=9 => TimeUnit::Nanosecond,
        _ => panic!(""),
    }
}

/// input-1: STRING(10) NOT NULL -> (STRING, (10, None))
///
/// input-2: STRING(10) -> (STRING, (10, None))
///
/// input-3: STRING -> Err(STRING)
///
/// input-4: DECIMAL(1, 38) -> (DECIMAL, (1, Some(38)))
pub(crate) fn extract_num(input: &str) -> IResult<&str, (i32, Option<i32>)> {
    let input = match input.find(" NOT NULL") {
        Some(idx) => input.split_at(idx).0,
        _ => input,
    };

    if input.contains('(') {
        let split_index = input.find('(').expect("");
        let (datatype_str, fix_num) = input.split_at(split_index + 1);
        let (_, fix_num) = take_until(")")(fix_num)?;
        let sp = fix_num
            .split(',')
            .map(|s| {
                let s = s.trim().to_string();
                s.parse::<i32>()
                    .unwrap_or_else(|_| panic!("transform number error: {}", fix_num))
            })
            .collect::<Vec<i32>>();
        let tuple = match sp[..] {
            [a, b] => (a, Some(b)),
            [a] => (a, None),
            _ => panic!("paimon datatype has multiple qualifications"),
        };

        let datatype_str = datatype_str.as_bytes();
        let datatype_str = &datatype_str[0..datatype_str.len() - 1];
        Ok((std::str::from_utf8(datatype_str).unwrap(), tuple))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            ErrorKind::Fail,
        )))
    }
}

struct Murmur3Hasher {
    buf: BytesMut,
}

impl Default for Murmur3Hasher {
    fn default() -> Self {
        Self {
            buf: BytesMut::from_iter(&HEADER_BITS),
        }
    }
}

impl Hasher for Murmur3Hasher {
    fn finish(&self) -> u64 {
        let bytes = self.buf.clone().freeze();
        let hash = murmur3_32(&bytes, 42) as u64;
        // self.buf.clear();
        hash
    }

    fn write(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }
}

/// https://github.com/apache/incubator-paimon/blob/10540c75c99494df2dacc9589e6ed04877305ebc/paimon-common/src/main/java/org/apache/paimon/utils/MurmurHashUtils.java
#[allow(dead_code)]
pub(crate) fn murmur3_32(buf: &Bytes, seed: u32) -> u32 {
    fn pre_mix(buf: [u8; 4]) -> u32 {
        u32::from_le_bytes(buf)
            .wrapping_mul(0xcc9e2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b873593)
    }

    let mut hash = seed;

    let mut i = 0;
    while i < buf.len() / 4 {
        let buf = [buf[i * 4], buf[i * 4 + 1], buf[i * 4 + 2], buf[i * 4 + 3]];
        hash ^= pre_mix(buf);
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);

        i += 1;
    }

    match buf.len() % 4 {
        0 => {}
        1 => {
            hash ^= pre_mix([buf[i * 4], 0, 0, 0]);
        }
        2 => {
            hash ^= pre_mix([buf[i * 4], buf[i * 4 + 1], 0, 0]);
        }
        3 => {
            hash ^= pre_mix([buf[i * 4], buf[i * 4 + 1], buf[i * 4 + 2], 0]);
        }
        _ => { /* unreachable!() */ }
    }

    hash ^= buf.len() as u32;
    hash = hash ^ (hash.wrapping_shr(16));
    hash = hash.wrapping_mul(0x85ebca6b);
    hash = hash ^ (hash.wrapping_shr(13));
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash = hash ^ (hash.wrapping_shr(16));

    hash
}

#[allow(unused)]
fn batch_to_bucket(record_batch: RecordBatch) -> Result<Vec<u32>, PaimonError> {
    let schema = record_batch.schema();
    let fields = schema.fields.clone();
    let mut row_converter = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect(),
    )
    .unwrap();

    let rows = row_converter
        .convert_columns(record_batch.columns())
        .unwrap();

    let back = row_converter.convert_rows(&rows).unwrap();

    todo!()
}

#[allow(unused)]
pub fn restore_java_mem_struct(row: Vec<ArrayRef>, fields: &Vec<Field>) -> BytesMut {
    let mut bytes = BytesMut::from_iter(&HEADER_BITS);
    for (array, field_type) in row.iter().zip(fields) {
        downcast_primitive_array!(
            array => {
                match *field_type.data_type() {
                    DataType::Int8 => {
                        let v = array.value(0);
                        bytes.extend_from_slice(v.to_byte_slice());
                        bytes.extend(vec![0u8; 7]);
                    }
                    DataType::Int16 => {
                        let array: Int16Array = downcast_array(array);
                        let v: i16 = array.value(0);
                        let p = v.to_byte_slice();
                        bytes.extend_from_slice(p);
                        bytes.extend(vec![0u8; 6]);
                    }
                    DataType::Int32 => {
                        let array: Int32Array = downcast_array(array);
                        let v: i32 = array.value(0);
                        let p = v.to_byte_slice();
                        bytes.extend_from_slice(p);
                        bytes.extend(vec![0u8; 4]);
                    }
                    DataType::Int64 => {
                        let array: Int64Array = downcast_array(array);
                        let v: i64 = array.value(0);
                        let p = v.to_byte_slice();
                        bytes.extend_from_slice(p);
                    }
                    DataType::Float32 => {
                        let array: Float32Array = downcast_array(array);
                        let v: f32 = array.value(0);
                        let p = v.to_bits();
                        bytes.extend_from_slice(p.to_byte_slice());
                        bytes.extend(vec![0u8; 4]);
                    }
                    DataType::Float64 => {
                        let array: Float64Array = downcast_array(array);
                        let v: f64 = array.value(0);
                        let p = v.to_bits();
                        bytes.extend_from_slice(p.to_byte_slice());
                    }
                    _ => {}
                }
            }
            DataType::Boolean => {
                let v = as_boolean_array(array).value(0);
                bytes.extend_from_slice(v.as_bytes());
                bytes.extend(vec![0u8; 7]);

            }
            DataType::Utf8 => {
                let v = as_string_array(array).value(0);
                println!("{:?}", v);
            }
            t => println!("Unsupported datatype {}", t)
        )
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::row::{RowConverter, SortField};
    use arrow_array::ArrayRef;
    #[allow(unused_imports)]
    use arrow_array::{Array, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[allow(unused)]
    fn bucket(hash: i32, bucket_num: i32) -> i32 {
        (hash % bucket_num).abs()
    }

    #[test]
    fn hash_test() {
        let fields = vec![
            Field::new("a", DataType::Float64, true),
            // Field::new("b", DataType::Boolean, true),
            // Field::new("c", DataType::UInt32, true),
        ];
        let schema = Arc::new(Schema::new(fields.clone()));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![5.55])),
            // Arc::new(BooleanArray::from(vec![true, false])),
            // Arc::new(UInt32Array::from(vec![7])),
        ];

        let mut row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .unwrap();

        let rows = &mut row_converter.convert_columns(&columns).unwrap();
        println!("rows size: {}", rows.num_rows());

        let back = row_converter.convert_rows(&*rows).unwrap();

        println!("back size: {}", back.len());

        let bytes = restore_java_mem_struct(back, &fields);

        let buf = bytes.freeze();
        let b = bucket(murmur3_32(&buf, 42) as i32, 100);
        println!("bucket: {}", b);
    }

    #[test]
    fn extract_num_test() {
        let input = "STRING(10) NOT NULL";
        assert_eq!(extract_num(input), Ok(("STRING", (10, None))));

        let input = "STRING(20)";
        assert_eq!(extract_num(input), Ok(("STRING", (20, None))));

        let input = "STRING";
        assert_eq!(
            extract_num(input),
            Err(nom::Err::Error(nom::error::Error::new(
                input,
                ErrorKind::Fail,
            )))
        );
        let input = "DECIMAL(1, 38)";
        assert_eq!(extract_num(input), Ok(("DECIMAL", (1, Some(38)))));
    }
}
