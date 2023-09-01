use std::sync::Arc;

use chrono::Local;
use datafusion::arrow::datatypes::{DataType, TimeUnit};

use crate::datafusion::paimon::error::PaimonError;
use nom::{bytes::complete::take_until, error::ErrorKind, IResult};
use object_store::DynObjectStore;

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
        "TINYINT" => DataType::UInt8,
        "SMALLINT" => DataType::UInt16,
        "INT" | "INTEGER" => DataType::UInt32,
        "BIGINT" => DataType::UInt64,
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

#[cfg(test)]
mod tests {
    use super::*;

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
