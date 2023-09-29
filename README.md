# Rust Paimon(实验)

基于[Arrow DataFusion](https://github.com/apache/arrow-datafusion)实现对数据湖[incubator-paimon](https://github.com/apache/incubator-paimon)读写

## Example

```sql
CREATE EXTERNAL TABLE ods_mysql_paimon_points_5 
STORED AS PAIMON 
OPTIONS ('scan.snapshot-id' '5') 
LOCATION 'E:\\rustProject\\paimon-rs\\test\\paimon/default.db\\ods_mysql_paimon_points_5'
```

## Features

- [x] 读元数据（`snapshot`、`schema`、`manifest`、`manifestList`）
- [x] 支持读`manifest`数据`avro`、`parquet`格式
- [x] 支持读data数据`avro`、`parquet`格式
- [x] 支持读`input`模式表数据
- [x] 支持批读
- [x] 批读实现**limit**(可优化)
- [x] 本地`paimon`表读取
- [x] 支持`hdfs`数据源(未测试)
- [x] 支持指定`tag`读取数据(未测试)
- [x] 支持指定`consumer-id`读取数据(未测试)

## Doing

- [ ] 集成`datafusion`(持续集成中)
- [ ] 兼容原始hash算法
  - [x] TINYINT
  - [x] SMALLINT
  - [x] INT
  - [x] BIGINT
  - [x] BOOLEAN
  - [x] DOUBLE
  - [x] FLOAT
  - [ ] DECIMAL
  - [ ] BINARY
  - [ ] DATE
  - [ ] TIME
  - [ ] TIMESTAMP

## RoadMap

- [ ] 支持`Appendonly`表读取数据
- [ ] 支持流读
- [ ] 支持写功能
- [ ] 集成`incubator-opendal`
