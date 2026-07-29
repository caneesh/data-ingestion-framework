DROP TABLE IF EXISTS membership_common_raw.health_sherpa_member;

-- The framework metadata columns (source_file .. extract_end_ts) are
-- REQUIRED: the RAW sink refuses to silently drop them (raw.metadata_columns
-- defaults to FAIL) because run_id drives replay guards, reconciliation and
-- --resume. extract_start_ts/extract_end_ts hold the serialized incremental
-- window for JDBC feeds (strings so numeric/composite watermarks round-trip).
CREATE EXTERNAL TABLE membership_common_raw.health_sherpa_member (
  subscriber_id STRING,
  hios_id STRING,
  source_file STRING,
  row_idx BIGINT,
  load_timestamp TIMESTAMP,
  file_type STRING,
  file_id STRING,
  run_id STRING,
  source_system STRING,
  source_database STRING,
  source_schema STRING,
  source_table STRING,
  extract_start_ts STRING,
  extract_end_ts STRING
)
PARTITIONED BY (ingest_dt STRING)
STORED AS ORC
LOCATION 'hdfs:///test/incoming/membership/common/raw/health_sherpa_member'
TBLPROPERTIES ('orc.compress'='ZLIB');

DROP TABLE IF EXISTS membership_common_curated.health_sherpa_member;

CREATE EXTERNAL TABLE membership_common_curated.health_sherpa_member (
  subscriber_id STRING,
  hios_id STRING,
  create_timestamp TIMESTAMP,
  last_modified_ts TIMESTAMP,
  last_modified_op STRING
)
STORED AS ORC
LOCATION 'hdfs:///test/incoming/membership/common/curated/health_sherpa_member'
TBLPROPERTIES ('orc.compress'='ZLIB');
