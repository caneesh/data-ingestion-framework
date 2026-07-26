DROP TABLE IF EXISTS membership_common_raw.health_sherpa_member;

CREATE EXTERNAL TABLE membership_common_raw.health_sherpa_member (
  subscriber_id STRING,
  hios_id STRING,
  source_file STRING,
  row_idx BIGINT,
  load_timestamp TIMESTAMP,
  file_type STRING,
  file_id STRING
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
