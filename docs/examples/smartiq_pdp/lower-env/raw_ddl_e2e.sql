-- ===========================================================================
-- SUBSTITUTE ${LOCATION} BEFORE RUNNING THIS FILE.
--
-- Hive does NOT always fail on an unresolved ${...}: it can create the
-- table with the placeholder as a LITERAL directory name, and the run then
-- dies at the first read with
--   [PATH_NOT_FOUND] Path does not exist: hdfs://.../${LOCATION}/<table>
--
-- Safest — substitute textually, so nothing depends on the client:
--   sed 's|${LOCATION}|hdfs://NAMESERVICE/path/to/base|g' THIS_FILE > run.sql
--   beeline -u '<jdbc-url>' -f run.sql
--
-- Or let the client do it:
--   beeline -u '<jdbc-url>' --hivevar LOCATION=hdfs://NAMESERVICE/base -f THIS_FILE
--
-- Verify afterwards — the Location row must contain no '$':
--   DESCRIBE FORMATTED <db>.<table>;
--
-- Pre-creating is OPTIONAL. The framework creates these tables itself when
-- they are absent, which sidesteps this entirely.
-- ===========================================================================
-- SmartIQ_PDP E2E RAW (lower environment) — 11 source columns + framework
-- metadata. All-string per the mapping contract; the framework writes the
-- typed values it reads from JDBC into these string columns unchanged.
-- ORC + EXTERNAL is deliberate: in Hive 3 a MANAGED ORC table is
-- created transactional (ACID) by DEFAULT, and Spark 3.5 cannot write
-- Hive ACID tables without the Hive Warehouse Connector. Dropping the
-- EXTERNAL keyword here would break the pipeline, not just change
-- ownership semantics.
CREATE EXTERNAL TABLE IF NOT EXISTS membership_common_raw.smartiq_pdp_e2e (
  `file_name` STRING COMMENT 'src: FileName | business key',
  `last_modified_datetime` STRING COMMENT 'src: LastModifiedDatetime | freshness + watermark (datetime)',
  `form_guid` STRING COMMENT 'src: FormGuid | column restored in v2',
  `user_email_id` STRING COMMENT 'src: UserEmailId | PII masking in rejects',
  `effective_date` STRING COMMENT 'src: EffectiveDate | date-typed source column',
  `group_number` STRING COMMENT 'src: GroupNumber | composite-key alternative (part 1)',
  `ai_groupand_ba_numbers_section_number` STRING COMMENT 'src: AIGroupandBANumbers.SectionNumber | composite-key alternative (part 2)',
  `ai_size_contract_count` STRING COMMENT 'src: AISize/ContractCount | source name with special chars -> alias',
  `funding_type` STRING COMMENT 'src: FundingType | plain varchar: trim/empty -> NULL',
  `mds_retail_max_day_supply` STRING COMMENT 'src: MDS.RetailMaximumDaySupply | collision pair (base)',
  `mds_retail_max_day_supply_incl_esn` STRING COMMENT 'src: MDSRetailMaximumDaySupply | collision pair (renamed *_incl_esn)',
  `source_file` STRING COMMENT 'framework metadata (RawMetadata)',
  `row_idx` BIGINT COMMENT 'framework metadata (RawMetadata)',
  `load_timestamp` TIMESTAMP COMMENT 'framework metadata (RawMetadata)',
  `file_type` STRING COMMENT 'framework metadata (RawMetadata)',
  `file_id` STRING COMMENT 'framework metadata (RawMetadata)',
  `run_id` STRING COMMENT 'framework metadata (RawMetadata)',
  `source_system` STRING COMMENT 'framework metadata (RawMetadata)',
  `source_database` STRING COMMENT 'framework metadata (RawMetadata)',
  `source_schema` STRING COMMENT 'framework metadata (RawMetadata)',
  `source_table` STRING COMMENT 'framework metadata (RawMetadata)',
  `extract_start_ts` STRING COMMENT 'framework metadata (RawMetadata)',
  `extract_end_ts` STRING COMMENT 'framework metadata (RawMetadata)',
  `record_hash` STRING COMMENT 'framework metadata (RawMetadata)',
  `source_modified_ts` STRING COMMENT 'framework metadata (RawMetadata)',
  `source_operation` STRING COMMENT 'framework metadata (RawMetadata)',
  `source_primary_key` STRING COMMENT 'framework metadata (RawMetadata)'
)
PARTITIONED BY (`ingest_dt` STRING)
STORED AS ORC
LOCATION '${LOCATION}/smartiq_pdp_e2e';
