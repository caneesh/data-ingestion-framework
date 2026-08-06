-- SmartIQ_PDP E2E CURATED (lower environment) — same 11 columns +
-- record_hash + framework audit columns. Unpartitioned: latest-per-key.
-- ORC + EXTERNAL is deliberate: in Hive 3 a MANAGED ORC table is
-- created transactional (ACID) by DEFAULT, and Spark 3.5 cannot write
-- Hive ACID tables without the Hive Warehouse Connector. Dropping the
-- EXTERNAL keyword here would break the pipeline, not just change
-- ownership semantics.
CREATE EXTERNAL TABLE IF NOT EXISTS membership_common_curated.smartiq_pdp_e2e (
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
  `record_hash` STRING COMMENT 'business-content hash: no-change skip + version advance',
  `create_timestamp` TIMESTAMP COMMENT 'framework audit: first publish of this key',
  `last_modified_ts` TIMESTAMP COMMENT 'framework audit: last publishing run',
  `last_modified_op` STRING COMMENT 'framework audit: I/U/D'
)
STORED AS ORC
LOCATION '${LOCATION}/smartiq_pdp_e2e';
