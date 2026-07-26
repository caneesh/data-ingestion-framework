-- Audit infrastructure tables. The framework auto-creates these when the
-- audit block is enabled; this DDL documents the schemas for external tools.

CREATE DATABASE IF NOT EXISTS ingest_audit;

CREATE TABLE IF NOT EXISTS ingest_audit.ingest_file_audit (
  run_id     STRING,
  entity     STRING,
  file_name  STRING,
  file_path  STRING,
  checksum   STRING,
  size_bytes BIGINT,
  status     STRING,   -- VALIDATED | QUARANTINED | SKIPPED_DUPLICATE | REJECTED_DUPLICATE | PROCESSED | FAILED
  reason     STRING,
  event_ts   TIMESTAMP
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ingest_audit.ingest_run_audit (
  run_id         STRING,
  entity         STRING,
  stage          STRING,   -- validate | raw | curated | publish
  status         STRING,   -- STARTED | SUCCESS | FAILED | SKIPPED
  source_count   BIGINT,
  raw_count      BIGINT,
  accepted_count BIGINT,
  rejected_count BIGINT,
  insert_count   BIGINT,
  update_count   BIGINT,
  delete_count   BIGINT,
  control_total  STRING,
  message        STRING,
  event_ts       TIMESTAMP
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ingest_audit.ingest_reconciliation (
  run_id     STRING,
  entity     STRING,
  check_name STRING,
  expected   STRING,
  actual     STRING,
  passed     BOOLEAN,
  event_ts   TIMESTAMP
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ingest_audit.ingest_file_registry (
  checksum     STRING,
  entity       STRING,
  file_name    STRING,
  file_path    STRING,
  size_bytes   BIGINT,
  run_id       STRING,
  status       STRING,   -- PROCESSED
  processed_ts TIMESTAMP
) STORED AS ORC;

CREATE TABLE IF NOT EXISTS ingest_audit.ingest_rejects (
  run_id          STRING,
  entity          STRING,
  file_id         STRING,
  source_file     STRING,
  row_idx         BIGINT,
  raw_record      STRING,
  error_code      STRING,
  error_message   STRING,
  reject_category STRING,
  reject_ts       TIMESTAMP
) STORED AS ORC;
