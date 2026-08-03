-- Batch control VIEW: one row per batch (batch_id = run_id) projected from
-- the append-only ingest_run_audit ledger. Read-only by design — the
-- ledger is the single source of truth; there is no physical batch table.
--
-- raw_status / curated_status = latest-terminal event per stage (display).
-- curated_done = EXISTS(curated SUCCESS) — the checkpoint truth used by
-- the decoupled pending driver; a later failed replay never flips it.
-- retry_count = count of FAILED events across all stages of the batch.
--
-- The BatchControl service (ingestion-core) computes the same projection
-- with pre-migration-ledger column guards; this view assumes the migrated
-- schema (run_mode/window columns present).

CREATE OR REPLACE VIEW ingest_audit.ingest_batch_control AS
WITH ev AS (
  SELECT *, CASE WHEN status = 'STARTED' THEN 0 ELSE 1 END AS terminal
  FROM ingest_audit.ingest_run_audit
)
SELECT
  run_id AS batch_id,
  entity,
  MIN(CASE WHEN stage = 'raw' AND status = 'SUCCESS'
        THEN named_struct('ts', event_ts, 'v', run_mode) END).v      AS run_mode,
  MIN(CASE WHEN stage = 'raw' AND status = 'SUCCESS'
        THEN named_struct('ts', event_ts, 'v', window_start) END).v  AS extract_window_start,
  MIN(CASE WHEN stage = 'raw' AND status = 'SUCCESS'
        THEN named_struct('ts', event_ts, 'v', window_end) END).v    AS extract_window_end,
  MAX(CASE WHEN stage = 'raw'
        THEN named_struct('ts', event_ts, 't', terminal, 'v', status) END).v     AS raw_status,
  MAX(CASE WHEN stage = 'curated'
        THEN named_struct('ts', event_ts, 't', terminal, 'v', status) END).v     AS curated_status,
  MAX(CASE WHEN stage = 'curated' AND status = 'SUCCESS' THEN TRUE ELSE FALSE END) AS curated_done,
  MAX(CASE WHEN stage = 'raw' THEN raw_count END)      AS raw_count,
  MAX(CASE WHEN stage = 'raw' THEN accepted_count END) AS accepted_count,
  MAX(CASE WHEN stage = 'raw' THEN rejected_count END) AS rejected_count,
  MAX(CASE WHEN stage = 'curated'
        THEN named_struct('ts', event_ts, 't', terminal, 'v', insert_count) END).v AS insert_count,
  MAX(CASE WHEN stage = 'curated'
        THEN named_struct('ts', event_ts, 't', terminal, 'v', update_count) END).v AS update_count,
  MAX(CASE WHEN stage = 'curated'
        THEN named_struct('ts', event_ts, 't', terminal, 'v', delete_count) END).v AS delete_count,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)   AS retry_count,
  MIN(event_ts) AS first_event_ts,
  MAX(event_ts) AS last_event_ts
FROM ev
GROUP BY run_id, entity;
