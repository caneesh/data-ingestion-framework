-- ============================================================================
-- One-time migration (decided 2026-08-30): rename the SmartIQ_PDP tables.
--
--   raw:      smartiq_pdp      -> order_capture_smartiq_pdp
--   curated:  smartiq_pdp      -> order_capture_pdp_forms
--
-- WHY THESE NAMES. PDP = Prescription Drug Plan (confirmed): ~130 of the
-- 364 columns describe drug benefit design, so "pdp" is BUSINESS content
-- and stays in the curated name. "smartiq" is PLUMBING — the form tool that
-- happens to capture the data today — so it stays only in RAW, whose
-- identity genuinely is "mirror of dbo.SmartIQ_PDP". If the form store is
-- ever replaced, curated keeps its truthful name; raw correctly becomes a
-- new table for a new feed.
--
-- WHAT DOES NOT CHANGE. The entity ('smartiq_pdp') keys the watermark,
-- ledger history, locks and RAW lineage — renaming it would orphan all of
-- them. Table names are config only (raw.table / curated.table). Control
-- tables, databases and HDFS paths are untouched.
--
-- RUN THIS while no run is in flight (check the Control-M jobs are idle;
-- the entity lock does not guard DDL). These are EXTERNAL tables, so
-- RENAME is metadata-only and every partition and file stays in place —
-- the LOCATION keeps its old directory name, which is harmless. Fresh
-- deployments use the updated raw_ddl.sql / curated_ddl.sql instead.
-- ============================================================================

ALTER TABLE membership_common_raw.smartiq_pdp
  RENAME TO membership_common_raw.order_capture_smartiq_pdp;

ALTER TABLE membership_common_curated.smartiq_pdp
  RENAME TO membership_common_curated.order_capture_pdp_forms;

-- ----------------------------------------------------------------------------
-- CONSUMER BRIDGE: existing queries against the old curated name keep
-- working through a view. Announce a deprecation date, then drop it —
-- an alias kept forever is a second name to maintain, which is the drift
-- this rename exists to remove.
-- ----------------------------------------------------------------------------
CREATE VIEW membership_common_curated.smartiq_pdp AS
  SELECT * FROM membership_common_curated.order_capture_pdp_forms;

-- After every consumer has migrated:
-- DROP VIEW membership_common_curated.smartiq_pdp;

-- ----------------------------------------------------------------------------
-- Lower environment (no consumers, no bridge view needed):
-- ----------------------------------------------------------------------------
ALTER TABLE membership_common_raw.smartiq_pdp_e2e
  RENAME TO membership_common_raw.order_capture_smartiq_pdp_e2e;

ALTER TABLE membership_common_curated.smartiq_pdp_e2e
  RENAME TO membership_common_curated.order_capture_pdp_forms_e2e;

-- ----------------------------------------------------------------------------
-- Verify (both should list the new names; the view answers for the old one):
--   SHOW TABLES IN membership_common_raw     LIKE 'order_capture_*';
--   SHOW TABLES IN membership_common_curated LIKE 'order_capture_*';
--   SELECT COUNT(*) FROM membership_common_curated.smartiq_pdp;  -- via view
-- Then deploy the updated feed configs IN THE SAME change window: a config
-- pointing at the old name after the rename fails its next run at the
-- table-existence check, which is loud but avoidable.
-- ----------------------------------------------------------------------------
