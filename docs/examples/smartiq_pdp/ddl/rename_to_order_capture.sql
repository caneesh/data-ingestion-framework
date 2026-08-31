-- ============================================================================
-- APPLIES ONLY where tables ALREADY EXIST under the old names (the test
-- environment). A fresh environment — production promotion — is created
-- under the new names via raw_ddl.sql / curated_ddl.sql and skips this
-- file entirely: see PROD_PROMOTION.md.
--
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
-- These are EXTERNAL tables, so RENAME is metadata-only and SYMMETRIC:
-- every partition and file stays in place, the LOCATION keeps its old
-- directory name (harmless cosmetic drift — do NOT move data during this
-- window), and rollback is the reverse ALTER. Fresh deployments use the
-- updated raw_ddl.sql / curated_ddl.sql instead of this file.
--
-- DEPLOYMENT PROCEDURE
--
-- Phase A — prepare (before the window)
--   1. Stage the updated feed configs (commit 7e107ba or later) on the
--      edge node — NOT into $SMARTIQ_CONF_DIR yet. No jar rebuild: this
--      change is config-only; a new jar just adds an unrelated variable.
--   2. Announce to consumers: curated becomes order_capture_pdp_forms,
--      old name served by a view until <deprecation date> — pick it now.
--   3. Pick a window with no run in flight; hold the Control-M folders.
--
-- Phase B — rehearse in the lower environment
--   Run the E2E section below, deploy the e2e configs, then prove it:
--     run_smartiq.sh e2e INCR --run-id rename-check-1   (no-op is fine)
--     run_smartiq.sh e2e INCR --stage reconcile
--   Reconcile is the deliberate proof: it REQUIRES the curated table
--   (CFG_022) and compares keys — a half-worked rename cannot pass it.
--   An empty INCR alone does not prove the curated name resolves.
--
-- Phase C — production window
--   1. Confirm idle: last ledger row terminal, no YARN app, jobs held.
--   2. Record SHOW CREATE TABLE for both old tables (rollback reference).
--   3. Run the PRODUCTION section below (two ALTERs + the bridge view).
--   4. Deploy the staged configs to $SMARTIQ_CONF_DIR — SAME window; a
--      config naming a table that does not exist fails its next run.
--   5. Verify (queries at the bottom of this file), then the real proof:
--        run_smartiq.sh prod INCR --stage reconcile
--   6. Release the Control-M holds.
--
-- Phase D — after
--   Watch the next scheduled load; on the deprecation date, confirm no
--   consumer still hits the view, then run the DROP VIEW below.
--
-- ROLLBACK (any point before consumers migrate): reverse the ALTERs,
-- drop the view, restore the previous configs. Metadata-only both ways —
-- no data is at risk in this procedure.
-- ============================================================================

ALTER TABLE membership_common_raw.smartiq_pdp
  RENAME TO membership_common_raw.order_capture_smartiq_pdp;

ALTER TABLE membership_common_curated.smartiq_pdp
  RENAME TO membership_common_curated.order_capture_pdp_forms;

-- ----------------------------------------------------------------------------
-- CONSUMER BRIDGE — ONLY if consumers in THIS environment query the old
-- curated name. Test has none: SKIP this section there, so test mirrors
-- prod (which is created under the new names and never has the view).
-- Where it is created: announce a deprecation date, then drop it — an
-- alias kept forever is a second name to maintain, which is the drift
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
