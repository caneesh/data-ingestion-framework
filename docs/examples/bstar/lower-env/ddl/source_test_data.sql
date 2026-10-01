-- bstar E2E — synthetic SOURCE tables + scenario batches (Hive / beeline).
--
-- Run section 0 once, then ONE numbered section per pipeline run, checking
-- the expected result in ../LOWER_ENV_TEST_PLAN.md before moving on. Every
-- value here is INVENTED: no row is or resembles a real bstar record.
--
-- The tables mirror the real source's SHAPE — same columns, same types,
-- same partition scheme (inc_ful_flag / file_date / file_time) — stored as
-- ORC rather than the real feed's pipe-delimited text (the pipeline reads
-- through the metastore; the storage format is invisible to it).
--
-- Partitions are ADDED, never rewritten: that is the source owner's
-- guarantee, and the scenarios depend on it exactly as the pipeline does.
--
-- Database: bluestar_e2e. Change the five occurrences (or sed them) if
-- BSTAR_E2E_DB points elsewhere.

-- =====================================================================
-- 0. Create the source tables (run once)
-- =====================================================================
CREATE DATABASE IF NOT EXISTS bluestar_e2e;

DROP TABLE IF EXISTS bluestar_e2e.priv_addr_src;
CREATE TABLE bluestar_e2e.priv_addr_src (
  corp_ent_cd           CHAR(3),
  acct_grp_nbr          CHAR(9),
  sub_seq_nbr           INT,
  mem_nbr               SMALLINT,
  addr_seq_nbr          SMALLINT,
  addr_type_cd          CHAR(1),
  str_ln_1              VARCHAR(40),
  str_ln_2              VARCHAR(40),
  cty_nm                VARCHAR(25),
  cntry_nm              CHAR(25),
  st_cd                 CHAR(2),
  zip_cd                CHAR(9),
  cntry_cd              CHAR(3),
  phn_nbr               CHAR(10),
  addr_eff_dt           VARCHAR(30),
  addr_end_dt           VARCHAR(30),
  bad_addr_ind          CHAR(1),
  last_chg_usrid        CHAR(8),
  priv_addr_lcts        VARCHAR(30),
  cdc_src_last_updt_ts  VARCHAR(30),
  cdc_src_actn_cd       CHAR(2)
)
PARTITIONED BY (inc_ful_flag STRING, file_date STRING, file_time STRING)
STORED AS ORC;

-- =====================================================================
-- 1. Initial load (two partitions: a full F and an incremental I)
--    -> run the pipeline (run-id pa-1)
--    Covers: first light over several partitions, trim of CHAR padding in
--    the key, PT/UP merge, latest-per-key across partitions, record_hash
-- =====================================================================
INSERT INTO bluestar_e2e.priv_addr_src
  PARTITION (inc_ful_flag='F', file_date='2026-03-01', file_time='04.00.00') VALUES
  ('E01','GRP000001',1,1,1,'H','100 TEST ST',NULL,'TESTVILLE','USA','IL','600010000','USA','3125550100','2025-01-01','9999-12-31','N','TESTUSR1','2025-01-01 08:00:00.000000','2026-02-28 23:10:00.000001','PT'),
  ('E01','GRP000001',1,1,2,'M','PO BOX 1',NULL,'TESTVILLE','USA','IL','600010000','USA','3125550101','2025-01-01','9999-12-31','N','TESTUSR1','2025-01-01 08:00:00.000000','2026-02-28 23:10:00.000002','PT'),
  ('E01','GRP000002',1,1,1,'H','200 TEST AVE','APT 2','TESTBURG','USA','TX','750010000','USA','2145550200','2025-06-01','9999-12-31','N','TESTUSR2','2025-06-01 08:00:00.000000','2026-02-28 23:10:00.000003','PT');
INSERT INTO bluestar_e2e.priv_addr_src
  PARTITION (inc_ful_flag='I', file_date='2026-03-02', file_time='04.00.00') VALUES
  -- UP for an existing key: newer capture ts, so it must win over the F row
  ('E01','GRP000001',1,1,1,'H','101 TEST ST',NULL,'TESTVILLE','USA','IL','600010000','USA','3125550100','2025-01-01','9999-12-31','N','TESTUSR1','2026-03-01 09:00:00.000000','2026-03-01 23:10:00.000001','UP'),
  -- new key
  ('E01','GRP000003',2,1,1,'H','300 TEST BLVD',NULL,'TESTFORD','USA','NM','870010000','USA','5055550300','2026-03-01','9999-12-31','N','TESTUSR3','2026-03-01 09:00:00.000000','2026-03-01 23:10:00.000004','PT');
-- Expected after pa-1: curated has 4 keys; GRP000001/1/1/1 shows '101 TEST ST'
-- and last_modified_op = 'I' (first publish of every key); watermark
-- 2026-03-02|04.00.00 version 1.

-- =====================================================================
-- 2. Steady state: one new partition  -> run (pa-2)
--    Covers: only the new partition is read (log_partition_counts shows
--    it), UP -> last_modified_op 'U', watermark_continuity passes
-- =====================================================================
INSERT INTO bluestar_e2e.priv_addr_src
  PARTITION (inc_ful_flag='I', file_date='2026-03-03', file_time='04.00.00') VALUES
  ('E01','GRP000002',1,1,1,'H','200 TEST AVE','APT 3','TESTBURG','USA','TX','750010000','USA','2145550200','2025-06-01','9999-12-31','N','TESTUSR2','2026-03-02 09:00:00.000000','2026-03-02 23:10:00.000003','UP');
-- Expected: GRP000002 str_ln_2 = 'APT 3', op 'U'; 4 keys; watermark 2026-03-03|04.00.00 v2.

-- =====================================================================
-- 3. LATE partition inside the lookback  -> run (pa-3)
--    A partition for an OLDER file_date lands after the watermark has
--    passed it. lookback = { days = 2 } rewinds 2026-03-03 to 2026-03-01,
--    so this partition is read. Its capture ts is OLDER than the row
--    curated already holds, so freshness must REJECT the stale version.
-- =====================================================================
INSERT INTO bluestar_e2e.priv_addr_src
  PARTITION (inc_ful_flag='I', file_date='2026-03-02', file_time='10.00.00') VALUES
  -- stale version of GRP000001/1/1/1 (capture ts before the UP in section 1)
  ('E01','GRP000001',1,1,1,'H','100 TEST ST STALE',NULL,'TESTVILLE','USA','IL','600010000','USA','3125550100','2025-01-01','9999-12-31','N','TESTUSR1','2025-01-01 08:00:00.000000','2026-02-28 23:30:00.000001','UP'),
  -- genuinely new key delivered late
  ('E01','GRP000004',3,1,1,'H','400 TEST LN',NULL,'TESTHAM','USA','OH','430010000','USA','6145550400','2026-03-01','9999-12-31','N','TESTUSR4','2026-03-01 10:00:00.000000','2026-03-01 23:40:00.000005','PT');
-- Expected: GRP000001 STILL '101 TEST ST' (stale lost); GRP000004 inserted; 5
-- keys; no duplicate keys; the ledger window_start is the STORED watermark
-- (2026-03-03|04.00.00), not the rewound bound; watermark unchanged at
-- 2026-03-03|04.00.00 (the late partition sorts BELOW it) — version
-- unchanged too, since nothing above the watermark was read.

-- =====================================================================
-- 4. DL tombstone  -> run (pa-4)
--    Covers: deletes.mode = SOFT — the key stays, is_deleted = true,
--    last_modified_op = 'D'
-- =====================================================================
INSERT INTO bluestar_e2e.priv_addr_src
  PARTITION (inc_ful_flag='I', file_date='2026-03-04', file_time='04.00.00') VALUES
  ('E01','GRP000001',1,1,2,'M',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'TESTUSR1','2026-03-03 09:00:00.000000','2026-03-03 23:10:00.000002','DL');
-- Expected: GRP000001/1/1/2 is_deleted = true, op 'D'; still 5 rows;
-- consumers filtering is_deleted IS NOT TRUE see 4.

-- =====================================================================
-- 5. Null business key + PII masking  -> run (pa-5)
--    Covers: CUR_001 quarantine (the run SUCCEEDS; the row lands in
--    ingest_rejects with str_ln_1 / zip_cd / phn_nbr MASKED)
-- =====================================================================
INSERT INTO bluestar_e2e.priv_addr_src
  PARTITION (inc_ful_flag='I', file_date='2026-03-05', file_time='04.00.00') VALUES
  ('E01',NULL,9,1,1,'H','900 MASKED ST',NULL,'TESTVILLE','USA','IL','600019999','USA','3125550900','2026-03-04','9999-12-31','N','TESTUSR9','2026-03-04 09:00:00.000000','2026-03-04 23:10:00.000009','PT'),
  -- blank-padded key must still match: '  E01' is NOT the same key, but
  -- 'E01   ' IS (CHAR padding) — this row is an UP to GRP000003
  ('E01','GRP000003',2,1,1,'H','301 TEST BLVD',NULL,'TESTFORD','USA','NM','870010000','USA','5055550300','2026-03-01','9999-12-31','N','TESTUSR3','2026-03-04 09:00:00.000000','2026-03-04 23:10:00.000004','UP');
-- Expected: 1 reject (CUR_001, payload masked), GRP000003 updated, 5 keys.

-- =====================================================================
-- 6. Empty window  -> run (pa-6)
--    Covers: no new partition — zero rows, no publish, watermark version
--    unchanged, run SUCCESS. (intentionally no DML)
-- =====================================================================

-- =====================================================================
-- 7. VERY late partition, OUTSIDE the lookback  -> run (pa-7), then
--    run --stage reconcile
--    file_date 2026-03-01 is more than 2 days behind the 2026-03-05
--    watermark, so pa-7 does NOT read it. --stage reconcile reports the
--    key as source_keys_present_in_curated FAILED (REPORT mode: exit 0,
--    row in ingest_reconciliation). Recovery: runbook §2.3 rewind.
-- =====================================================================
INSERT INTO bluestar_e2e.priv_addr_src
  PARTITION (inc_ful_flag='I', file_date='2026-03-01', file_time='23.00.00') VALUES
  ('E01','GRP000005',4,1,1,'H','500 VERY LATE RD',NULL,'TESTON','USA','WA','980010000','USA','2065550500','2026-02-28','9999-12-31','N','TESTUSR5','2026-02-28 20:00:00.000000','2026-02-28 23:50:00.000006','PT');

-- =====================================================================
-- 8. Replay and the purged-window failure (no DML; operator actions)
--    a. --stage curated --run-id pa-2 : re-reads pa-2's window from the
--       source; curated unchanged (the replayed rows are older than what
--       curated holds); watermark untouched.
--    b. ALTER TABLE bluestar_e2e.priv_addr_src DROP PARTITION
--         (inc_ful_flag='I', file_date='2026-03-03', file_time='04.00.00');
--       then --stage curated --run-id pa-2 again: HIVE_008, exit 20
--       (DATA_INTEGRITY), ledger curated row FAILED, curated unchanged.
--       Re-add the partition afterwards (re-run the section 2 INSERT) if
--       you want to continue testing from a complete source.
-- =====================================================================


-- =====================================================================
-- S. SUB_PREM_DET — a second entity on the same control tables
--    Create once, load section S1, run the sub_prem_det e2e feed (sp-1).
--    Covers: 7-column composite key, DECIMAL columns, two entities sharing
--    ingest_run_audit / ingest_watermarks / ingest_run_locks.
-- =====================================================================
DROP TABLE IF EXISTS bluestar_e2e.sub_prem_det_src;
CREATE TABLE bluestar_e2e.sub_prem_det_src (
  corp_ent_cd           CHAR(3),
  acct_nbr              CHAR(6),
  ben_agmt_nbr          SMALLINT,
  sub_seq_nbr           INT,
  sub_prm_hst_sq_nbr    SMALLINT,
  sub_prm_eff_sq_nbr    SMALLINT,
  sub_prm_det_sq_nbr    SMALLINT,
  corp_tier_nbr         SMALLINT,
  tier_agrgt_seq_nbr    SMALLINT,
  mem_nbr               SMALLINT,
  prem_amt              DECIMAL(13,2),
  rate_struc_typ        CHAR(1),
  split_fee_ind         CHAR(1),
  bill_tier_abbr        CHAR(6),
  covd_mem_nbr          SMALLINT,
  cvg_abbr_cd           CHAR(4),
  ba_cvg_seq_nbr        SMALLINT,
  cms_ins_mem_cd        CHAR(3),
  dob                   VARCHAR(30),
  gndr_cd               CHAR(1),
  tobac_ind             CHAR(1),
  tot_prr_fee           DECIMAL(11,2),
  tot_prr_day_nbr       SMALLINT,
  last_chg_usrid        CHAR(8),
  sub_prem_det_lcts     VARCHAR(30),
  cdc_src_last_updt_ts  VARCHAR(30),
  cdc_src_actn_cd       CHAR(2)
)
PARTITIONED BY (inc_ful_flag STRING, file_date STRING, file_time STRING)
STORED AS ORC;

-- S1. Two rows that share every key column but the LAST (sub_prm_det_sq_nbr):
--     a six-column key would wrongly collapse them. -> run (sp-1)
INSERT INTO bluestar_e2e.sub_prem_det_src
  PARTITION (inc_ful_flag='F', file_date='2026-03-01', file_time='05.00.00') VALUES
  ('E01','ACC001',1,1,1,1,1,1,1,NULL,123.45,'T','N','TIER1',1,'MED',1,'ABC','1990-01-01','F','N',0.00,0,'TESTUSR1','2026-02-28 08:00:00.000000','2026-02-28 23:20:00.000001','PT'),
  ('E01','ACC001',1,1,1,1,2,1,1,NULL,67.89,'T','N','TIER1',1,'DEN',2,'ABC','1990-01-01','F','N',0.00,0,'TESTUSR1','2026-02-28 08:00:00.000000','2026-02-28 23:20:00.000002','PT');
-- Expected: 2 curated rows, prem_amt 123.45 / 67.89 as DECIMAL(13,2).

-- S2. UP on one of them + a DL on the other -> run (sp-2)
INSERT INTO bluestar_e2e.sub_prem_det_src
  PARTITION (inc_ful_flag='I', file_date='2026-03-02', file_time='05.00.00') VALUES
  ('E01','ACC001',1,1,1,1,1,1,1,NULL,130.00,'T','N','TIER1',1,'MED',1,'ABC','1990-01-01','F','N',0.00,0,'TESTUSR1','2026-03-01 08:00:00.000000','2026-03-01 23:20:00.000001','UP'),
  ('E01','ACC001',1,1,1,1,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'TESTUSR1','2026-03-01 08:00:00.000000','2026-03-01 23:20:00.000002','DL');
-- Expected: det 1 prem_amt 130.00 op 'U'; det 2 is_deleted = true op 'D'.
