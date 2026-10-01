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
--   DESCRIBE FORMATTED bluestar_current.<table>;
--
-- Pre-creating is OPTIONAL. The framework creates these tables itself when
-- they are absent, which sidesteps this entirely.
-- ===========================================================================
-- BSTAR CURATED (v1, 2026-09-30) — framework-compatible, latest-per-key.
--
-- SOURCE: bstar_raw.priv_addr and bstar_raw.sub_prem_det — EXTERNAL
-- pipe-delimited text, partitioned inc_ful_flag / file_date / file_time.
-- Table names are taken from the HDFS locations (.../BSTAR/PRIV_ADDR,
-- .../BSTAR/SUB_PREM_DET); confirm against SHOW TABLES IN bstar_raw.
--
-- THIS IS A CDC FEED. Two source columns are not business data:
--   cdc_src_actn_cd      CHAR(2)  — I/U/D action, BLANK-PADDED ('D ').
--                                   Drives deletes.mode = SOFT: a 'D' row
--                                   merges as a tombstone (freshness still
--                                   decides) and the framework stamps
--                                   last_modified_op = 'D', is_deleted = true.
--   cdc_src_last_updt_ts VARCHAR(30) — CDC capture timestamp. The FRESHNESS
--                                   column: newer capture wins per key,
--                                   regardless of which partition delivered
--                                   it. The partition tuple is the tie-break.
-- Both are carried in curated as audit columns (last action seen, last
-- capture time) — consumers filter is_deleted IS NOT TRUE.
--
-- TYPES. Every CHAR(n)/VARCHAR(n) becomes STRING, as in every curated table
-- in this repo: Hive CHAR is blank-padded (keys like acct_grp_nbr CHAR(9)
-- arrive padded, and the contract trims them), and Spark 3.1+ enforces
-- CHAR/VARCHAR length on write — a STRING target cannot fail that way.
-- SMALLINT / INT / DECIMAL(p,s) are kept exactly. The VARCHAR(30) timestamp
-- and date columns stay STRING; the feed declares
-- freshness.compare_as = timestamp (or compare_format once a sample value
-- has been seen) so comparisons are chronological and an unparseable value
-- fails CUR_008 instead of silently losing every contest.
--
-- UNPARTITIONED, deliberately: a latest-per-key table moves rows between
-- partitions on every update, and CUR_006 rejects full-replace publishes
-- into partitioned Hive-format tables. corp_ent_cd is the only stable
-- candidate if consumer reads ever need pruning; revisit then, not now.
--
-- BUSINESS KEYS are PROPOSED from the column names and MUST be confirmed
-- against the source system's primary key before first light.
-- publish.enforce_unique_keys (default on) fails the first publish if a
-- key is too COARSE; a key that is too FINE fails nothing — updates simply
-- look like inserts and the table grows. Confirm, do not infer.
--
-- FRAMEWORK AUDIT COLUMNS (last six) are stamped by CuratedService; without
-- record_hash in the TARGET the no-change skip never operates. No foreign
-- audit columns (curated_load_ts, src_run_date, ...): the framework never
-- populates them and they would stay NULL forever.
--
-- PRIVACY: street lines, zip, phone (priv_addr) and dob (sub_prem_det) are
-- PII. Tag them sensitivity = PII in the schema contract so reject payloads
-- mask them; the curated TABLES hold them in the clear — restrict access.
--
-- ORC + EXTERNAL is deliberate: in Hive 3 a MANAGED ORC table is created
-- transactional (ACID) by DEFAULT, and Spark 3.5 cannot write Hive ACID
-- tables without the Hive Warehouse Connector. Dropping the EXTERNAL keyword
-- here would break the pipeline, not just change ownership semantics.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- PRIV_ADDR — member private address, latest per address sequence.
-- Proposed key: corp_ent_cd, acct_grp_nbr, sub_seq_nbr, mem_nbr, addr_seq_nbr
-- (addr_type_cd may belong in the key instead of / as well as addr_seq_nbr).
-- Source: 227 partitions, 144 files, ~77 KB — trivial volume.
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS bluestar_current.priv_addr (
  `corp_ent_cd`           STRING        COMMENT 'src: char(3)       | KEY (proposed)',
  `acct_grp_nbr`          STRING        COMMENT 'src: char(9)       | KEY (proposed)',
  `sub_seq_nbr`           INT           COMMENT 'src: int           | KEY (proposed)',
  `mem_nbr`               SMALLINT      COMMENT 'src: smallint      | KEY (proposed)',
  `addr_seq_nbr`          SMALLINT      COMMENT 'src: smallint      | KEY (proposed)',
  `addr_type_cd`          STRING        COMMENT 'src: char(1)',
  `str_ln_1`              STRING        COMMENT 'src: varchar(40)   | PII',
  `str_ln_2`              STRING        COMMENT 'src: varchar(40)   | PII',
  `cty_nm`                STRING        COMMENT 'src: varchar(25)',
  `cntry_nm`              STRING        COMMENT 'src: char(25)',
  `st_cd`                 STRING        COMMENT 'src: char(2)',
  `zip_cd`                STRING        COMMENT 'src: char(9)       | PII',
  `cntry_cd`              STRING        COMMENT 'src: char(3)',
  `phn_nbr`               STRING        COMMENT 'src: char(10)      | PII',
  `addr_eff_dt`           STRING        COMMENT 'src: varchar(30)',
  `addr_end_dt`           STRING        COMMENT 'src: varchar(30)',
  `bad_addr_ind`          STRING        COMMENT 'src: char(1)',
  `last_chg_usrid`        STRING        COMMENT 'src: char(8)       | source audit: last change user',
  `priv_addr_lcts`        STRING        COMMENT 'src: varchar(30)   | source audit: last change ts; freshness tie-break',
  `cdc_src_last_updt_ts`  STRING        COMMENT 'src: varchar(30)   | CDC capture ts — FRESHNESS column',
  `cdc_src_actn_cd`       STRING        COMMENT 'src: char(2)       | CDC action I/U/D (padded) — SOFT delete indicator; last action seen',
  `inc_ful_flag`          STRING        COMMENT 'source partition   | audit: I=incremental F=full; contract category=audit',
  `file_date`             STRING        COMMENT 'source partition   | audit: yyyy-MM-dd of the delivering partition',
  `file_time`             STRING        COMMENT 'source partition   | audit: HH.mm.ss of the delivering partition',
  `record_hash`           STRING        COMMENT 'business-content hash: no-change skip + same-hash version advance',
  `create_timestamp`      TIMESTAMP     COMMENT 'framework audit: first publish of this key (preserved on update)',
  `last_modified_ts`      TIMESTAMP     COMMENT 'framework audit: last publishing run',
  `last_modified_op`      STRING        COMMENT 'framework audit: I/U/D',
  `last_modified_run_id`  STRING        COMMENT 'framework audit: run_id of the last publishing run',
  `is_deleted`            BOOLEAN       COMMENT 'framework audit: true when the last source action was a CDC delete (deletes.mode = SOFT)'
)
STORED AS ORC
LOCATION '${LOCATION}/priv_addr';

-- ---------------------------------------------------------------------------
-- SUB_PREM_DET — subscriber premium detail, latest per detail sequence.
-- Proposed key: corp_ent_cd, acct_nbr, ben_agmt_nbr, sub_seq_nbr,
--               sub_prm_hst_sq_nbr, sub_prm_eff_sq_nbr, sub_prm_det_sq_nbr
-- (mem_nbr, corp_tier_nbr, tier_agrgt_seq_nbr may also belong in the key —
-- the source PK decides).
-- Source: 637 partitions, 414 files, ~30.9 GB UNCOMPRESSED TEXT — first
-- light is a real Spark job; parse cost, not scan cost, dominates.
-- ---------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS bluestar_current.sub_prem_det (
  `corp_ent_cd`           STRING        COMMENT 'src: char(3)       | KEY (proposed)',
  `acct_nbr`              STRING        COMMENT 'src: char(6)       | KEY (proposed)',
  `ben_agmt_nbr`          SMALLINT      COMMENT 'src: smallint      | KEY (proposed)',
  `sub_seq_nbr`           INT           COMMENT 'src: int           | KEY (proposed)',
  `sub_prm_hst_sq_nbr`    SMALLINT      COMMENT 'src: smallint      | KEY (proposed)',
  `sub_prm_eff_sq_nbr`    SMALLINT      COMMENT 'src: smallint      | KEY (proposed)',
  `sub_prm_det_sq_nbr`    SMALLINT      COMMENT 'src: smallint      | KEY (proposed)',
  `corp_tier_nbr`         SMALLINT      COMMENT 'src: smallint      | key candidate — confirm',
  `tier_agrgt_seq_nbr`    SMALLINT      COMMENT 'src: smallint      | key candidate — confirm',
  `mem_nbr`               SMALLINT      COMMENT 'src: smallint      | key candidate — confirm',
  `prem_amt`              DECIMAL(13,2) COMMENT 'src: decimal(13,2)',
  `rate_struc_typ`        STRING        COMMENT 'src: char(1)',
  `split_fee_ind`         STRING        COMMENT 'src: char(1)',
  `bill_tier_abbr`        STRING        COMMENT 'src: char(6)',
  `covd_mem_nbr`          SMALLINT      COMMENT 'src: smallint',
  `cvg_abbr_cd`           STRING        COMMENT 'src: char(4)',
  `ba_cvg_seq_nbr`        SMALLINT      COMMENT 'src: smallint',
  `cms_ins_mem_cd`        STRING        COMMENT 'src: char(3)',
  `dob`                   STRING        COMMENT 'src: varchar(30)   | PII',
  `gndr_cd`               STRING        COMMENT 'src: char(1)',
  `tobac_ind`             STRING        COMMENT 'src: char(1)',
  `tot_prr_fee`           DECIMAL(11,2) COMMENT 'src: decimal(11,2)',
  `tot_prr_day_nbr`       SMALLINT      COMMENT 'src: smallint',
  `last_chg_usrid`        STRING        COMMENT 'src: char(8)       | source audit: last change user',
  `sub_prem_det_lcts`     STRING        COMMENT 'src: varchar(30)   | source audit: last change ts; freshness tie-break',
  `cdc_src_last_updt_ts`  STRING        COMMENT 'src: varchar(30)   | CDC capture ts — FRESHNESS column',
  `cdc_src_actn_cd`       STRING        COMMENT 'src: char(2)       | CDC action I/U/D (padded) — SOFT delete indicator; last action seen',
  `inc_ful_flag`          STRING        COMMENT 'source partition   | audit: I=incremental F=full; contract category=audit',
  `file_date`             STRING        COMMENT 'source partition   | audit: yyyy-MM-dd of the delivering partition',
  `file_time`             STRING        COMMENT 'source partition   | audit: HH.mm.ss of the delivering partition',
  `record_hash`           STRING        COMMENT 'business-content hash: no-change skip + same-hash version advance',
  `create_timestamp`      TIMESTAMP     COMMENT 'framework audit: first publish of this key (preserved on update)',
  `last_modified_ts`      TIMESTAMP     COMMENT 'framework audit: last publishing run',
  `last_modified_op`      STRING        COMMENT 'framework audit: I/U/D',
  `last_modified_run_id`  STRING        COMMENT 'framework audit: run_id of the last publishing run',
  `is_deleted`            BOOLEAN       COMMENT 'framework audit: true when the last source action was a CDC delete (deletes.mode = SOFT)'
)
STORED AS ORC
LOCATION '${LOCATION}/sub_prem_det';
