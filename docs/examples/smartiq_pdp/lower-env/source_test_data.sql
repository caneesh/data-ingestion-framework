-- SmartIQ_PDP E2E — SQL Server source table + scenario batches.
-- Run section 0 once, then ONE numbered section per pipeline run, checking
-- the expected result in LOWER_ENV_TEST_PLAN.md before moving on. The
-- batches are ordered so each one exercises a guarantee the previous run
-- established.
-- Column names mirror the real source exactly, including the '.' and '/'
-- characters that require bracket quoting.

-- =====================================================================
-- 0. Create the test source table (run once)
-- =====================================================================
IF OBJECT_ID('dbo.SmartIQ_PDP_E2E', 'U') IS NOT NULL DROP TABLE dbo.SmartIQ_PDP_E2E;
CREATE TABLE dbo.SmartIQ_PDP_E2E (
  FileName                              varchar(500)  NULL,
  LastModifiedDatetime                  datetime      NOT NULL,
  FormGuid                              varchar(100)  NULL,
  UserEmailId                           varchar(200)  NULL,
  EffectiveDate                         date          NULL,
  GroupNumber                           varchar(2000) NULL,
  [AIGroupandBANumbers.SectionNumber]   varchar(2000) NULL,
  [AISize/ContractCount]                varchar(2000) NULL,
  FundingType                           varchar(2000) NULL,
  [MDS.RetailMaximumDaySupply]          varchar(2000) NULL,
  MDSRetailMaximumDaySupply             varchar(2000) NULL
);

-- =====================================================================
-- 1. Initial load  -> run the pipeline (run-id e2e-1)
--    Covers: first publish, in-batch dedup, trim/empty -> NULL
-- =====================================================================
INSERT INTO dbo.SmartIQ_PDP_E2E VALUES
 ('F001.pdf','2026-03-01 10:00:00','g-001','alice@example.com','2026-01-01',
  'GRP100','SEC1','250','ASO','30','90'),
 ('F002.pdf','2026-03-01 10:05:00','g-002','bob@example.com','2026-01-01',
  'GRP200','SEC1','100','FI','30','90'),
 -- empty strings must land as NULL in curated (trim; empty -> NULL)
 ('F003.pdf','2026-03-01 10:10:00','g-003','carol@example.com','2026-02-01',
  'GRP300','','   ','ASO','',''),
 -- SAME business key twice in one batch: the 10:20 version must win
 ('F004.pdf','2026-03-01 10:15:00','g-004a','dave@example.com','2026-02-01',
  'GRP400','SEC2','50','FI','30','90'),
 ('F004.pdf','2026-03-01 10:20:00','g-004b','dave@example.com','2026-02-01',
  'GRP400','SEC2','75','FI','30','90');

-- =====================================================================
-- 2. Insert + update  -> run the pipeline (run-id e2e-2)
--    Covers: new key inserted, existing key updated by a NEWER version
-- =====================================================================
UPDATE dbo.SmartIQ_PDP_E2E
   SET LastModifiedDatetime = '2026-03-02 11:00:00', [AISize/ContractCount] = '999',
       FundingType = 'ASO-CHANGED'
 WHERE FileName = 'F001.pdf';
INSERT INTO dbo.SmartIQ_PDP_E2E VALUES
 ('F005.pdf','2026-03-02 11:05:00','g-005','erin@example.com','2026-03-01',
  'GRP500','SEC3','500','FI','60','180');

-- =====================================================================
-- 3. Same content, newer source version  -> run the pipeline (e2e-3)
--    Covers: the version-advance fix — business columns are untouched so
--    the record_hash is identical, but the source bumped its version.
--    Curated must ADVANCE last_modified_datetime without restamping the
--    row as a business update.
-- =====================================================================
UPDATE dbo.SmartIQ_PDP_E2E
   SET LastModifiedDatetime = '2026-03-03 12:00:00'
 WHERE FileName = 'F002.pdf';

-- =====================================================================
-- 4. Null business key  -> run the pipeline (e2e-4)
--    Covers: null-key quarantine (CUR_001) + PII masking in the reject
--    payload (UserEmailId is sensitivity = PII)
-- =====================================================================
INSERT INTO dbo.SmartIQ_PDP_E2E VALUES
 (NULL,'2026-03-04 13:00:00','g-006','frank@example.com','2026-03-01',
  'GRP600','SEC4','10','FI','30','90');

-- =====================================================================
-- 5. No source changes  -> run the pipeline (e2e-5)
--    Covers: empty increment is a clean no-op (nothing extracted, nothing
--    published, run still SUCCESS)
-- =====================================================================
-- (intentionally no DML)

-- =====================================================================
-- 6. OPTIONAL — composite-key variant
--    Switch curated.merge.keys to
--      ["group_number", "ai_groupand_ba_numbers_section_number"]
--    in the feed, point curated at a fresh table, and re-run from a clean
--    watermark. These two rows share GRP700 but differ in section, so a
--    prefix-only match would wrongly collapse them into one row.
-- =====================================================================
-- INSERT INTO dbo.SmartIQ_PDP_E2E VALUES
--  ('F007.pdf','2026-03-05 14:00:00','g-007','gina@example.com','2026-03-01',
--   'GRP700','SEC-A','10','FI','30','90'),
--  ('F008.pdf','2026-03-05 14:05:00','g-008','gina@example.com','2026-03-01',
--   'GRP700','SEC-B','20','FI','30','90');
