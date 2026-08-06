# SmartIQ_PDP — lower-environment end-to-end test plan

A slim stand-in for the 364-column production feed: **11 columns, its own
entity and tables, the same machinery**. A green run here means the
mechanisms the full feed depends on are working against the real SQL
Server, the real Hive metastore and the real cluster.

## Why these 11 columns

Not the first eleven — each one is carrying a specific mechanism:

| Column | Exercises |
|---|---|
| `file_name` | the business key of the merge |
| `last_modified_datetime` | freshness comparison **and** the extraction watermark (`datetime` in source) |
| `form_guid` | a column restored in mapping v2 (was dropped in v1) |
| `user_email_id` | PII tagging → masking in reject payloads |
| `effective_date` | the `date`-typed source column (typed, not string) |
| `group_number` + `ai_groupand_ba_numbers_section_number` | the composite-key alternative (scenario 8) |
| `ai_size_contract_count` | source name with `/` in it → alias mapping (`AISize/ContractCount`) |
| `funding_type` | plain varchar → `trim; empty → NULL` |
| `mds_retail_max_day_supply` + `..._incl_esn` | the workbook's name-collision pair and its rename |

Names, aliases, types, transforms and PII tags are copied verbatim from
`hive_raw_curated_mapping_v2_all_columns.xlsx` — this is a **subset of the
production contract, not a variant of it**.

## Isolation

The entity is `smartiq_pdp_e2e`, distinct from `smartiq_pdp`. Because the
framework keys watermarks, ledger history and entity locks by entity name,
this feed gets its own of each automatically. Tables are
`membership_common_raw.smartiq_pdp_e2e` and
`membership_common_curated.smartiq_pdp_e2e` — nothing is shared with
production except the databases themselves.

## Setup

1. **Source:** run section 0 of `source_test_data.sql` against the lower
   SQL Server to create `dbo.SmartIQ_PDP_E2E`.
2. **Hive:** run `raw_ddl_e2e.sql` and `curated_ddl_e2e.sql`, substituting
   `${LOCATION}`. (Optional — the framework creates the tables itself if
   absent; pre-creating validates that your DDL and the framework agree.)
3. **Config:** copy `feed-smartiq-pdp-e2e.conf` + `smartiq-pdp-e2e-schema.conf`
   side by side (the feed `include`s the schema by relative name), set
   `SQLHOST-LOWER` and export `SMARTIQ_DB_PASSWORD`.
4. **Dry run first:**
   `--entity smartiq_pdp_e2e --validate-only` — config, contract and
   connectivity, no reads or writes.

The feed has `executor_probe = true`: every run proves **executors** can
reach SQL Server before extraction begins, which is the half a firewall
test from the edge node cannot cover.

## Run command

```bash
spark-submit --class com.hcsc.generic.ingest.app.IngestMain \
  --name ingest-smartiq_pdp_e2e --master yarn --deploy-mode cluster \
  --files feed-smartiq-pdp-e2e.conf,smartiq-pdp-e2e-schema.conf \
  --driver-java-options "-Dconfig.file=feed-smartiq-pdp-e2e.conf" \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity smartiq_pdp_e2e --mode INCR --run-id e2e-1
```

Explicit `--run-id` values matter: scenario 7 replays one by id. Use
`--deploy-mode client` for the first attempt if executor connectivity is
still unproven — it isolates driver-side from executor-side failures.

## Scenarios

Run one section of `source_test_data.sql`, then one pipeline run, then
check. **Counts assume the default `overlap = 300` (5 minutes)**, which
deliberately re-reads rows at the window boundary — those re-reads are the
idempotency test, not a fault.

| # | Source change | Extracted | RAW total | CURATED total | The point |
|---|---|---|---|---|---|
| 1 | initial 5 rows (`--run-id e2e-1`) | 5 | 5 | **4** | first publish; F004 appears twice → in-batch dedup keeps the 10:20 version; empty strings become NULL |
| 2 | F001 updated, F005 inserted (`e2e-2`) | 3 | 8 | 5 | insert **1** (F005), update **1** (F001), ignored **1** (F004 re-read at the overlap boundary — same key, same freshness → target wins) |
| 3 | F002 timestamp bumped only (`e2e-3`) | 2 | 10 | 5 | **version advance:** F002's content is unchanged so its hash matches — curated keeps the business values and does **not** restamp `last_modified_op`, but `last_modified_datetime` advances to 12:00. insert 0, update 0, ignored 2 |
| 4 | NULL-key row inserted (`e2e-4`) | 2 | 12 | 5 | the null-key row lands in RAW (faithful) but is **quarantined** before curated: one `ingest_rejects` row with `CUR_001`, `user_email_id` hashed in the payload |
| 5 | no change (`e2e-5`) | 1 | 13 | 5 | idempotency: the boundary row is re-read, nothing changes, run SUCCESS |

### Verification queries

```sql
-- counts per scenario
SELECT COUNT(*) FROM membership_common_raw.smartiq_pdp_e2e;
SELECT COUNT(*) FROM membership_common_curated.smartiq_pdp_e2e;

-- 1: in-batch dedup kept the newer F004, empty strings became NULL
SELECT file_name, ai_size_contract_count, funding_type,
       ai_groupand_ba_numbers_section_number
  FROM membership_common_curated.smartiq_pdp_e2e WHERE file_name IN ('F004.pdf','F003.pdf');
-- expect F004 -> 75 ; F003 section/mds columns NULL (not '')

-- 3: version advanced, business content and audit stamp untouched
SELECT file_name, last_modified_datetime, ai_size_contract_count, last_modified_op
  FROM membership_common_curated.smartiq_pdp_e2e WHERE file_name = 'F002.pdf';
-- expect 2026-03-03 12:00:00 , content unchanged , op still 'I'

-- 4: quarantine with PII masked
SELECT error_code, raw_record FROM ingest_audit.ingest_rejects
 WHERE entity = 'smartiq_pdp_e2e';
-- expect CUR_001 ; no plaintext e-mail address in raw_record

-- per-run outcome, counts and status
SELECT run_id, stage, status, raw_count, insert_count, update_count, event_ts
  FROM ingest_audit.ingest_run_audit
 WHERE entity = 'smartiq_pdp_e2e' ORDER BY event_ts;

-- watermark advanced only after curated succeeded
SELECT * FROM ingest_audit.ingest_watermarks WHERE entity = 'smartiq_pdp_e2e';
```

### 6. Watermark holds on failure

Add `publish { validation_query = "SELECT * FROM {table} WHERE file_name = 'F005.pdf'" }`
to the curated block and re-run after any source change. Expect: the run
fails, curated is **unchanged**, and `ingest_watermarks` still shows the
previous value. Remove the query and re-run — the delta is picked up. This
is the guarantee that a curated failure can never lose data.

### 7. Replay cannot regress curated

```
--stage curated --run-id e2e-1
```

Replays batch 1's RAW slice — which contains F001 at 10:00, older than the
11:00 version already in curated. Expect curated **unchanged**: the
freshness merge ignores the stale rows. This is also the recovery drill for
"raw succeeded, curated failed".

### 8. Composite keys (optional)

Switch `curated.merge.keys` to
`["group_number", "ai_groupand_ba_numbers_section_number"]`, point curated
at a fresh table, clear the watermark row for the entity, and load the two
rows in section 6 of the seed script. They share `GRP700` but differ by
section — expect **two** curated rows. A single-column or prefix-only match
would collapse them into one.

## Notes and honest limits

- **RAW accumulates the overlap re-reads** (13 rows for 8 logical records by
  scenario 5). That is the default `AT_LEAST_ONCE_APPEND` delivery mode
  working as designed — raw is history, curated is truth. Set
  `raw.delivery_mode = "DEDUPLICATED_APPEND"` with an identity column to
  suppress them if the duplication bothers reviewers.
- **A truly empty extraction** needs `overlap = "0"`; with an overlap the
  boundary row is re-read on every run by design.
- **Scenario 3 is the one to watch.** It is the newest correctness fix in
  the framework and the least intuitive: same content, newer version. If
  `last_modified_datetime` does *not* advance there, stop and report it.
- Timestamps in the seed data are fixed (March 2026) so results are
  reproducible; they are unrelated to the wall clock.
- This plan validates the *pipeline*, not the 364-column mapping. Column
  breadth is covered by `SmartIqMappingConfigTest` in the build; what only a
  real run can prove is connectivity, permissions, Hive DDL agreement,
  watermark persistence and executor behaviour.
