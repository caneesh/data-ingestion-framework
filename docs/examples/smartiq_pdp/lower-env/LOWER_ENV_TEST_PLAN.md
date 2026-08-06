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
   `${LOCATION}`. Both are **ORC, EXTERNAL** — keep the `EXTERNAL` keyword:
   in Hive 3 a *managed* ORC table is created transactional (ACID) by
   default, and Spark 3.5 cannot write Hive ACID tables without the Hive
   Warehouse Connector. (Optional — the framework creates the tables itself if
   absent; pre-creating validates that your DDL and the framework agree.)
3. **Config:** copy `feed-smartiq-pdp-e2e.conf` + `smartiq-pdp-e2e-schema.conf`
   side by side (the feed `include`s the schema by relative name), then
   supply the connection details — see below.
4. **Dry run first:**
   `--entity smartiq_pdp_e2e --validate-only` — config, contract and
   connectivity, no reads or writes.

The feed has `executor_probe = true`: every run proves **executors** can
reach SQL Server before extraction begins, which is the half a firewall
test from the edge node cannot cover.

## Where the MS SQL connection details go

All of them live in the `sqlserver { }` block at the top of
`feed-smartiq-pdp-e2e.conf`. Each key carries a lower-environment default
and an optional environment override, so the same file serves both
environments:

| Setting | Config key | Environment override |
|---|---|---|
| Host | `sqlserver.host` | `SMARTIQ_HOST` |
| Port | `sqlserver.port` | `SMARTIQ_PORT` |
| Database | `sqlserver.database` | `SMARTIQ_DB` |
| Schema + table | `sqlserver.table` | `SMARTIQ_TABLE` |
| Service account | `sqlserver.user` | `SMARTIQ_USER` |
| Password | — (never in the file) | `SMARTIQ_DB_PASSWORD` |

The `source` block composes them into the JDBC URL, so you never edit the
URL by hand:

```hocon
url = "jdbc:sqlserver://"${sqlserver.host}":"${sqlserver.port}";databaseName="${sqlserver.database}
```

Either edit the defaults in the file, or leave them and export:

```bash
export SMARTIQ_HOST=lower-sql-01.corp.example.com
export SMARTIQ_DB=SmartIQ
export SMARTIQ_TABLE=dbo.SmartIQ_PDP_E2E
export SMARTIQ_DB_PASSWORD='...'
```

**Gotcha (verified, not assumed):** on the `--conf-path` route the
framework resolves substitutions against **environment variables only** —
`-D` system properties are *not* consulted there. Use `export`, not
`--conf spark.driver.extraJavaOptions=-Dsmartiq.host=...`. The password is
never read from the config file at all: it is fetched at run time through
the `env` secret provider, so it never lands in source control or in a
logged config dump.

For extra hardening the password can come from CyberArk, Azure Key Vault or
Conjur instead — swap the `password` provider block; the rest is unchanged.

## Run command

```bash
spark-submit --class com.hcsc.generic.ingest.app.IngestMain \
  --name ingest-smartiq_pdp_e2e --master yarn --deploy-mode cluster \
  --files feed-smartiq-pdp-e2e.conf,smartiq-pdp-e2e-schema.conf \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity smartiq_pdp_e2e --mode INCR --run-id e2e-1 \
  --conf-path ./feed-smartiq-pdp-e2e.conf
```

**Both files must be in `--files`** (the feed `include`s the schema), and
the config is handed over with **`--conf-path`**, not `-Dconfig.file` — see
the troubleshooting entry below for why.

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

## Troubleshooting

**`include was not found: 'smartiq-pdp-e2e-schema.conf'`** (job dies with
YARN exit code 13, `ApplicationMaster: User class threw exception ...
ConfigException$IO`)

HOCON resolves `include` relative to the *parent directory* of the
including file. A **bare filename** — the natural form in a YARN container,
where `--files` drops everything in the working directory — has no parent,
so the include falls back to the classpath and is not found.

Three things to check, in order:
1. Is the schema file in `--files`? Both the feed and the schema must be
   shipped; only the feed being present is the most common cause.
2. Are you passing `--conf-path ./feed-...conf` rather than
   `-Dconfig.file=feed-...conf`? The framework absolutises the
   `--conf-path` value, which restores the parent directory and makes the
   include resolve.
3. Running an older jar? The absolutising fix landed 2026-08-06; before it,
   a bare `-Dconfig.file` name could not resolve includes at all.

A stack ending in `cleanupStagingDir` with *"Operation category READ is not
supported in state standby"* is a **secondary** error from the shutdown
hook against a standby HDFS NameNode — it hides the real failure above it.
Always read the first `ERROR ApplicationMaster: User class threw exception`
line, not the tail. Pull the whole log with
`yarn logs -applicationId <appId>`.

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
