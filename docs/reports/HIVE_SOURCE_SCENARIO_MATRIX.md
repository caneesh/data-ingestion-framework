# Hive Source — Scenario Matrix and Regression Safeguards

**Status:** author pass merged with an independent QA enumeration and a
regression inventory of every touched file (2026-09-30). Baseline `mvn test`
recorded (R-01: 884 tests, 0 failures; after H1: 890; after H2–H5: 949). Items marked ◆ were
found by the independent pass and missed by the author pass. Section L
(2026-09-30) covers `raw.mode = SOURCE`, decided after the first pass. Companion to `HIVE_RAW_CURATED_IMPLEMENTATION_PLAN.md`;
plan item references (H0–H10) are to that document.

**Purpose:** before any code, (1) enumerate every scenario the `hive` source
must handle with a pass/fail criterion, and (2) name the evidence that no
existing feature regresses.

---

## 0. Two findings that change the plan — decide before coding

### 0.1 Late-arriving partitions are the realistic failure, not overwrites — **DECIDED 2026-09-30: lookback in scope**

The partition listing for `bstar_raw` shows irregular `file_date` gaps
(01-07, 01-08, 01-10, 01-11, 01-15, 01-23 …) — files do not arrive daily,
and `file_time` in the path means several can land per date. Under the
plan's strict `tuple > watermark` rule, a file for `2025-01-09` arriving
**after** `2025-01-10` was processed is skipped **permanently**: its tuple is
lower than the watermark. `inc_ful_flag = F` (periodic full re-sends) makes a
resend of an older date more, not less, plausible. The plan treats
"lookback" as an out-of-scope contingency gated on the overwrite question;
the evidence says it should be **in scope by default**.

Recommended change — a partition lookback, which is exactly JDBC's
`incremental.overlap` applied to partitions:

- `source.incremental.lookback = { partitions = N }` (or `{ days = N }`).
  Selection becomes `tuple > rewind(watermark, N)`; the committed upper is
  still the max tuple read. The recorded `window_start` stays the stored
  watermark, never the rewound bound — exactly how JDBC records overlap
  (`IngestPipeline.scala:1037-1044`), so `watermark_continuity` keeps
  working unchanged.
- Re-read rows re-append into RAW under a new `run_id` and are absorbed by
  the curated freshness merge — the framework's documented overlap pattern,
  already measured by the `raw_overlap_reread` check. Opting into
  `raw.delivery_mode = DEDUPLICATED_APPEND` with `raw.idempotency_key =
  [<key>, file_date, file_time]` removes even the RAW duplicates.
- This also covers an **overwritten** partition inside the window: the
  re-read picks up the new content; equal freshness tuples resolve by run
  order (later wins), which is the overwritten content.
- Beyond the window, `--stage reconcile` (`source_keys_present_in_curated`)
  remains the detector — as it is for JDBC.

Net effect on the plan: H3 gains the rewind in step 4; H5's `CFG_024` must
define hive's own `lookback` key rather than rejecting the concept; the
"Out of scope — lookback" item moves in; the H10 question to the source
owner becomes the answerable **"how late can a file arrive?"** (sizes N)
instead of the abstract "are partitions immutable?". Estimate: the 16–24h
conditional contingency becomes ~8–12h of base scope — cheaper, because it
is the existing overlap pattern, not a new model.

*Alternative considered:* a processed-partition **set** ledger (Kafka-offset
style — select `catalog − processed`). Handles arbitrarily late partitions
with no ordering assumption, but breaks `watermark_continuity` semantics,
forgoes `ingest_watermarks` and its reset/retention procedures, and grows a
ledger row per partition. Rejected in favor of lookback; noted so it is not
re-derived later.

### 0.2 `source.mode` is already a JDBC key — the H5 injection must not use it

H5 says "inject `mode` beside `entity`/`run_id`" into the source config.
`source.mode` **already exists** for JDBC (`FULL_TABLE | SELECT_QUERY |
CUSTOM_SQL | INCREMENTAL`, read by `JdbcSourceConfig` and by
`IngestionPattern.derive` at `:64`); the smartiq feed sets
`source.mode = "INCREMENTAL"`. Injecting the run mode under that key would
overwrite it and silently turn every incremental JDBC feed into a
`FULL_TABLE` extract. Inject as **`run_mode`**. Regression test: a JDBC feed
with `source.mode = INCREMENTAL` still derives `TIMESTAMP_INCREMENTAL` after
the pipeline change (Section 2, R-06).

---

## 1. Scenario matrix

Columns: **Expected** is what the operator observes (exit, ledger rows,
watermark, tables). **Pass** is the automated or manual check. **Plan** is
the item that covers it, or **GAP**.

### A. Selection and watermark lifecycle

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| A1 | First light: store has no row for entity; `initial_value` below every partition | All partitions read under one `run_id`; RAW gets all accepted rows; curated initial build; watermark = max tuple, version 1; ledger `window_start = initial_value`, `window_end = max` | RAW count = source count − rejects; one `ingest_watermarks` row; continuity n/a | H3, H10 |
| A2 | Steady state: N new partitions above watermark | Only those partitions read (pruned scan); merge; watermark = new max; `window_start` = previous `window_end` | RAW delta = rows of N partitions; `watermark_continuity` passes | H3, H4 |
| A3 | Empty window: no partition above watermark | Zero rows; ledger `raw`/`curated` SUCCESS with 0 counts; no RAW write, no publish; watermark **unchanged, version unchanged**; window recorded as `(lower, lower)` | No new RAW files; `ingest_watermarks` row count unchanged; next run's continuity passes via `start == prevStart` | H3 step 5 — **verify** `advanceWatermark` with an empty selection does not bump the version |
| A4a | Late partition, **same** `file_date` as watermark, higher `file_time` | Tuple is greater → selected normally | As A2 | H3 |
| A4b | Late partition, **older** `file_date` than watermark | Strict rule: skipped forever. With §0.1 lookback: read if within N | Without lookback: `source_keys_present_in_curated` > 0 at next reconcile. With lookback: rows land; RAW overlap re-read recorded | H3 step 4 (lookback, inside N); reconcile beyond N |
| A5 | Existing partition overwritten after being read | Strict rule: never re-read; curated stale; key-based reconcile **cannot** detect (keys still present). With lookback inside N: re-read, later run wins | Manual: content diff. With lookback: curated reflects new content | H3 step 4 (lookback, inside N); undetectable beyond N. **Owner confirms 2026-10-01: partitions are never rewritten in place** — defensive only |
| A6 | ◆ Catalog partition whose files were **deleted** (external table, data removed) | Selected; the read returns 0 rows or throws `FileNotFoundException` depending on `spark.sql.files.ignoreMissingFiles`; watermark advances past it | The setting is pinned explicitly for this read and the partition is named in the log; nothing to recover either way | H3 step 8 (amended) |
| A6b | ◆ Partition **registered in HMS before its files finish landing** (writer registers, then writes) | Read sees 0 or partial rows; the upper comes from enumeration → watermark advances; files land later → **lost** under the strict rule. Lookback (§0.1) re-reads it next run | With lookback: rows land on the next run. Without: `source_keys_present_in_curated` is the only detector | H3 step 4 (lookback) |
| A7 | Files on disk not registered in catalog (`MSCK REPAIR` never run) | Invisible to enumeration; never read; a later registered partition advances the watermark past them → **lost** | Runbook preflight; reconcile is the detector | **GAP** — runbook must state `MSCK REPAIR TABLE` is the source owner's obligation |
| A8 | Partition tuple exactly equal to watermark | Not selected (strict) | Empty window | H3 |
| A9 | `initial_value` above every partition | Empty first light; nothing ever loads until a newer partition appears | `min_accepted_rows` floor (existing) is the alarm if configured | H3; H8 note |
| A10 | Partition columns present but `watermark_columns` in a different order than the path | Order is taken from config, not the path; lexicographic over config order | Unit test: order independence of `HIVE_003`; predicate order = config order | H3 |
| A11 | ◆ Equal-length but invalid value (`file_time = 99.99.99`) | Passes the length guard; sorts past every real time → watermark jumps ahead; later real partitions under that date are skipped | `watermark_formats` strict parse rejects it before selection | H9 (amended — `watermark_formats` is now **set** in the bstar config) |

### B. Rejects

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| B1 | Every row of the newest partition fails a reject rule, `on_reject_watermark = ADVANCE` | Rows in `ingest_rejects` (PII masked); RAW gets none of them; watermark advances past the partition (upper from enumeration, not accepted rows) | Watermark = that partition's tuple; `rejected_count` on ledger | H3 step 7 |
| B2 | Same, `= HOLD` | `discardWindow`; watermark unchanged; next run re-reads the window; previously accepted rows re-append (absorbed) | Watermark unchanged; `windowHadRejects` true; next run's continuity passes | H4 (existing pipeline policy) |
| B3 | `max_reject_percent` / `max_reject_count` tripped | Run FAILS at raw stage; watermark not advanced (failure → discard) | Ledger `raw FAILED`; watermark unchanged; alert sent | existing |
| B4 | Null / blank business key rows | Quarantined by curated (CUR_001), counted in `nullKeyCount`; accounting identity holds | `curated_accounts_for_accepted_rows` passes | existing |

### C. Failure and recovery

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| C1 | Crash during RAW write | Partial RAW files under this `run_id`; watermark unchanged. Next run (new `run_id`) re-reads the window; RAW carries both attempts; curated dedups | No loss; `raw_equals_accepted` passes on the new run | existing (run-id guard `IngestPipeline.scala:597-633`) |
| C2 | RAW SUCCESS, curated publish FAILS | Watermark **not** advanced (requires curated success). Recovery A: next INCR re-reads + re-appends (absorbed). Recovery B: `--stage curated --pending` publishes from the existing RAW slice | Either path ends with curated current; ledger shows both runs | existing |
| C3 | After C2 recovery B (`--pending` in a fresh JVM) | `readWindows` is in-memory → `advanceWatermark` has nothing to commit → watermark **still** unchanged → next INCR re-reads the same window again (absorbed) | Documented; duplicates absorbed | **GAP (improvement):** `HiveSource.advanceWatermark` can fall back to the ledger's persisted `window_end` for that `run_id` (`AuditService.lastRawWindow`-style query) and commit it — closes a limitation JDBC has today. Recommend in scope for H4. |
| C4 | `WatermarkConflictException` on commit (curated already published) | Run FAILS; alert; watermark unchanged → next run re-reads (absorbed) | Ledger FAILED with JDBC_005 text; lock is the primary guard so this is rare | H4 |
| C5 | `--resume --run-id X` after raw SUCCESS | Raw SKIPPED; curated from RAW slice; `curatedResumedComplete` → advance path taken, but same in-memory limitation as C3 | As C3 | **GAP** — same fix as C3 |
| C6 | Same `run_id` re-run **without** `--resume` | Run-id guard skips RAW write; `raw_equals_accepted` fails by construction; reconciliation FAIL with the explicit re-run hint | Clear message; no double data | existing |
| C7 | `--resume-ingest-dt D` (curated-only replay) | Re-merges RAW rows with `ingest_dt = D`; no watermark involvement | Curated idempotent | existing |
| C8 | `--replay-from/--replay-to` | Decoupled replay by raw-SUCCESS date | As documented | existing |
| C9 | Crash after enumeration, before any write | Nothing persisted; in-memory window discarded on JVM exit | No ledger row past `validate`; watermark unchanged | H3 |
| C10 | Lease lost mid-run (heartbeat renewal fails) | `ensureOwnership` throws before the watermark commit; run FAILS | Watermark unchanged | existing (`IngestPipeline.scala:368`) |
| C11 | ◆ Crash between the watermark commit and any later ledger write | Curated SUCCESS is recorded before the commit in the completion block, so no run is lost; verify nothing is written to the ledger *after* `advanceWatermark` | Code read + one test asserting row order | H7 (verify, low) |

### D. Modes and stages

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| D1 | `--mode FULL`, store empty (first light) | `HIVE_007` passes; all partitions; curated full build; watermark committed | As A1 | H5 |
| D2 | `--mode FULL`, store non-empty | `HIVE_007` refuses **before any read**; exit non-zero; ledger FAILED; alert | RAW, curated, watermark untouched | H5 |
| D3 | `--stage raw` only | RAW written; curated skipped; watermark **not** advanced unless `watermark.advance_after = RAW`; warning logged | Next run re-reads (absorbed) | existing |
| D4 | `--stage curated` / `--pending` | See C2/C3 | | |
| D5 | `--stage reconcile` | Hive dispatch; three checks recorded; `where` honored on the source side; REPORT vs FAIL respected | Three rows in `ingest_reconciliation` | H6 |
| D6 | `--stage retention` | `ingest_dt` partition drops on `bluestar_raw`; rejects/audit purge; `watermarks_keep_last` trims this entity's history; **`bstar_raw` never touched** | History rows ≤ N; source table unchanged (invariant test) | existing via H1 reuse; invariant is **GAP** in H7 |
| D7 | `--dry-run` | Reads and validates; writes nothing; ledger SUCCESS with `dry-run` message; watermark not advanced | No RAW/curated/watermark change | existing (`IngestPipeline.scala:364`) |
| D8 | `--validate-only` | No `entity` injected → validate mode: enumerate, `HIVE_003/005/006`, contract validation on the schema frame; exit 0; no side effects | No ledger row; no store access | H3 step 1 |
| D9 | Override file changes `initial_value` mid-life | Ignored while a stored watermark exists | Log states stored value used | H3 step 3; H8 note |
| D10 | Override file changes `watermark_columns` mid-life | Stored value arity ≠ new size → `HIVE_005`, fail closed | Clear error; runbook: "requires a watermark reset" | H3 step 3; **GAP** runbook line |
| D11 | `--explain-mapping` | No-op for hive (managed-folder feeds only) | Documented | H8 |

### E. Concurrency

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| E1 | Two runs of the same entity overlap | Second blocked (`PIPE_001`); no double read | Exit per runbook | existing |
| E2 | Crashed holder; lease expiry | Takeover after `lease_minutes` | existing | existing |
| E3 | Two JVMs both past a best-effort lock | Second commit hits CAS → `JDBC_005` | H4 | H4 |

### F. Source-side change on `bstar_raw`

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| F1 | Business column **added** | Contract `on_extra_column` (default WARN); RAW pre-created DDL: `HiveSink` drops it with WARN until DDL is altered | Per policy; runbook: `ALTER TABLE … ADD COLUMNS` | existing |
| F2 | Business column **removed** | `on_missing_column` FAIL for required; default-fill for optional | Per policy | existing |
| F3 | Business column **renamed** | Alias in contract resolves (H3 step 9); else treated as F2 | Canonical names in RAW | H3 |
| F4 | Business column **type changed** | `on_type_change` FAIL | Fail closed | existing |
| F5 | **New partition column** added (4 levels) | Not a watermark column → flows as a data column; `HIVE_003` not triggered; RAW DDL must add it or it is WARN-dropped | Documented | H3; H8 note |
| F6 | Watermark partition column **removed** | `HIVE_003` at read; fail closed before any write | Clear error | H3 |
| F7 | Non-zero-padded value appears (`6.24.07`) | `HIVE_006` before read; no partial load | Clear error naming column and value | H3 step 6 |
| F8 | Watermark partition column **type** changed to non-STRING | Lexicographic vs numeric mismatch risk | **GAP:** add to `HIVE_003` — watermark columns must be STRING-typed partition columns | H3 |
| F9 | Source table dropped / renamed | `HIVE_002`; alert | Fail closed | H3 |
| F10 | HMS / catalog unreachable | Spark exception → FAILED; Control-M retry | Per runbook exit codes | existing |
| F11 | Source table recreated unpartitioned | `HIVE_003` | Fail closed | H3 |

### G. Contract

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| G1 | Aliases map source names to canonical | Renames applied inside `HiveSource` before the pipeline's contract gate | Canonical names in RAW | H3 step 9 |
| G2 | Required column missing | `HDR` failure; run FAILED (no file to quarantine) | Fail closed | existing |
| G3 | PII-tagged columns | Reject payloads masked | existing | existing |
| G4 | `record_hash` with partition columns tagged `category = "audit"` | Identical row re-delivered under a new `file_time` hashes equal → no curated churn | Hash equality test | H9 |
| G5 | Contract omitted entirely | Every non-framework column hashed; `file_time` makes each re-delivery a change; RAW DDL drop-with-WARN unguarded | Plan makes the contract mandatory | H9 |
| G6 | Contract `version` mismatch with RAW table property | `on_version_mismatch` policy | existing | existing |

### H. RAW target

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| H1 | Pre-created DDL missing `file_date`/`file_time`/`inc_ful_flag` | Contract `required = true` → run FAILS, not WARN-and-drop | Integration test | H7, H9 |
| H2 | DDL declares one as DATE/other non-STRING | Positional `insertInto` with no cast; ANSI store-assignment may error or coerce per value | **GAP:** H9 must state STRING; H7 test that a DATE-typed `file_time` fails loudly | H9 |
| H3 | RAW table absent | Framework creates it partitioned by `ingest_dt` with framework columns | Table exists after first run | existing |
| H4 | Run straddles midnight in the session zone | `current_timestamp()` is fixed per query → one `ingest_dt` per write | Single partition per run | existing |
| H5 | Source has a column named like a framework column (`run_id`, `source_file`, …) | `RAW_003` fail closed | Clear error | existing |
| H6 | `file_type` on RAW | Stamped `I|F` from `--mode`, unrelated to `inc_ful_flag` | Documented | H8 |

### I. Curated

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| I1 | Key re-delivered in a later partition | Freshness `(file_date, file_time)` string compare → newer wins | Curated row = newer | config |
| I2 | Older re-delivery (only reachable via A4b/lookback) | Older tuple → ignored as stale; `ignoredCount` | Accounting identity holds | config |
| I3 | **Duplicate keys within one partition** (equal tuples) | Which row wins? Undefined without `dedup.order_by`; `enforce_unique_keys` fails the publish | **GAP:** target config must set `dedup.order_by` (or a deterministic tie-breaker) — the plan's config block omits `dedup` | H9 |
| I4 | Null key | Quarantined (`drop_null_keys` default) | existing | existing |
| I5 | `deletes.mode = IGNORE`, keys absent from source | Retained; reconcile reports the count | existing | existing |
| I6 | Empty window + `publish.allow_empty = false` | No publish attempted on an empty frame (smartiq weekends prove this) | Ledger SUCCESS, no CUR error | existing — **verify** in H7 A3 test |
| I7 | `FULL` first light against a pre-existing curated table | Full overwrite | existing | existing |
| I8 | CDC action-code vocabulary — `DL` delete, `PT` insert, `UP` update (owner-confirmed 2026-10-01) | `deletes.indicator_values = ["dl"]`; match is `lower(trim(value))`, so CHAR(2) padding is harmless. A `DL` row tombstones; `PT`/`UP` merge by freshness | Test: a `DL` row sets `is_deleted = true` / `last_modified_op = 'D'`; a later `UP` for the same key reactivates it | H9 (resolved) |

### J. Reconcile

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| J1 | `where` excludes `F` partitions | Source-side count/keys honor `where`; otherwise false "missing keys" | No false positives | H6 |
| J2 | Late partition beyond lookback (A4b) | `source_keys_present_in_curated` > 0 — **the** detector | Reported/failed per `on_mismatch` | H6 |
| J3 | Thousands of partitions | Source side is a key-only projection scan of `bstar_raw`; schedule off-peak | Runbook | H6, H8 |
| J4 | `curated_keys_absent_from_source` | Informational; non-zero under IGNORE is the evidence of source deletes | existing | existing |

### K. Operations

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| K1 | Watermark reset (delete rows / set to `initial_value`) | Next run re-reads all; `watermark_continuity` FAILS with the reset hint — the hint compares `window_start` to `initial_value`, which matches because both use `WatermarkValue` serialization | Hint text appears; one run with `on_mismatch = WARN` | H4, H8 |
| K2 | Failure notification | Webhook/command fires with sanitized message | existing | existing |
| K3 | Exit codes | Per runbook retry table | existing | existing |
| K4 | Jar upgrade between runs | `framework_version` differs on ledger; `config_fingerprint` unchanged | existing | existing |
| K5 | HMS pushdown throws (`metastorePartitionPruningFallbackOnException = false`) | `listPartitionsByFilter` fails → fallback `listPartitions` + driver filter; logged | Unit test of the fallback branch | H3 step 5 |
| K6 | `hive.metastore.limit.partition.request` rejects the unfiltered fallback | FAILED with a message naming the setting | Clear error | H3 step 5; H8 |
| K7 | **Invariant: the source table is never written, altered or dropped** | No DDL/DML ever targets `bstar_raw` | Test asserts source partition set and row count unchanged after every scenario | **GAP** — add to H7 |
| K8 | Feed run under a different `--entity` by mistake | New watermark row; re-reads all | Runbook footgun note | H8 |
| K9 | Two feeds on the same source table with different entities | Independent watermarks | Fine | — |
| K10 | Watermark store table absent on first run | `HiveTables.ensure` creates it | existing | existing |
| K11 | ◆ **Exit-code class for `HIVE_001–007` and metastore failures** | Control-M needs TRANSIENT (retry) vs CONFIGURATION / DATA_INTEGRITY (page). Today an uncaught HMS exception exits unclassified | `HIVE_001–007` → CONFIGURATION; HMS/catalog connectivity (`MetaException`, `TException`, connection refused during `listPartitionsByFilter`/`tableExists`) → TRANSIENT; mapped in `runtime/FailureClass.scala` and the runbook table | H5, H8 (amended) |
| K12 | ◆ Source partition **key set** changes after first light (a 4th partition column appears) | Pruning is still correct on the watermark columns; every value of the new column is selected — silent semantic widening | WARN naming the new column; no error | H3 step 2 (amended, low) |
| K13 | ◆ Long HMS enumeration vs. lock lease | Heartbeat runs on its own thread every `lease/3`; enumeration cannot starve it | Assert renewals during a slow (mocked) enumeration | H7 (low) |
| K14 | **The upstream Sqoop run from DB2 never lands a file** | The INCR run finds no new partition and succeeds with zero rows; every in-run check passes; the curated freshness monitor stays green because the last SUCCESS is recent | MONITORING checks the newest `file_date` partition in `bstar_raw` against an age threshold; `min_accepted_rows = 1` only if a zero-change day is impossible | H10 — **the one failure this feed's in-run checks cannot see** |

### L. `raw.mode = SOURCE` — the source as the raw layer (decided 2026-09-30)

| # | Scenario | Expected | Pass | Plan |
|---|----------|----------|------|------|
| L1 | First light under `SOURCE` | No RAW table is written or created; ledger `raw SUCCESS` row carries the partition window and `raw_count` = accepted; curated built; watermark committed | `SHOW TABLES IN bluestar_raw` shows only control tables; ledger row present | H11 |
| L2 | `--stage curated --pending` in a fresh JVM | Re-reads the *source* for the recorded `window_start`/`window_end` of that run and publishes; watermark advanced from the ledger (C3 fix) | Curated current; no RAW access attempted | H11 |
| L3 | A recorded window's partition was purged from the source before replay | `HIVE_008`, fail loudly naming the partition; nothing published | Clear error; ledger `curated FAILED` | H11 — the coupling cost of `SOURCE` |
| L4 | `--resume-ingest-dt` under `SOURCE` | Rejected at CLI validation — there is no `ingest_dt` | Clear error pointing at `--pending` / `--replay-from` | H11 |
| L5 | `retention.raw`, `raw.database/table`, `delivery_mode`, `idempotency_key`, `partitioning` present under `SOURCE` | `CFG_028` each | Validator test per key | H11 |
| L6 | `raw.mode = SOURCE` with `source.type = jdbc` / `file` / `kafka` | `CFG_028` — only a Hive source is a durable table | Validator test | H11 |
| L7 | Lookback re-read under `SOURCE` | Re-read rows re-feed curated; freshness absorbs; `raw_overlap_reread` recorded; no RAW-duplicate question exists | Accounting identity holds | H11 |
| L8 | `raw_equals_accepted` and the run-id idempotency guard | Skipped under `SOURCE`; `source_equals_accepted_plus_rejected`, `curated_accounts_for_accepted_rows`, `watermark_continuity` unchanged | Reconciliation rows present for the unchanged checks only | H11 |
| L9 | A `COPY`-mode hive feed (future) and every existing JDBC/file feed | Byte-identical behavior before and after H11 | R-17 goldens | H11 |
| L10 | Source retention shorter than the replay horizon | Replay beyond the source's retention is impossible by construction; `--stage reconcile` still proves curated against whatever the source retains | Runbook: `SOURCE` makes the source owner's retention promise part of the feed's contract | H8 |
| L11 | Daily morning load, one partition per table per day (the bstar cadence) | INCR once daily after the load window reads exactly one new partition plus the lookback re-read; a second same-day file, a late afternoon re-delivery or an `F` on any day are read by the same run — the cadence is scheduling, not an assumption | Integration: two partitions in one day land in one run | H3, H10 |

---

## 2. Regression safeguards

**Principle:** every existing test passes with **zero behavioral edits**
(import-path rewrites for the H1 move are the only permitted change); the
baseline `mvn test` counts per module are recorded before the first PR and
re-compared after each of the four PRs in the plan's execution order. Where
a behavior the plan touches has **no test today**, a golden test is added
*before* the change so the "before" is pinned.

| # | Invariant | Evidence / action |
|---|-----------|-------------------|
| R-01 | Baseline test counts per module, before any change | **Recorded 2026-09-30, main @ `a4ab516`, `mvn test -fae -Dmaven.test.failure.ignore=true`, BUILD SUCCESS in 11:17** — core 356 tests / 48 suites; file 26 / 4; jdbc 251 / 35; kafka **0 / 0 (no test directory)**; config-gen 78 / 15; app 173 / 19. **Total 884 tests, 121 suites, 0 failed / canceled / ignored.** Re-run after each PR; a module whose count drops or whose failures rise above 0 is a stop. **After H1** (branch `h1/watermark-to-core`, 2026-09-30): core 358 (+2 `WatermarkValueTest`), file 26, jdbc 255 (+4 `WatermarkStoresTest`), kafka 0, config-gen 78, app 173 — **total 890, 0 failed / canceled / ignored**, BUILD SUCCESS 11:32, fat-jar layout verified. **After H2–H4** (`8df162f`): + hive 42 → **932**. **After H5**: core 372 (+2 goldens, +3 pattern, +6 validator, +3 FailureClass incl. the `ARCHIVE_` regression), jdbc 256 (+1 R-09), app 175 (+2 R-10), hive 42, others unchanged → **949, 0 failed / canceled / ignored**, BUILD SUCCESS 11:31; goldens pass before and after the pattern change |
| R-02 | JDBC watermark behavior unchanged after H1 (predicates, overlap, bounded windows, CAS, duplicate-version pick) | `WatermarksTest`, `WatermarkCodecsTest`, `WatermarkHardeningTest:60`, `IncrementalWindowFixesTest:69, :97`, `BoundedWindowH2Test`, `JdbcSourceH2Test:184`; `HiveWatermarkStoreSpec`, `HiveWatermarkDuplicateVersionSpec:47` (app) |
| R-03 | `JDBC_003` / `JDBC_005` message text unchanged by the package move | Asserted in `HiveWatermarkStoreSpec:70-74, :86`, `JdbcSourceH2Test`, `PartitionStrategiesH2Test:80`, `BoundedFullLoadTest:49` — the strings are the contract |
| R-04 | `WatermarkValue` codec: the two existing round-trip tests move with the type | `WatermarkCodecsTest:89` (+ legacy unescaped at :102-103), `WatermarksTest:103`. The plan's "no codec test exists" was wrong; corrected in H1 |
| R-05 | Fully-qualified references to the old package rewritten, nothing else | `JdbcSourceH2Test:203, :206` name the moving types fully qualified; 9 test files import `…jdbc.watermark`; 6 test files live in that package — imports only, since every `private[watermark]` member is on `object Watermarks`, which stays. **Inventory miss, found in execution:** `ingestion-config-gen/…/DryRunValidator.scala:6` also imported `WatermarkValue` (config-gen depends on jdbc). Compile gates must run the whole reactor, never a module subset |
| R-06 | `IngestionPattern.derive` output identical for every existing `jdbc`/`file`/`kafka` fixture | `IngestionPatternTest` (:16, :33, :51, :59, :72, :87, :94). **Golden tests to add first:** the `case _ => FILE` fallthrough and the `upperBoundStrategy = MAX_VALUE` default — untested today, and both are rewritten by H5 |
| R-07 | Every `CFG_001–022` rule fires exactly as before | `FeedCompatibilityValidatorTest` (:20-131, :154), `IngestionPatternTest` (010/011/012), `ExecutionModeTest` (016), `JdbcPipelineIntegrationSpec` (013/014). `CFG_015` has **no test** — pre-existing |
| R-08 | The smartiq feed still validates clean after the new rules | `SmartIqMappingConfigTest:16` — live canary |
| R-09 | **`source.mode = INCREMENTAL` on a JDBC feed still derives `TIMESTAMP_INCREMENTAL` after the `run_mode` injection** (§0.2) | New test in `IngestionPatternTest` |
| R-10 | Sources still receive `entity` / `run_id` after the `effectiveSource` change | Today only indirect (`JdbcPipelineIntegrationSpec:371, :423`; `PipelineIntegrationSpec:183, :194`). Add one direct assertion via a capturing test source |
| R-11 | `--stage reconcile` on a JDBC feed behaves identically through `IngestMain` | **No test drives `runReconcile` today** — lock acquire/release, heartbeat, the `reconcile` ledger rows, `notifyFailure`, the FAIL-throw and the `finally` are all unverified, and H6 rewrites exactly that dispatch. Add an `IngestMain`-driven JDBC test *before* H6 |
| R-12 | `--validate-only`, `--dry-run` on the existing smartiq feed unchanged | e2e config in lower env; H10 checklist |
| R-13 | Assembly jar contents | `unzip -l` before/after: `com/hcsc/generic/ingest/hive/` present, `com/hcsc/generic/ingest/watermark/` present, `jdbc/watermark/WatermarkStore*` absent; every JDBC driver still bundled; `spark-sql`/`spark-hive` still `provided`. All nine `scripts/*.sh` hardcode the jar name — no script change |
| R-14 | No `source.type` site changes its default for an absent or unknown type | Complete inventory: `IngestionPattern:63, 74-78`; `FeedCompatibilityValidator:20, 29, 86, 97`; `RawStageRunner:38`; `IngestPipeline:80, 244, 369, 511, 586, 669`; `confgen/DryRunValidator:39-45`, `ConfigAssembler:37-38, 122`, `ConfigGeneratorMain:100, 131`. No exhaustive match throws on an unknown type |
| R-15 | Kafka path unchanged | **`ingestion-kafka` has no test directory** — nothing can prove this. The plan cites `KafkaSource` as the `WatermarkAdvancing` template; it is the shape to copy, not verified behavior. Adding Kafka tests is out of scope; recorded |
| R-16 | Legacy 5-column `ingest_watermarks` tables | Pre-existing, untested: `HiveTables.ensure` is `CREATE IF NOT EXISTS` only, so a store created before `lower_value`/`query_hash` fails positional `insertInto`. Not hive-specific; `bluestar_raw` is new and cannot hit it. Backlog |
| R-17 | `raw.mode = COPY` (default) path byte-identical after H11 | Goldens: a `COPY`-mode hive feed through the full pipeline, plus the existing `JdbcPipelineIntegrationSpec` / `PipelineIntegrationSpec` / `IngestFlowIntegrationSpec` unchanged; the `SOURCE` branch is reached only when the flag is set |

---

## 3. Gaps this pass adds to the plan

Ranked by severity. ◆ = found by the independent QA pass, missed by the
author pass. *(Amended)* = the plan has been edited accordingly.

1. **Late partitions (A4b) and partitions registered before their files land (A6b) → lookback in scope** — §0.1. **Decided 2026-09-30: in scope.** *(Amended: target config, H3 steps 4 & 7, H5 `CFG_027`, H7, H10, Out-of-scope)*
2. **`source.mode` collision** — §0.2. Inject `run_mode`; R-09. *(Amended)*
3. ◆ **Exit-code classification for `HIVE_001–007` and metastore failures** (K11) — without it Control-M cannot tell retry from page. *(Amended: H5, H8)*
4. **Resume / `--pending` never advances the watermark** (C3, C5) — commit from the ledger's persisted `window_end`. *(Amended: H4)*
5. ◆ **`watermark_formats` must be set, not optional** (A11). *(Amended: target config, H9)*
6. **Duplicate keys within one partition have no winner** (I3) — needs a deterministic source tie-breaker column or an explicit statement. *(Amended: target config, H9 — the column choice is H9's)*
7. **Plan's H1 codec-test claim was wrong** — two round-trip tests exist and move with the type (R-04). *(Corrected)*
8. **`--stage reconcile` has no `IngestMain`-driven test** (R-11) and the two `IngestionPattern` branches H5 rewrites have none (R-06) — goldens first. *(Amended: H5, H6, H7)*
9. **Source-table write invariant untested** (K7). *(Amended: H7)*
10. **STRING-typed watermark columns not enforced** (F8, H2). *(Amended: H3, H7)*
11. ◆ **Deleted-files partition behavior unpinned** (A6) — `ignoreMissingFiles` explicit, zero-row partitions logged. *(Amended: H3 step 8)*
12. ◆ **Partition key set widening** (K12) — WARN on change. *(Amended: H3 step 2)*
13. **`MSCK REPAIR` obligation, `watermark_columns` change → reset, `initial_value` override no-op — undocumented** (A7, D10, D9). *(Amended: H8)*
14. **Empty-window version-bump and `allow_empty` interplay unverified** (A3, I6). *(Amended: H7)*
15. ◆ **Ledger-write ordering after the commit** (C11) and **heartbeat during long enumeration** (K13) — verify, low. *(Amended: H7)*
16. **`raw.mode = SOURCE` decided 2026-09-30** — the bstar feeds use the source as the raw layer; Section L and R-17 added; plan H11 written. *(Amended: plan Goal, Decisions, target config, H9, H10, H11, Out-of-scope, execution order)*
