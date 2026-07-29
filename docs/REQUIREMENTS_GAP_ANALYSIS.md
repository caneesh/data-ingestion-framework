# SQL Server Raw/Curated Ingestion — Requirements Gap Analysis

**Date:** 2026-07-28 (verified revision)
**Scope:** Assessment of the `data-ingestion-framework` codebase against the
"SQL Server to Raw and Curated Data Ingestion Requirements" specification
(§1–§22). All file/line references are to the current `main` branch.

**Verification status:** every finding in sections 2–3 was re-checked by an
independent adversarial review pass that attempted to *refute* each claim
(searching for existing code paths, config options, or tests that already
provide the capability). Verdicts are noted inline as **[CONFIRMED]**,
**[CONFIRMED — understated]** (worse than originally stated), or
**[RESCOPED]** (original claim partially correct; corrected here). No
finding was refuted outright. The verification pass also surfaced five new
critical findings (C8–C12) missing from the original analysis.

---

## 1. Executive Summary

The framework meets the requirements **partially — roughly half**.

The **extraction side is strong and close to spec**: versioned watermarks
committed only after end-to-end success, bounded incremental windows with a
configurable overlap, partitioned parallel JDBC reads, enterprise secret
providers with enforced TLS, metadata-driven schema contracts with
configurable drift policies, and a full reject/quarantine subsystem.

The **curated layer, raw lineage metadata, delete handling, and change
detection do not meet the requirements**, and verification confirmed
several additional silent-data-loss paths (rejected rows lost from the
incremental window, unbounded-window fallback, no publish volume guardrail).
These are correctness gaps that cause the specification's acceptance
criteria (§22) to fail today — most notably, a late-arriving older record
**will overwrite a newer curated record** because the incremental publish
performs no freshness comparison.

| Verdict | Count | Requirement sections |
|---|---|---|
| Met | 2 | §12 Reject/quarantine, §5.4 Overlap window |
| Largely met | 3 | §5 Incremental extraction, §13 Schema evolution, §16 Security |
| Partial | 9 | §2, §3, §4, §10, §11, §15, §19, §6.2/6.3 (keys/dedup) |
| Not met / Absent | 7 | §6 Curated merge, §7 Change detection, §8 Deletes, §9 Late-arriving, §14 Timestamps, §17 Retention, §18 Concurrency |

---

## 2. Critical Findings (correctness — acceptance criteria fail)

### C1. Curated merge has no freshness comparison (§6.3, §6.4, §9) — [CONFIRMED — understated]

The incremental publish is an anti-join + union + full-table
`INSERT OVERWRITE`, not a keyed MERGE:

- `CuratedService.scala:168-171` — `target.join(incomingKeys, keys, "left_anti")`
  then `unionByName(alignedIncoming)`; incoming rows **unconditionally**
  replace target rows for matching business keys.
- There is no `incoming.source_last_modified_ts > target.source_last_modified_ts`
  predicate anywhere in the repo (`source_last_modified_ts` has zero
  occurrences). Target rows never participate in `deduplicate` (which runs
  earlier, at `:152`, on the incoming batch only).
- Verification found no config option, merge-strategy knob, or code path
  that prevents older-overwrites-newer across runs. The only escape hatch is
  the user-supplied `publish.validation_query` (`PublishService.scala:93-100`),
  which could hand-detect staleness but would **fail the whole run** rather
  than preserve the newer row, and depends on a freshness column the
  framework never provides (C2).

**Aggravation found in verification:** a stale overwrite also inherits the
original `create_timestamp` and is stamped `last_modified_op = 'U'` with a
fresh `last_modified_ts` (`CuratedService.scala:119-122`) — the audit trail
actively misrepresents the regression as a legitimate, current update.

**Consequence:** a late-arriving *older* record delivered in a later run
overwrites a newer curated record — directly violating acceptance criteria
"older late-arriving records do not overwrite newer curated records" and
"the latest source version wins."

### C2. Raw technical metadata is almost entirely missing; what exists is useless for JDBC (§3.2) — [CONFIRMED — understated]

The framework stamps exactly six columns
(`RawMetadata.scala:9-24`): `run_id`, `source_file`, `row_idx`,
`load_timestamp`, `file_type`, `file_id`.

Of the 14 required technical columns, **13 do not exist under any name**:
extract window (`extract_start_ts`/`extract_end_ts`), source identity
(`source_system`/`source_database`/`source_schema`/`source_table`),
`source_last_modified_ts`, `record_hash`, `source_operation`,
`source_primary_key`, `load_status`.

**Verification finding — the existing columns carry nothing for JDBC feeds:**
`source_file` comes from `input_file_name()`, which is **empty** for JDBC
rows, and `file_id = sha2(source_file, 256)` is therefore a **single
constant for every JDBC row of every run** (`RawMetadata.scala:14-23`).
JDBC raw rows carry no source lineage at all beyond `run_id`. `load_timestamp`
is ingestion time — monotonically increasing with run order — and is
therefore actively *wrong* as a freshness proxy (a late-arriving older
record gets the higher timestamp).

**Aggravating factor — `run_id` is silently dropped by the shipped DDL:**
`HiveSink` projects the DataFrame down to the target table's columns and
only logs a warning for extras (`HiveSink.scala:35-40`; the `require` at
`:33` guards only the opposite direction). The repo's own reference DDL
(`ddl/health_sherpa_member.sql:3-11`) and `README.md:51` both omit `run_id`.
Following them silently disables the raw replay guard
(`IngestPipeline.scala:316-318`), raw-count measurement (`:337-340` →
`rawCount = -1`), and the `raw_equals_accepted` reconciliation check
(`:437`); `--resume` curated replay fails **loudly** with an
`AnalysisException` (`:404`, unresolved `run_id` column). No validation
anywhere catches the missing column, and no test exercises the drop path —
every integration test lets the framework auto-create the raw table, which
does include `run_id` (`PipelineIntegrationSpec.scala:180`), so the
pre-created-DDL production path has zero coverage.

### C3. Source deletes are unsupported (§8) — [CONFIRMED]

No soft-delete handling, no SQL Server Change Tracking, no CDC, no
reconciliation-based delete marking, no `is_deleted` flag. `deleteCount` is
hard-coded to `0` on both publish paths (`CuratedService.scala:99,172`).
There is no `last_modified_op = 'D'` handling despite the column existing
in the curated DDL. On the INCR path a source-deleted record persists in
curated indefinitely via the `left_anti` "unchanged" branch; only a FULL
run removes it (hard delete by replacement, `PublishService.scala:68`).

### C4. Initial full load: no supported cutoff/seed handoff, and the workaround drops NULL-watermark rows (§4.2) — [RESCOPED]

Original claim ("a full load captures no cutoff and seeds nothing") was
**partially correct** and is corrected as follows:

- Literally true for `source.mode = FULL_TABLE` (and `SELECT_QUERY`/
  `CUSTOM_SQL`/`SQL_TEMPLATE`): watermark config is parsed only for
  `INCREMENTAL` (`JdbcSourceConfig.scala:105`), so no cutoff is captured
  and `advanceWatermark` is a no-op (`JdbcSource.scala:307`). The seed is
  always the operator-supplied `incremental.initial_value`
  (`JdbcSourceConfig.scala:377-378`).
- **However**, `INCREMENTAL` mode with a minimum seed is a working — and
  documented — substitute for the initial load: the shipped config uses
  `initial_value = "1900-01-01 00:00:00"` (`application.conf:242`; same in
  tests). That first run captures a real cutoff before the read
  (`JdbcSource.scala:72,215-240`), extracts everything under it, and
  commits the captured upper on success (`:324,336`). Partitioned
  extraction is fully available in INCREMENTAL mode
  (`JdbcSourceConfig.scala:103`; `JdbcSource.scala:120-162`), so the big
  first pull can be parallelized.
- **The real gaps:** (a) the substitute **silently excludes rows whose
  watermark column is NULL** — `col > literal` under SQL three-valued
  logic (`Watermarks.scala:58`) — rows a true `FULL_TABLE` load would have
  ingested; nothing validates watermark columns as NOT NULL; (b) note also
  that pipeline `--mode FULL` and `source.mode = FULL_TABLE` are orthogonal —
  `--mode FULL` only affects the curated publish path and raw flag
  (`IngestPipeline.scala:43`, `CuratedService.scala:75`); watermark
  behavior is driven purely by `source.mode`.

### C5. Upper bound is `MAX(watermark_column)`, not the source clock (§14, §5.2) — [CONFIRMED]

There is no `SYSUTCDATETIME()` / `GETUTCDATE()` capture anywhere. The upper
bound is `SELECT MAX([wm_col]) FROM …` executed on the driver before the
read (`JdbcSource.scala:215-240`). Implications:

- An idle-but-alive source cannot advance the watermark, and a zero-row
  window has no recordable extract boundary.
- The interval convention is `(lower, upper]` (lower-exclusive,
  upper-inclusive — `Watermarks.scala:34-42`), not the specified half-open
  `[start, end)`. It is internally consistent (consecutive windows abut
  with no gap or boundary duplication), but should be recorded as an
  accepted deviation if kept.

### C6. JDBC reruns duplicate raw data; the only dedup key is a constant (§10) — [CONFIRMED — understated]

- The cross-run raw idempotency guard applies only to managed file feeds —
  it is gated on staged files existing (`IngestPipeline.scala:304-306`,
  `RawIdempotency.scala:36-51`); JDBC always has `staged = None`.
- Rerunning a failed JDBC window **without** `--resume --run-id` mints a
  new UUID (`IngestPipeline.scala:40`), so the run-id replay guard never
  matches and the same window is appended to raw again. The configured
  overlap window additionally re-reads rows by design, so raw is
  at-least-once with no compensating key and no raw-duplicate
  reconciliation check.
- **Latent trap found in verification:** because `file_id` is a constant
  for JDBC rows (C2), any fix that simply relaxes the `staged.nonEmpty`
  gate would make `excludeLoadedFiles` drop **100% of every JDBC run after
  the first**. The idempotency fix requires a real raw dedup key (source
  PK + freshness column), not a gate change.

### C7. No concurrency control — concurrent runs erase each other's curated writes (§18) — [CONFIRMED — understated]

Exhaustive search found **zero** coordination primitives: no control-table
lock, lease, file lock, or CLI guard. Refutation attempts all failed:

- The watermark optimistic CAS (`WatermarkStore.scala:90-116`) fires
  **after every irreversible write** — RAW append (`IngestPipeline.scala:323`),
  curated `INSERT OVERWRITE` (`PublishService.scala:68`), file completion
  (`:177`) — so the losing run throws `JDBC_005` with duplicate raw rows
  already appended and curated already clobbered. Even a perfectly
  transactional watermark store would not prevent the data damage.
- `latestVersioned` duplicate detection (`WatermarkStore.scala:80-85`) only
  warns post-hoc; it aborts nothing.
- Worst case is the curated read-modify-write: two concurrent runs both
  snapshot the target (`CuratedService.scala:135,168`) and both
  `INSERT OVERWRITE` it in full — a **classic lost update: the second
  overwrite silently discards the first run's inserts entirely**, not
  merely interleaves them. The existing tests
  (`JdbcSourceH2Test.scala:190-200`, `IncrementalWindowFixesTest.scala:85-104`)
  assert only the watermark conflict, never the data damage.
- Any lock must be acquired **before extraction** and must also cover the
  `--stage curated` path (S13), which never touches the watermark store.

### C8. The watermark commits whenever the curated publish didn't happen — not only under `--stage raw` (§5.3) — [CONFIRMED, broadened]

`advanceWatermark` at `IngestPipeline.scala:181-188` is gated **only on
`!ctx.dryRun`**. It runs identically when:

- `--stage raw` is passed — curated is skipped (`IngestPipeline.scala:158-168`)
  yet the captured window is committed and gone forever;
- the feed has no `curated` block, or `curated.enabled = false` — both
  return `None` from the curated stage with only a log line
  (`CuratedStageRunner.scala:27-38`) and reach the same commit;
- reconciliation degrades quietly with `curated = None` (`:441-445` adds
  no curated check), so the gate cannot notice the missing stage.

This contradicts the framework's own documented contract in three places:
the comment at `IngestPipeline.scala:180` ("after everything above
succeeded"), `WatermarkAdvancing.scala:8-11` ("ONLY after the full run —
RAW write, CURATED publish, file completion — has succeeded"), and
`docs/OPERATIONS_RUNBOOK.md:85,254`. It is a defect, not a supported
topology. No test covers the interaction (the only `--stage raw` test is a
file feed asserting raw/audit state only, `PipelineIntegrationSpec.scala:383-402`).

### C9. Rejected rows are permanently lost from the incremental window (§10, §12) — [NEW — found in verification]

`RejectService.split` diverts rows to `ingest_rejects`
(`RejectService.scala:98-147`); only the accepted rows are written to RAW
(`IngestPipeline.scala:310-311,323`). The watermark then advances to the
**captured upper regardless** (`JdbcSource.scala:324`) — covering rows that
were never landed. With a bounded window plus the advance-only comparison
(`:326`), a rejected record **can never be re-extracted**. There is no
reject-replay mechanism and no watermark hold-back. Silent, permanent,
per-record data loss on a run that reports success.

### C10. `captureUpper` returning `None` silently degrades to an unbounded, non-reproducible window (§5.2) — [NEW — found in verification]

When the driver-side upper capture yields no usable value, the read falls
back to an **unbounded** `> lower` predicate (`JdbcSource.scala:77-80`),
and the commit falls back to `computeNext(accepted)` — a boundary derived
from whatever each Spark partition happened to read, at different
wall-clock times, against a live source (`JdbcSource.scala:324-326`). Rows
at or below that boundary that a partition never saw are skipped forever.

The code comment assumes `None` means "empty source", but `captureUpper`
also returns `None` whenever **any component of the captured row is NULL**
(`JdbcSource.scala:236-239`) — e.g. a nullable watermark column or a
composite watermark with a NULL later component. Additionally,
`computeNext` does `.na.drop("any")` (`Watermarks.scala:81-91`), and rows
with NULL watermark values are never selected by `col > lower` at all —
so NULL-watermark rows are silently never ingested and never reported
(compounds C4).

### C11. No volume guardrail before the curated overwrite (§11, §19) — [NEW — found in verification]

`PublishRequest.expectedCount` exists (`PublishService.scala:12`, checked
at `:86-91`) but is **never populated by any caller** and has no config
key — `CuratedService.scala:65-72` builds the request without it. The only
pre-overwrite guard is "not zero rows" (`:81-84`). A FULL run that
extracts 5% of the source (bad filter, wrong parameter, truncated
source-side view) passes validation and `INSERT OVERWRITE`s curated with
the truncated set — single-run total data loss in curated.

### C12. First run of an INCR feed takes the unfiltered `publishFull` path (§6.2, §6.3) — [NEW — found in verification]

`CuratedService.scala:75`:
`if (runMode.equalsIgnoreCase("FULL") || !spark.catalog.tableExists(fullTable)) publishFull(...)`.

`publishFull` (`:87-100`) applies **no** null-key filtering, **no**
deduplication, and skips the `require(keys.nonEmpty)` guard (`:139`) —
`filterNullKeys` and `deduplicate` each have exactly one caller, both
inside `publishIncremental` (`:151-152`). So the **initial load of an
incremental feed** seeds curated with duplicate and null business keys,
which every subsequent INCR run then perpetuates (the anti-join cannot
remove null-key target rows — see S3). The shipped integration test
codifies this as expected behavior (`IngestFlowIntegrationSpec.scala:182-183`
asserts a duplicated key survives a FULL publish).

---

## 3. Significant Findings (functional / operational)

### S1. No `record_hash` change detection (§7) — [CONFIRMED]

`record_hash` has zero occurrences in the repo. Every matched key is
rewritten every run and stamped `last_modified_op = 'U'`
(`CuratedService.scala:122`) regardless of whether any attribute changed —
expensive at ~350 columns, and it destroys "when did this row actually
change" semantics.

### S2. Incremental publish rewrites the entire curated table via a non-atomic swap (§15) — [CONFIRMED — understated]

Every incremental run stages the merged full dataset and executes
`INSERT OVERWRITE TABLE … SELECT *` (`PublishService.scala:38-78`),
rewriting all untouched rows — cost grows with total table size, not
increment size, and **empty increments are not skipped** (they still
rewrite everything). The "transactional publish" is transactional only up
to the swap: the final `INSERT OVERWRITE` on a non-ACID Hive/ORC table is
not atomic — a driver/executor failure mid-swap leaves curated truncated
or partially written, on *every* run. No Delta Lake / ACID `MERGE INTO`
support exists; targets are Hive tables in ORC (default) or Parquet only
(`HiveSink.scala:21`, `IngestMain.scala:74-77`).

### S3. Null-key handling: silent drops, and unbounded accumulation when drops are disabled (§6.2, §11) — [RESCOPED]

- `CuratedTransform.filterNullKeys` (`CuratedTransform.scala:54-61`) is a
  bare filter — no count, no reject write, no log. Defaults are on
  (`drop_null_keys = true`, `treat_blank_as_null = true`,
  `CuratedService.scala:147-148`). `CuratedResult` has no dropped-count
  field, and curated audit rows carry `source/accepted = -1`, so the
  ledger cannot even infer the loss by subtraction.
- **Caveat from verification:** `RejectService` *can* catch null keys
  upstream — its doc comment shows exactly that rule, and
  `use_contract_nullability = true` auto-generates null-or-blank predicates
  for non-nullable contract columns (`RejectService.scala:33-35,75-88`),
  with full lineage, counts, and thresholds. But nothing links reject rules
  to `curated.merge.keys` (pure operator discipline); keys nulled by
  `merge.normalize` **bypass the reject gate entirely** (normalization runs
  later, `CuratedService.scala:49`); and `CuratedStageRunner`-driven flows
  have no reject stage at all.
- **New defect found in verification:** with `drop_null_keys = false` (a
  documented option, `application.conf:279-282`), null-key rows
  **accumulate one more copy every run, forever**: the anti-join at
  `CuratedService.scala:168` uses `EqualTo` semantics, so `NULL = NULL` is
  never true — the null-key target row is always retained *and* the
  incoming null-key row is always appended. The same NULL semantics make
  `updateCount` misclassify those rows as inserts (`:163`).

### S4. Run-control ledger is partial and opt-in; without it `--resume` silently degrades (§10) — [CONFIRMED — understated]

`ingest_run_audit` (`AuditService.scala:34-49`) covers run id, entity,
stage, status, most counts, and error message — but:

- Run type (FULL/INCR) is in `RunContext` and **never persisted**; no
  explicit run start/end or watermark fields (watermarks live in a
  separate `ingest_watermarks` table).
- The entire audit subsystem is **opt-in**: without an `audit` config
  block every call is a logged no-op (`AuditService.scala:100-101,157-160`).
- **Verification finding:** with audit disabled, `stageStatus` returns
  `None` (`AuditService.scala:220-230`), so `--resume` **silently degrades
  into a full re-run** — no stage skipping, no replay guard — while the
  watermark still advances. Any dropped-row counts added per S3 also have
  nowhere to be persisted until audit is mandatory.

### S5. Contract lacks required mapping attributes; no contract generation (§2) — [CONFIRMED]

`ColumnContract` (`SchemaContract.scala:63-73`) supports: canonical/target
name, aliases (with governance dates), one data type, nullable, position,
required, default, structural category, per-column validation. Missing per
the mapping-specification table: source-vs-target data type pair,
per-column transformation rule, sensitive classification (PII/PHI),
business-key indicator, incremental-column indicator, and source
database/schema/table identity. Merge keys and watermark columns are
configured separately (`curated.merge.keys`,
`source.incremental.watermark_columns`), disconnected from the contract.

`ingestion-config-gen` cannot generate a contract from JDBC metadata (no
`DatabaseMetaData`/`INFORMATION_SCHEMA` introspection) or a spreadsheet.
A 350-column contract must be hand-authored; the `@file` column import
exists only in the **file** flow, and the JDBC wizard flow asks no
schema-contract questions at all (`JdbcQuestionFlow.scala:54-57`).

### S6. No timestamp/UTC standard — a prerequisite for the freshness merge (§14) — [CONFIRMED — elevated]

- No UTC normalization anywhere (`to_utc_timestamp`/`ZoneId`/`ZoneOffset`:
  zero hits); no `spark.sql.session.timeZone` (`IngestMain.scala:33` sets
  only case sensitivity); no JDBC `sessionInitStatement`.
- All technical timestamps come from `current_timestamp()` in the
  JVM/session zone (`RawMetadata.scala:21`, `CuratedTransform.scala:31-42`),
  while a source freshness column arrives in the source's zone. **A
  freshness-compared merge built without a zone convention yields wrong
  winners** (and DST-ambiguous partitions via
  `ingest_dt = date_format(current_timestamp(), …)`).
- Hazard: `ensureAudit` is add-if-missing, so a source column already named
  `last_modified_ts` passes through unstamped (`CuratedTransform.scala:36-38`)
  and would silently become the de-facto freshness field.
- `DATETIMEOFFSET(7)` precision is handled correctly, but only for
  watermark literals (`JdbcDialect.scala:58-104`).

### S7. Source consistency is not addressed (§4.3) — [CONFIRMED]

No snapshot isolation, RCSI, `applicationIntent`, or transaction-isolation
support. Arbitrary driver properties pass through
`source.connection_properties` (`JdbcSourceConfig.scala:271-294`), so
`applicationIntent=ReadOnly` *can* be supplied, but nothing sets,
validates, defaults, or documents it. Consistency is only logical: the
frozen window predicate ensures all partitions see the same range, but rows
updated mid-extraction are unprotected.

### S8. Retention and purging are absent (§17) — [CONFIRMED]

No retention, TTL, purge, or archive code for raw, reject, audit, registry,
or watermark data. The only deletion logic is a 24-hour cleanup of
crash-leftover curated **staging** tables (`PublishService.scala:107-125`).
`docs/OPERATIONS_RUNBOOK.md` documents retention as a manual operator task.

### S9. Curated partitioning/bucketing is absent (§6.6) — [CONFIRMED]

`Partitioning` is wired only into the raw `HiveSink`
(`HiveSink.scala:23-24,50-51`). `CuratedService`/`PublishService` never
partition, bucket, or cluster, and the curated path never enables dynamic
partitioning — a pre-created partitioned curated table would break the
`INSERT OVERWRITE`. Acceptable only if "no partitioning" is the chosen
strategy; there is no other option.

### S10. Empty-run behavior is inconsistent (§19) — [CONFIRMED]

- Empty **JDBC incremental** run: success; watermark advances via the
  captured upper when it exceeds the stored lower (`JdbcSource.scala:324-330`)
  — matches spec. But the merge is not skipped: the run still rewrites the
  entire curated table (S2).
- Empty **FULL** run: **fails** at publish (`PublishValidationException`)
  unless `curated.publish.allow_empty = true` (`PublishService.scala:81-84`;
  default false).
- Managed file feed with no files: succeeds as a SKIPPED no-op, but
  reconciliation and watermark advancement are both skipped
  (`IngestPipeline.scala:137-141`).
- Reconciliation mismatches default to **WARN** (`IngestPipeline.scala:452-459`),
  and the watermark advances anyway — the safety net is off by default and
  the watermark is committed over a known-inconsistent load.

### S11. Security gaps within an otherwise strong posture (§16) — [CONFIRMED]

Present and solid: CyberArk/Conjur/Azure Key Vault providers with defensive
registration and blank-secret rejection; Entra/managed-identity auth modes;
TLS enforced by default with parse-time rejection of downgrades unless
`allow_insecure_tls = true`; SQL logged only as a SHA-256 hash by default
with `log_sql` self-suppressing when secret-sourced parameters are present;
URL credential masking; sanitized vault errors. Gaps:

- No encryption at rest, no PII/PHI masking of data values, no RBAC.
- Reject rows persist the **full business record as unredacted JSON**
  (`RejectService.scala:122-136` — `to_json(struct(businessCols…))`), with
  no masking, column allow-list, or retention. Note the interaction with
  S3: routing null-key rows to quarantine sends *more* PHI into this table.
- Config-generator drafts/answer files store inline secrets in plaintext
  JSON (`Drafts.scala:19-45`).
- CyberArk over plain `http://` only warns (weaker than the AKV/Conjur
  hard gates).

### S12. Dedup fallback and validator coverage (§6.3) — [RESCOPED]

- `CuratedTransform.deduplicate` silently degrades to `dropDuplicates(keys)`
  (nondeterministic survivor) when the configured `order_by` columns are
  missing from the batch (`CuratedTransform.scala:65-66`); a *partial*
  order-by list silently narrows the ordering; per-column sort direction is
  not configurable (all DESC NULLS LAST).
- **Verification correction:** `CuratedContractValidator.scala:45-48` does
  raise a blocking violation for configured-but-missing dedup columns, in
  all modes — but only when a schema contract exists (`SchemaContract.parse`
  returns `None` for feeds with no `schema` block) *and* the modern call
  path passes it (`IngestPipeline.scala:163` does; the legacy
  `CuratedService.process` / `CuratedStageRunner.run` overloads hard-code
  `None`, `CuratedService.scala:31-35`, `CuratedStageRunner.scala:17-21`).
- The validator cannot close the most common hole: `dedup.order_by`
  **absent entirely** → zero violations → nondeterministic
  `dropDuplicates(keys)` among genuinely different rows. The shipped
  example config (`application.conf:195-197`) gives no guidance.
- Minor: `applyUpdateAudit` uses `dropDuplicates(keys)` on the target
  projection (`CuratedService.scala:119`), so with duplicate target keys
  (easy to produce via C12) the inherited `create_timestamp` is arbitrary.
  The all-null required-column check is skipped whenever any earlier
  violation fired (`CuratedContractValidator.scala:52`), so issues surface
  one run at a time.

### S13. `--stage curated` is an unaudited production write path — [NEW — found in verification]

The curated-only replay entry point (`IngestMain.scala:37-41,52-71`) is a
separate legacy path: `RawStageRunner.readFromRaw` selects a whole
`ingest_dt` partition — all runs of that day, filtered only by
`ingest_dt` + `file_type` (`RawStageRunner.scala:62-77`) — and publishes to
the real curated table under a synthetic
`RunContext(UUID.randomUUID(), "unknown", …)` (`CuratedService.scala:31-32`)
with **no AuditService, no reject split, no reconciliation, no contract
validation, no run-ledger row, and no watermark interaction**. Any
concurrency lock or reconciliation check added to `IngestPipeline` will not
reach this path unless explicitly wired.

### S14. Silent value corruption and column loss on curated schema drift (§13) — [NEW — found in verification]

- `align` and `castConfigured` use Spark `cast`, which returns **NULL
  instead of failing** when a value doesn't fit the target type
  (`CuratedTransform.scala:107-110,14-22`). At 350 columns, a single
  `varchar → decimal` drift nulls a column across the whole table.
- `align` projects strictly onto the target schema, so any new or renamed
  source column silently disappears from curated (no warning, no
  violation — `CuratedContractValidator` checks only for *missing* required
  columns, never unmapped extras). Raw has the same shape: `HiveSink`
  warn-and-drops new columns (`HiveSink.scala:35-40`). Behavior differs
  between FULL-on-first-create (column persisted) and INCR (column
  dropped).
- `schema.compatibility` (BACKWARD/…) is parsed but **decorative** — its
  only consumer is a log-message interpolation (`SchemaValidator.scala:281`).

### S15. Miscellaneous verified defects

- `advanceWatermark` re-parses the source config — re-resolving secrets —
  as the *last* step (`JdbcSource.scala:306`); a vault hiccup or expired
  token there fails the run after RAW and curated are committed, forcing a
  duplicate-window re-extract (compounds C6).
- Stale `readWindows` entries leak in a long-lived driver JVM on any
  failure between read and advance (`JdbcSource.scala:41,313-314`); on
  `--resume` in a reused JVM, a failed attempt's stale window (captured
  upper + stale version) is what gets committed.
- On the resume path, `computeNext` throws JDBC_004 if the watermark column
  is missing from the RAW slice (`Watermarks.scala:77-79`) — which is
  exactly what the C2 projection drop produces — so every resume fails at
  the final statement after all writes committed.
- Curated `insert/update/published` counts are approximations (key-set
  intersection; `publishedCount` is the whole staged-table count,
  `PublishService.scala:64,74`), and the `curated_inserts_plus_updates_covered`
  reconciliation check is a weak inequality that passes trivially
  (`IngestPipeline.scala:441-445`) — rows removed by `filterNullKeys` /
  `deduplicate` are invisible to all accounting.
- `KafkaSource` stores no offsets and is not `WatermarkAdvancing`
  (`KafkaSource.scala:12-38,73`) — every batch run re-ingests the whole
  configured offset range (same idempotency hole as C6 in a second
  connector; outside the SQL Server spec's direct scope).

### S16. Test coverage for merge semantics is effectively zero (§22) — [CONFIRMED]

`publishIncremental` has **no test at all** — no test anywhere runs a
pipeline or curated stage in INCR mode. Zero tests assert business-key
uniqueness after a merge; zero tests feed the same key across two runs in
any order (stale-update/late-arriving). What exists:
`CuratedUpdateAuditTest` (audit columns in isolation — its own fixture is
the exact shape of a stale overwrite, but it never asserts which value
wins), `CuratedTransformTest` (in-batch dedup and null-key filter units —
one test *codifies* the silent fallback as acceptable),
`AlignGuardTest` (alignment), `IngestFlowIntegrationSpec` (FULL mode, no
merge keys — asserts a duplicate key survives), `PipelineIntegrationSpec`
(publish rollback/resume mechanics, FULL mode). There is also no runtime
uniqueness assertion to test against — the only hook is the unshipped
`validation_query` (see roadmap item 9).

---

## 4. What the Framework Already Meets

| Capability | Evidence |
|---|---|
| Watermark commit as the final pipeline step, never on dry-run or failure (but see C8 for the curated-skipped hole) | `IngestPipeline.scala:181-188`; `WatermarkAdvancing.scala:8-19` |
| Versioned, append-only watermark history with optimistic CAS and conflict detection | `WatermarkStore.scala:60-117` |
| Bounded windows frozen on the driver before the read; consecutive windows abut with no gaps | `JdbcSource.scala:71-83,215-240`; `Watermarks.scala:34-42` |
| Overlap/lookback window (`previous_end − N`), advance-only guard prevents regression | `JdbcSourceConfig.scala:354`; `Watermarks.scala:53-55`; `JdbcSource.scala:326` |
| Unprotected-watermark guardrail (timestamp watermark without overlap/tie-breaker warns or fails) | `JdbcSourceConfig.scala:354-370` |
| Documented initial-load pattern: INCREMENTAL with min seed captures a real cutoff and commits it atomically, with partitioned extraction available | `application.conf:242`; `JdbcSource.scala:72,215-240,324` |
| Parallel extraction: static range, per-run MIN/MAX discovery, explicit predicates; fetch size; partition caps; skew warning | `JdbcSource.scala:120-211`; `JdbcSourceConfig.scala:198-254` |
| Metadata-driven query model: typed projections, whitelisted filter operators, parameterized templates (incl. `LAST_WATERMARK`/`UPPER_WATERMARK`/`RUN_ID`), SQL guardrails | `QueryModel.scala:138-298`; `QueryBuilder.scala:21-96` |
| Classified transient-only retry with jittered backoff (driver-side) | `RetryPolicy.scala:24-64`; `SqlFailureClassifier.scala:31-94` |
| Schema contracts: aliases with governance dates, positions, defaults, required/optional, per-column content validation | `SchemaContract.scala:63-73,196-220,344-377` |
| Drift policy engine FAIL/WARN/IGNORE per drift type; stable HDR_001–HDR_018 error catalog; validation before the raw write | `SchemaValidator.scala`; `IngestPipeline.scala:261-291` |
| Name-first column mapping; positional mapping only via guarded, audited fallback | `SchemaContract.scala:27-43`; `FileSource.scala:75-127` |
| Schema version persisted as a table property and compared before load | `SchemaVersions.scala:9-38`; `IngestPipeline.scala:283,325` |
| Record-level rejects with full lineage + thresholds + file-level quarantine (but see C9 for the watermark interaction and S11 for redaction) | `RejectService.scala:98-196`; `FileIntakeService.scala` |
| Resume support: `--resume --run-id`, per-stage skip, raw/reject double-append guards, run-scoped curated replay (contingent on audit enabled — S4 — and `run_id` in raw — C2) | `IngestPipeline.scala:197-215,315-321,398-407` |
| Secrets: CyberArk CCP, Conjur, Azure Key Vault, env/file/sysprop; Entra auth modes | `SecretProvider.scala:57-124`; `JdbcAuthenticationProvider.scala` |
| TLS enforced for SQL Server; downgrades rejected at parse without explicit opt-in | `JdbcDialect.scala:110-115`; `JdbcSourceConfig.scala:271-294` |
| Log hygiene: query hashing, `log_sql` secret guard, URL masking, sanitized vault errors, regression tests | `JdbcSource.scala:243-266`; `JdbcHealthCheck.scala:74-76`; `JdbcLogSanitizationTest.scala` |
| Staged curated publish via per-run staging table + validation before the swap (swap itself non-atomic — S2) | `PublishService.scala:37-110` |

---

## 5. Requirement-by-Requirement Scorecard

| § | Requirement | Verdict | Key gaps |
|---|---|---|---|
| 2 | Source-to-raw column mapping | **Partial** | No source/target type pair, transformation rule, PII tag, business-key or incremental indicators; no contract generation (S5) |
| 3.1 | Raw append-only, traceable, reprocessable | **Partial** | Append ✓ (first-run bootstrap is a real Overwrite); traceability contingent on `run_id` in target DDL, and JDBC rows carry no source lineage (C2) |
| 3.2 | Raw technical metadata | **Not met** | 13 of 14 columns absent; existing columns empty/constant for JDBC (C2) |
| 3.3 | Raw partitioning by ingestion date | **Partial** | Fully config-driven with derive expressions; shipped examples use ingest date; no framework default or guardrail |
| 4.1–4.2 | Initial load + cutoff watermark seed | **Partial** | INCREMENTAL-with-min-seed is a documented working substitute, but it drops NULL-watermark rows and there is no supported FULL→INCR handoff (C4) |
| 4.3 | Source consistency | **Not met** | No isolation support; passthrough only (S7) |
| 4.4 | Large-table batched extraction | **Largely met** | Range/predicate partitioning ✓; hash-based absent; predicate disjointness unchecked |
| 5.1–5.2 | Incremental column + boundaries | **Largely met** | Bounded windows ✓; upper = MAX(col) not source clock; `(lower, upper]` not `[start, end)` (C5); unbounded fallback on NULL capture (C10) |
| 5.3 | Watermark update after full success | **Partial** | Commit is last ✓, but fires even when curated never ran (C8) and over rejected rows (C9) |
| 5.4 | Overlap window | **Met** | Lower edge only, per spec |
| 6.1–6.2 | Curated purpose + business keys | **Partial** | Composite keys, normalization ✓; null keys silently dropped or accumulate unbounded (S3); first INCR run unguarded (C12) |
| 6.3 | Latest-record selection + tie-breakers | **Partial** | In-batch row_number with multi-column ordering ✓; nondeterministic fallback when order_by unset (S12); INCR only (C12) |
| 6.4 | Merge behavior (freshness-compared upsert) | **Not met** | Unconditional replace; no freshness predicate; audit disguises stale overwrites (C1) |
| 6.5 | Curated technical metadata | **Partial** | `create_timestamp`/`last_modified_ts`/`last_modified_op` ✓; no run id, `is_deleted`, `record_hash`; add-if-missing passthrough hazard (S6) |
| 6.6 | Curated partitioning strategy | **Not met** | No curated partitioning/bucketing (S9) |
| 7 | Change detection (`record_hash`) | **Not met** | Absent (S1) |
| 8 | Source deletes | **Not met** | Absent; `deleteCount` hard-coded 0 (C3) |
| 9 | Late-arriving / backfilled data | **Not met** | Consequence of C1 |
| 10 | Idempotency, restartability, run control | **Partial** | Strong for file feeds; JDBC rerun duplication with constant `file_id` (C6); rejected rows unrecoverable (C9); ledger partial, opt-in, resume degrades without it (S4) |
| 11 | Audit and reconciliation | **Partial** | 3 checks; no raw-dup, curated-uniqueness, watermark-continuity, or null-key checks; WARN default; no volume guardrail (C11, S10) |
| 12 | Reject and quarantine | **Largely met** | Record + file level, lineage, thresholds ✓; but rejected rows lost from the window (C9) and payloads unredacted (S11) |
| 13 | Schema evolution | **Partial** | Policies, versioning, name-based mapping ✓; compatibility decorative; cast-to-null corruption and silent column loss in curated (S14) |
| 14 | Timestamp standards | **Not met** | No UTC normalization, session timezone, or source-clock boundary (C5, S6) |
| 15 | Performance | **Partial** | Extraction tuning ✓; non-atomic full-table rewrite every run (S2); no hash partitioning |
| 16 | Security and compliance | **Largely met** | Secrets/TLS/log hygiene ✓; no at-rest encryption, PII masking, RBAC; reject payloads unredacted (S11) |
| 17 | Retention and purging | **Not met** | Absent (S8) |
| 18 | Concurrency control | **Not met** | No locking; concurrent runs lose data, CAS fires after irreversible writes (C7) |
| 19 | Empty-run behavior | **Partial** | JDBC incremental ✓; empty FULL fails by default; file no-op skips watermark/reconcile; merge not skipped (S10) |
| 20 | End-to-end flow | **Partial** | Incremental flow largely matches; initial-load steps 3/13 (cutoff capture/commit) only via the INCREMENTAL substitute; `--stage curated` path bypasses the flow entirely (S13) |
| 22 | Acceptance criteria | **Fails today** on: late-arriving protection, deterministic cross-run winner, raw metadata, delete strategy, concurrent-run protection, count capture (null-key drops uncounted, rejected rows unrecoverable) |

---

## 6. Verified Remediation Backlog (priority order)

Grouped by priority. P0 items are what the §22 acceptance criteria actually
test, plus the silent-data-loss paths found in verification. Scope notes
incorporate the verification corrections — several items were rescoped from
the original draft list.

### P0 — correctness / silent data loss

| # | Work item | Addresses | Scope notes (verified) |
|---|---|---|---|
| 1 | **Preserve a dedicated source freshness/version field in Raw and Curated** | C1, C2, §3.2, §6.5 | Must be contract-designated (not inferred), persisted in both layers, and paired with a timezone convention (item 10) and a deterministic tie-breaker. Beware the `ensureAudit` add-if-missing passthrough (S6): a source column named `last_modified_ts` silently becomes the field today. |
| 2 | **Freshness-compared Curated upsert** | C1, §6.3–6.4, §9 | Union target-matches with incoming, apply the dedup window across **both** sides (source freshness DESC + tie-breakers + ingestion ts DESC), keep row 1 per key. Must also fix the audit stamping so a losing (stale) incoming row does not flip the target to `'U'`. Unsound without items 1 and 10. |
| 3 | **Deduplicate and null-key-filter FULL and first-INCR-run publishes; make dedup fallback fail instead of degrade** | C12, S12, §6.3 | The `publishFull` path is taken by FULL mode *and* the first run of every INCR feed (`CuratedService.scala:75`). Also: fail (don't silently `dropDuplicates`) when `order_by` columns are missing or unset; the contract validator only covers feeds that have a contract and use the modern call path. |
| 4 | **Quarantine and count null business keys** | S3, §6.2, §11 | Route drops through `RejectService` (post-normalization — keys nulled by `merge.normalize` bypass the reject gate today), persist a dropped count in the ledger (requires item 8), and fix the NULL-anti-join accumulation bug when `drop_null_keys = false`. Note S11: quarantining routes more PHI into the unredacted reject table — pair with redaction. |
| 5 | **Source-clock cutoff for both FULL and INCR; NOT-NULL watermark validation; supported FULL→INCR handoff** | C4, C5, C10, §4.2, §14 | Rescoped from "capture a full-load cutoff": capture `SYSUTCDATETIME()` (dialect-specific) as the upper bound; validate/require watermark columns NOT NULL (NULL rows are silently never ingested); remove or fail the unbounded `captureUpper = None` fallback instead of committing a partition-dependent boundary; document the `(lower, upper]` vs `[start, end)` convention decision. |
| 6 | **Prevent concurrent runs of the same entity** | C7, §18 | Lease-style lock acquired **before extraction** (the watermark CAS fires after all irreversible writes and cannot protect data), released after watermark commit — and it must also cover the `--stage curated` path (S13), which never touches the watermark store. |
| 7 | **Make required Raw lineage columns mandatory *and populated*** | C2, §3.2 | Rescoped: enforcing presence of `run_id` (fail or `ALTER TABLE ADD COLUMNS` instead of warn-and-drop in `HiveSink`) is necessary but insufficient — for JDBC, `source_file` is empty and `file_id` is a constant. Add and populate source identity (system/database/schema/table), the extract window (`extract_start_ts`/`extract_end_ts`), and optionally `record_hash`. Update the reference DDL and add a test for the pre-created-DDL path (currently zero coverage). |
| 8 | **Commit the watermark only when the curated publish actually succeeded; never over rejected rows** | C8, C9, §5.3, §10 | Rescoped from "`--stage raw` must not commit": the flag is one of three routes (`--stage raw`, `curated.enabled=false`, missing `curated` block) to the same unguarded commit. Separately decide the rejected-row policy: hold the watermark back, or provide reject-replay — today rejected records are permanently unrecoverable on a "successful" run. |
| 9 | **Curated business-key uniqueness assertion + stale-update tests** | S16, §11, §22 | Tests need an enforcement point: add a built-in post-publish uniqueness check (not just the unshipped `validation_query` hook), then INCR-mode integration tests: same key across two runs in both orders, null-key accounting, first-run dedup, concurrent-run behavior. `publishIncremental` currently has zero tests. |

### P1 — data-integrity guardrails

| # | Work item | Addresses |
|---|---|---|
| 10 | **Timezone/UTC standard**: set `spark.sql.session.timeZone=UTC`, define source-offset handling, store technical timestamps in UTC. Prerequisite for items 1–2. | S6, §14 |
| 11 | **Publish volume guardrail**: wire `expectedCount` (exists, never populated) plus a configurable delta threshold before the curated overwrite; skip the merge entirely on zero-row increments. | C11, S2, §19 |
| 12 | **Raw idempotency key + reconciliation checks**: configurable raw dedup key (source PK + freshness column) and raw-duplicate check; curated uniqueness check (item 9); watermark-continuity check; replace the trivially-true `inserts+updates <= published` inequality with real source→curated accounting. Do **not** fix C6 by relaxing the `staged.nonEmpty` gate — the constant `file_id` would then drop 100% of every JDBC run. | C6, S15, §10–11 |
| 13 | **Make the run ledger mandatory and complete**: fail loudly (or default-on) when `audit` is unconfigured — today `--resume` silently degrades to a full re-run without it; persist run mode, start/end, and watermark bounds; default `reconciliation.on_mismatch` to FAIL. | S4, S10, §10–11 |
| 14 | **Retire or harden the `--stage curated` legacy path**: run-scoped raw selection, real run identity, audit/reconciliation/contract validation, covered by the item-6 lock. | S13 |
| 15 | **Schema-drift hardening in curated**: fail (or reject-route) on cast-to-null; detect and policy-gate unmapped incoming columns instead of silently dropping; enforce or remove the decorative `compatibility` mode. | S14, §13 |

### P2 — functional completeness / operational

| # | Work item | Addresses |
|---|---|---|
| 16 | **Delete strategy** (soft-delete column mapping → `is_deleted`, or periodic key reconciliation; "ignore deletes" only as an explicit documented decision) | C3, §8 |
| 17 | **`record_hash` change detection** over normalized contract business columns; skip unchanged rows | S1, §7 |
| 18 | **Contract mapping attributes + generation**: source type, transform, sensitivity, business-key and incremental flags on `ColumnContract`; derive `merge.keys`/watermark columns from the contract; JDBC `INFORMATION_SCHEMA` introspection in `ingestion-config-gen` for 350-column bootstrap | S5, §2 |
| 19 | **Atomic curated publish**: evaluate Delta/Iceberg for true MERGE (fixes the non-atomic `INSERT OVERWRITE` and the full-table rewrite cost in one move) | S2, §15 |
| 20 | **Retention/purge jobs** for raw, rejects, audit, registry, watermark history; reject-payload redaction and access restriction | S8, S11, §16–17 |
| 21 | **Misc verified defects**: resolve `advanceWatermark` config re-parse (cache the parsed config from read time); clear stale `readWindows` on failure; curated partitioning support or explicit "none"; Kafka offset persistence if Kafka feeds are in scope | S15, S9 |

---

## 7. Open Decisions Required from the Business (per §21)

These cannot be resolved by the framework and block a complete design:
exact business key and null-key policy; the reliable incremental column,
whether it updates on every insert/update, and **whether it is nullable**
(NULL watermark rows are silently excluded today — C4/C10); timestamp
precision and tie-breaker field; whether the source physically deletes and
the required curated representation; whether rejected records must be
recoverable (they are permanently lost from the window today — C9);
expected volumes and initial-load duration; raw/curated/reject retention
periods; target technology confirmation (current framework supports Hive +
ORC/Parquet only — a Delta Lake requirement would be a significant change);
and whether invalid records fail the run or quarantine (the framework
supports both, per feed).
