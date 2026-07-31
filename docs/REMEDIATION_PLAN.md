# Remediation Implementation Plan

**Date:** 2026-07-28
**Input:** `docs/REQUIREMENTS_GAP_ANALYSIS.md` (verified revision) — findings
C1–C12, S1–S16, backlog items 1–21.

> **Implementation status (2026-07-28):** Phase 0 and all of Phase 1
> (0.1–0.2, 1.1–1.8) plus 2.1 are **IMPLEMENTED** and covered by tests
> (CuratedMergeIntegrationSpec, LockServiceSpec, HiveSinkPreCreatedDdlTest,
> new JdbcSourceH2Test/JdbcPipelineIntegrationSpec cases). Notable deltas
> from the plan as written: the shrink guard defaults on for INCR only
> (FULL replaces opt in via `curated.publish.max_shrink_percent`); the lock
> auto-enables when an `audit.database` exists rather than being universally
> REQUIRED (feeds with neither block get a loud warning); `--force-unlock`
> is a `LockService.forceRelease` API + manual RELEASE row, not yet a CLI
> flag; per-column dedup sort direction remains all-DESC.
>
> **Phase 2 implemented (2026-07-29):** 2.2 (window-based JDBC replay guard,
> `raw.idempotency_key` + `raw_duplicate_versions` check; watermark
> continuity is structural — the lower bound is read from the store — and
> per-run windows are now persisted in the ledger for audit), 2.3
> (mandatory ledger with explicit `audit.enabled=false` opt-out, `--resume`
> aborts without a ledger, `run_mode`/`window_start`/`window_end` persisted
> with in-place ALTER migration, `reconciliation.on_mismatch` default
> WARN→FAIL), 2.4 (CUR_002 cast-to-null guard with `curated.on_cast_error`,
> dropped-column policy surfacing, BACKWARD compatibility enforced via the
> `ingest.schema.required` snapshot), 2.5 (`--stage curated` reimplemented
> as `IngestPipeline.curatedReplay` — governed, locked, audited, never the
> watermark; synthetic-RunContext overloads deleted), 2.6
> (`rejects.payload = FULL|HASHED|KEYS_ONLY` with a loud FULL warning;
> drafts exclude secret answers and are written 0600).
>
> **Remote-framework adoption (2026-07-29):** `FeedCompatibilityValidator`
> (CFG_001..009) is wired into pipeline startup (runInternal and
> curatedReplay) and the config generator's dry run; and Phase 3 item #17
> is implemented — `RecordHash` (hash recipe identical to
> `raw.RawMetadataStamper`: case-insensitively sorted business columns,
> U+0001 separator, U+0000 null marker, SHA-256) stamps RAW rows when
> `raw.record_hash = true` and the freshness merge skips rewriting (and
> restamping) rows whose key+hash already match the target. Phase 3
> otherwise remains open, **except** item 3.3/backlog #18's tooling half
> (2026-07-29): the config generator now emits the schema contract as a
> separate `<entity>-schema.conf` via HOCON `include required(...)`, the
> JDBC flow gained the contract questions, and `JdbcSchemaIntrospector`
> bootstraps wide mappings from `DatabaseMetaData`/INFORMATION_SCHEMA.
>
> **Phase 3 implemented (2026-07-30) except the Delta/Iceberg spike:**
> #16 soft-delete strategy (`curated.merge.deletes = IGNORE | SOFT` with
> indicator column/values; tombstones compete by freshness, stamped
> op='D'/is_deleted; stale records cannot resurrect a newer delete;
> RECONCILE remains future work); #17 record_hash (done earlier); #18
> `ColumnContract` gains source_type/transform/sensitivity/business_key/
> incremental — merge keys and watermark columns derive from the contract
> (explicit config wins, contradictions are HDR_017), contract transforms
> (TRIM/UPPER/LOWER/{col} expressions) run at the curated stage, and
> `rejects.payload = MASKED` hashes only sensitivity-tagged columns; #20
> `RetentionService` via `--stage retention` (raw ingest_dt partition
> drops, staged-rewrite purges for reject/audit tables, watermark
> keep-last-N, dry-run); #21 advanceWatermark reuses the read-time parsed
> config (no last-step secret re-resolution), failed runs discard their
> read windows, and `curated.partitioning` is rejected explicitly.
> **#19 (Delta/Iceberg atomic publish) remains open, gated on the target-
> technology business decision** — the staged INSERT OVERWRITE stays the
> publish mechanism until then.
**Goal:** close the P0 correctness gaps first, then the P1 guardrails, with
every phase leaving the framework releasable (no phase depends on a later
one to be safe). File/line references are to current `main`.

Effort scale: **S** ≤ 1 day, **M** = 2–4 days, **L** = 1–2 weeks.

---

## Phase 0 — Foundations (prerequisites for the merge work)

Nothing in Phase 1 is sound without these two conventions. Both are small.

### 0.1 UTC timestamp standard (backlog #10) — S

- `IngestMain.scala:33` — add `spark.sql.session.timeZone = UTC` (and
  `spark.sql.datetime.java8API.enabled = true`) next to the existing
  `caseSensitive` setting. Make it overridable via
  `app.spark.session_time_zone` for feeds that genuinely need local time,
  defaulting to UTC.
- Document the convention in `docs/ARCHITECTURE.md`: all framework-stamped
  timestamps (`load_timestamp`, `create_timestamp`, `last_modified_ts`,
  `ingest_dt` derivations) are UTC; the source freshness column is compared
  as an instant (no conversion needed for `datetimeoffset`; plain
  `datetime` sources must declare their zone — see 0.2).
- **Migration note:** existing tables stamped in local server time will
  show a one-time discontinuity in `load_timestamp`/`ingest_dt` at rollout.
  Call this out in the release notes; do not backfill.
- Tests: assert session zone in a small `IngestMainSpec`; assert
  `ingest_dt` derivation is stable across a DST boundary.

### 0.2 Freshness column designation (backlog #1, config half) — S

Add the *convention*; the plumbing lands in 1.1/1.2.

- New config block under `curated.merge`:

  ```hocon
  merge {
    keys            = ["member_id"]
    freshness {
      column     = "source_last_modified_ts"   # required for INCR once flag day passes
      tie_breakers = ["source_change_seq", "source_pk"]  # optional, compared DESC after column
      source_zone  = "UTC"                     # for plain datetime sources; default UTC
    }
  }
  ```

- Parse in `CuratedService` next to the existing `merge.*` reads
  (`CuratedService.scala:136-149`). For the transition, absent
  `freshness.column` logs a WARN and falls back to current behavior;
  a follow-up release makes it required for INCR (like `keys` is today,
  `:139`).
- Guard against the S6 hazard: if the configured freshness column collides
  with a framework audit column name (`last_modified_ts` etc.), fail at
  parse — `ensureAudit`'s add-if-missing behavior
  (`CuratedTransform.scala:31-42`) must never silently adopt a source
  column. Rename the framework columns' collision check into
  `CuratedContractValidator`.

---

## Phase 1 — P0 correctness (backlog #1–#9)

### 1.1 Raw lineage metadata: mandatory *and populated* (backlog #7, C2) — L

**Populate.** Extend `RawMetadata.add` (`RawMetadata.scala:9-24`) with a
typed context instead of growing positional args:

```scala
final case class RawLineage(
  runId: String,
  sourceSystem: String, sourceDatabase: String,
  sourceSchema: String, sourceTable: String,
  extractStart: Option[String], extractEnd: Option[String],
  freshnessColumn: Option[String]
)
def add(df: DataFrame, flag: String, lineage: RawLineage): DataFrame
```

- `source_system` — new required key `source.system` (feed-level; the
  config generator should prompt for it). `source_database/schema/table` —
  parse from the JDBC config (`JdbcSourceConfig` already validates
  table/schema identifiers); empty strings for file/Kafka feeds.
- `extract_start_ts` / `extract_end_ts` — from the JDBC `ReadWindow`. The
  window currently lives only in `JdbcSource.readWindows`
  (`JdbcSource.scala:41,309-314`); expose it on the source result instead:
  add `def lastWindow(entity: String, runId: String): Option[(String, String)]`
  to the `WatermarkAdvancing` trait (`WatermarkAdvancing.scala`) and have
  `IngestPipeline.runRaw` fetch it after the read, before
  `RawMetadata.add` (`IngestPipeline.scala:293`). This also removes one
  consumer of the fragile static map (S15).
- `source_last_modified_ts` — when `freshness.column` is configured, copy
  it under the canonical name (`withColumn("source_last_modified_ts",
  col(actual))`) so raw always carries the freshness field under one name
  regardless of source column naming.

**Enforce.** `HiveSink.scala:35-40` currently warn-and-drops extras:

- Split the check: business columns absent from the target keep the WARN
  (drift policy owns that decision — see 2.6), but a **framework metadata
  column** absent from the target becomes an error. Maintain the list in
  `RawMetadata.RequiredColumns`.
- Remediation path instead of hard failure: new config
  `raw.metadata_columns = FAIL | ALTER | WARN` (default `FAIL`). `ALTER`
  issues `ALTER TABLE … ADD COLUMNS (…)` for the missing metadata columns
  (safe on Hive external ORC tables — appended at end, pre-existing files
  read NULL).
- Update `ddl/health_sherpa_member.sql` and `README.md:51` to the full
  column set, including `run_id`.

**Tests.** New `HiveSinkPreCreatedDdlTest`: pre-create the raw table from
the shipped DDL (the path with zero coverage today), assert FAIL and ALTER
modes; assert JDBC rows carry populated source identity and window; assert
`--resume` works against a pre-created table (C2's `AnalysisException`).

### 1.2 Freshness-compared upsert (backlog #2, C1) — L

Rework `publishIncremental` (`CuratedService.scala:128-177`):

```scala
val incoming  = deduplicate(filterNullKeys(normalized), keys, ordering)   // in-batch winners
val matched   = target.join(incomingKeys, keys, "left_semi")              // target rows being challenged
val unchanged = target.join(incomingKeys, keys, "left_anti")              // untouched target rows
val contested = matched.select(alignedCols).unionByName(incoming.select(alignedCols))
val winners   = deduplicate(contested, keys, ordering)                    // cross-run freshness decision
val merged    = unchanged.unionByName(winners)
```

where `ordering = freshness.column DESC :: tie_breakers DESC ::: ingestion-ts DESC`.

Details:

- `deduplicate` gains an ordering-spec parameter (per-column direction —
  fixes the all-DESC limitation in `CuratedTransform.scala:69`) and
  **fails** when a configured ordering column is missing instead of
  degrading (see 1.3).
- The target side must expose the freshness column: it does after 1.1
  (raw carries `source_last_modified_ts` and curated aligns to a target
  schema that includes it). Add the column to the curated DDL; during
  transition, a target lacking the column triggers the WARN-fallback path
  from 0.2, never a crash.
- **Audit stamping fix (C1 aggravation):** compute `insert/update` from the
  *winners*, not the incoming batch: a key whose winner came from the
  target side is neither an insert nor an update — count it as `ignored`
  (new field on `CuratedResult`, `CuratedService.scala:15-20`) and do not
  restamp `last_modified_ts`/`'U'` on it. `applyUpdateAudit`
  (`:109-126`) moves after winner selection and only touches keys whose
  winner is the incoming row.
- Keep the staging + validate + `INSERT OVERWRITE` publish mechanics
  unchanged in this phase (atomicity is Phase 3, backlog #19).

### 1.3 Deterministic dedup everywhere (backlog #3, C12, S12) — M

- `CuratedService.scala:74-78` — hoist the hygiene out of the branch:
  run `normalizeKeys → filterNullKeys → deduplicate` **before** choosing
  `publishFull` vs `publishIncremental`, whenever `merge.keys` is
  configured (FULL mode, first-INCR-run table-creation path included).
  Feeds with no keys (true snapshot replaces) skip it, unchanged.
- `CuratedTransform.deduplicate` (`CuratedTransform.scala:63-74`):
  - configured-but-missing ordering columns → throw
    `IllegalStateException` (HDR_019, new catalog entry) instead of the
    silent `flatMap` narrowing / `dropDuplicates` fallback;
  - `order_by` empty **with keys configured** → parse-time failure with a
    clear message ("dedup requires an ordering to be deterministic; set
    curated.dedup.order_by or curated.merge.freshness.column"). The
    freshness config from 0.2 supplies the default ordering, so most feeds
    fix this by adopting 0.2.
- Update `IngestFlowIntegrationSpec.scala:182-183` (currently codifies the
  duplicate-survives behavior) to the new expectation.

### 1.4 Null-key quarantine and counting (backlog #4, S3) — M

- `filterNullKeys` returns both sides:
  `def splitNullKeys(df, keys, blanks): (valid: DataFrame, dropped: DataFrame)`.
- Route `dropped` to `RejectService.persist` with
  `error_code = "CUR_001"`, `reject_category = "null_business_key"` — this
  runs **post-normalization**, closing the `merge.normalize` bypass. Reuse
  the existing reject table and lineage columns
  (`RejectService.scala:122-136`); pair with reject-payload redaction
  (Phase 2.7) before enabling on PHI feeds.
- Add `nullKeyCount` (and `ignoredCount` from 1.2) to `CuratedResult` and
  thread into `StageCounts` (`IngestPipeline.scala:164-166`) so the ledger
  can reconcile: `accepted = curated_written + ignored + null_key + deduped`.
- **Fix the accumulation bug:** with `drop_null_keys = false`, the
  anti-join must be null-safe — build the join condition with `<=>`
  instead of the `Seq[String]` overload at `CuratedService.scala:168` so a
  null-key target row is matched (and replaced), not duplicated forever.

### 1.5 Source-clock cutoff, NOT-NULL watermarks, bounded-window integrity (backlog #5, C4/C5/C10) — L

- **Dialect clock:** add `def currentTimestampSql: String` to `JdbcDialect`
  (`SELECT SYSUTCDATETIME()` for SQL Server, `now()`/`CURRENT_TIMESTAMP`
  for others). New config `incremental.upper_bound = SOURCE_CLOCK | MAX_VALUE`
  (default `MAX_VALUE` for backward compatibility; flip the shipped
  examples and generator to `SOURCE_CLOCK`). `SOURCE_CLOCK` is only valid
  for single TIMESTAMP/DATETIMEOFFSET watermarks; validated at parse.
  `captureUpper` (`JdbcSource.scala:215-240`) gains the clock branch via
  the existing `DriverQueries.firstRow` machinery.
- **Kill the unbounded fallback (C10):** `captureUpper = None` in
  `SOURCE_CLOCK` mode is a driver failure → retryable error, never a
  fallback. In `MAX_VALUE` mode, distinguish "table empty" (count query,
  cheap) from "MAX returned NULL with rows present" — the latter fails
  with a new `JDBC_006` (nullable watermark data) instead of degrading to
  the unbounded `> lower` predicate at `JdbcSource.scala:77-80`. Remove
  the `computeNext` commit fallback for windowed reads (`:324-326` keeps
  the captured upper only); `computeNext` stays solely for the resume path.
- **NOT-NULL validation:** at first run per entity (and on `--validate`),
  probe `SELECT COUNT(*) WHERE wm_col IS NULL`; non-zero →
  fail with JDBC_006 guidance (add a filter, fix the source, or accept the
  loss explicitly via `incremental.allow_null_watermark = true`, which
  logs the excluded count every run).
- **FULL→INCR handoff (C4):** allow the `incremental { … }` block alongside
  `mode = FULL_TABLE`. When present: capture the cutoff (per
  `upper_bound`) before extraction, run the unfiltered full read, and on
  success seed the watermark store with the cutoff via the existing
  `recordIfVersion`. `advanceWatermark`'s `cfg.watermark.foreach` gate
  (`JdbcSource.scala:307`) changes to cover this seeding path. Document
  the interval convention decision: keep `(lower, upper]` (code reality),
  note the deviation from the spec's `[start, end)` in the ops runbook.

### 1.6 Entity-level run lock (backlog #6, C7) — L

- New `LockService` in `ingestion-core` with a Hive-backed lease table
  `ingest_run_locks(entity, holder_run_id, acquired_ts, lease_until, released)`
  — same append-only + read-latest pattern as `WatermarkStore` (it is the
  proven Hive-compatible idiom in this codebase, `WatermarkStore.scala:60-117`),
  with the same documented best-effort caveat, plus:
  - acquire = append claim row, wait a settle interval (config, default
    5 s), re-read latest; if another live claim is newest, back off/abort
    with a new `PIPE_001` error. This closes the same-instant race to a
    window far smaller than a full pipeline run (today: the entire run).
  - lease TTL (default 4 h, config) so a crashed run doesn't wedge the
    entity; `--force-unlock` CLI escape hatch.
  - pluggable store (`type = hive | jdbc`) so sites with a real RDBMS can
    get true row-lock semantics later without API change.
- Acquire in `IngestPipeline.run` **before the source read** (before
  `IngestPipeline.scala:229`); release in a `finally` after the watermark
  commit. Heartbeat-renew between stages for long runs.
- Wire the same acquire into `IngestMain.runCuratedOnly`
  (`IngestMain.scala:52-71`) so the legacy path is covered (full retirement
  of that path is 2.5).
- Config: `concurrency.lock = REQUIRED | OFF` (default `REQUIRED`;
  `OFF` logs a loud warning for feeds that genuinely want parallel runs).
- Tests: two interleaved pipelines on one entity (H2 + local Hive
  metastore, same harness as `JdbcPipelineIntegrationSpec`) — second run
  aborts with PIPE_001; lease expiry allows takeover; `--force-unlock`.

### 1.7 Watermark gating (backlog #8, C8/C9) — M

- `IngestPipeline.scala:181-188`: advance only when
  `curatedResult.isDefined` **or** the feed explicitly declares
  `watermark.advance_after = RAW` (new config for genuinely raw-only
  topologies; default `CURATED`). `--stage raw`, `curated.enabled=false`,
  and a missing `curated` block all stop advancing by default — matching
  the documented contract in `WatermarkAdvancing.scala:8-11` and the
  runbook.
- **Rejected rows (C9):** default policy stays "advance" but the loss stops
  being silent and unrecoverable:
  - `rejects.on_reject_watermark = ADVANCE | HOLD` (default `ADVANCE`).
    `HOLD` fails the run before the watermark commit when
    `rejectedCount > 0` (operator fixes and re-runs the window).
  - For `ADVANCE`: add a `replay` entry point that re-emits rows from
    `ingest_rejects` (the full record is already stored as JSON,
    `RejectService.scala:131`) through the raw→curated path under a new
    run id, marking the reject row's `reprocessing status`. This is the
    §12 "reprocessing status" requirement and makes C9's loss recoverable.
  - Log + audit the rejected-row count against the committed window either
    way (reconciliation row `rejected_in_window`).

### 1.8 Uniqueness assertion + merge test suite (backlog #9, S16) — M

- Built-in post-stage check in `PublishService.validate`
  (`PublishService.scala:80-101`): when merge keys are configured, run
  `SELECT keys, COUNT(*) FROM staging GROUP BY keys HAVING COUNT(*) > 1 LIMIT 10`
  and fail the publish with the offending keys in the message. Config
  `curated.publish.enforce_unique_keys = true` (default true when keys
  exist). This is the enforcement point item 9's tests assert against.
- New `CuratedMergeIntegrationSpec` (ingestion-app, local Hive):
  1. same key, newer-then-older across two INCR runs → older ignored,
     `ignoredCount = 1`, audit not restamped;
  2. older-then-newer → newer wins;
  3. equal freshness → tie-breakers decide, deterministic across reruns;
  4. first INCR run (table absent) deduplicates and null-filters (C12);
  5. null-key rows land in `ingest_rejects` with CUR_001 and are counted;
  6. uniqueness check trips on a hand-corrupted staging table;
  7. `--stage raw` does not advance the watermark; `advance_after = RAW`
     does;
  8. lock contention (from 1.6).

**Phase 1 exit criteria:** every §22 acceptance criterion in the gap
analysis scorecard that currently reads "fails today" passes an automated
test, except delete handling (Phase 3) and atomic publish (Phase 3).

---

## Phase 2 — P1 guardrails (backlog #10–#15)

### 2.1 Publish volume guardrail (#11, C11) — S

- Wire `expectedCount` (`PublishService.scala:12,86-91` — exists, dead):
  `CuratedService` passes the post-hygiene incoming count + unchanged
  count. New config `curated.publish.max_shrink_percent` (default 20):
  fail when the staged count is more than N% below the current target
  count (cheap `spark.table(fullTable).count()` guarded by table
  existence). Skip the merge and publish entirely on zero-row increments
  (S10) — return the previous counts with `SKIPPED` audit status.

### 2.2 Raw idempotency key + reconciliation (#12, C6) — M

- New config `raw.idempotency_key = [cols]` (defaults to
  `merge.keys + freshness column` when present). New reconciliation check
  `raw_duplicate_versions`: count of duplicate key-tuples written *in this
  run's window*, WARN/FAIL per policy. Do **not** touch the
  `staged.nonEmpty` gate (`IngestPipeline.scala:304-306`) — the constant
  `file_id` trap is documented in the gap analysis; instead the replay
  guard for JDBC becomes window-based: before the raw append, count rows
  where `extract_end_ts = this window's end` (populated by 1.1) and skip
  the append on match, replacing the run-id-only guard for re-runs without
  `--resume`.
- Replace `curated_inserts_plus_updates_covered`
  (`IngestPipeline.scala:441-445`) with the real identity:
  `accepted == inserts + updates + ignored + null_key + deduped` using the
  counts added in 1.2/1.4.
- Add `watermark_continuity`: this run's lower == previous committed upper
  (unless overlap configured) — data for it already exists in
  `ingest_watermarks.lower_value`.

### 2.3 Mandatory, complete run ledger (#13, S4) — M

- `AuditService`: absent `audit` block → **fail at startup** unless
  `audit.enabled = false` is set explicitly (loud opt-out replaces silent
  no-op, `AuditService.scala:100-101`). `--resume` without a functioning
  audit store aborts instead of silently full-re-running
  (`AuditService.scala:220-230` returning None becomes an error on the
  resume path).
- Add `run_mode`, `window_start`, `window_end` columns to
  `RunAuditRecord`/DDL (`AuditService.scala:34-49,123-126`,
  `ddl/ingest_audit.sql`) — additive `ALTER TABLE ADD COLUMNS`, so
  existing tables migrate in place.
- Flip `audit.reconciliation.on_mismatch` default WARN → FAIL
  (`IngestPipeline.scala:452-459`); release-note the behavior change.

### 2.4 Schema-drift hardening in curated (#15, S14) — M

- `align`/`castConfigured` (`CuratedTransform.scala:14-22,107-111`): after
  casting, count rows where `source IS NOT NULL AND casted IS NULL` per
  column (single aggregate pass, same pattern as
  `SchemaValidator.validateData` `:255-270`); non-zero → policy-gated
  violation (`on_cast_error = FAIL | WARN | REJECT_ROWS`, default FAIL).
- Unmapped incoming columns dropped by `align`'s strict projection: emit an
  `on_extra_column`-policy violation (reuse HDR_006) instead of silence.
- `compatibility`: either enforce (BACKWARD = new contract may add
  optional/defaulted columns only, may not narrow types — checked in
  `SchemaValidator.versionMismatch` against the stored version's contract
  snapshot, which requires persisting the contract JSON as a table
  property next to `ingest.schema.version` in `SchemaVersions.scala`) or
  delete the field. Recommendation: enforce; the storage hook already
  exists.

### 2.5 Retire/harden `--stage curated` (#14, S13) — M

- Reimplement `IngestMain.runCuratedOnly` (`IngestMain.scala:52-71`) as a
  thin wrapper over `IngestPipeline` with `--resume`-style raw-slice
  replay: require `--run-id` (replay that run's slice via the existing
  `readRawSlice`, `IngestPipeline.scala:398-407`) or an explicit
  `--ingest-dt` with a real entity name — always with `RunContext`, audit,
  reject split, contract validation, the 1.6 lock, and no watermark
  advancement. Delete the synthetic
  `RunContext(UUID.randomUUID(), "unknown")` path
  (`CuratedService.scala:31-35`, `CuratedStageRunner.scala:17-21` legacy
  overloads go with it — they are also the S12 contract-bypass route).

### 2.6 Reject-payload redaction + draft hygiene (#20 partial, S11) — M

- `RejectService`: config `rejects.payload = FULL | MASKED | KEYS_ONLY`
  (default `MASKED` once contract sensitivity tags exist — Phase 3.3;
  until then default `FULL` with a startup warning on feeds that enable
  quarantine). `MASKED` hashes/nulls columns tagged sensitive; `KEYS_ONLY`
  stores key columns + error info only.
- `Drafts.scala:19-45`: mask `_auth.secret.value` (and anything matching
  the existing `ConfigSummary` sensitive-key regex) before writing draft
  JSON; set file permissions 600.

**Phase 2 exit criteria:** a misconfigured or partial run cannot silently
corrupt curated (volume guardrail, cast guardrail, mandatory ledger,
FAIL-default reconciliation), and every write path runs under audit + lock.

---

## Phase 3 — P2 functional completeness (backlog #16–#21)

Sequenced by value; each independent.

1. **Delete strategy (#16, C3)** — M. Soft-delete mapping first:
   `source.delete_indicator { column, true_values }` → curated `is_deleted`
   + `last_modified_op = 'D'` via the existing merge (a delete is just a
   record whose winner sets the flag). Periodic key-reconciliation job
   (compare source keys vs curated, mark missing as deleted) as a separate
   entry point later. "Ignore deletes" stays the default but must be
   declared: new required config `curated.merge.deletes = IGNORE | SOFT | RECONCILE`.
2. **`record_hash` (#17, S1)** — M. `sha2(concat_ws(sep, normalized business
   cols))` with documented null/trim/case/scale normalization, stamped in
   raw (column exists from 1.1's DDL update) and used in the 1.2 merge as
   a cheap pre-filter: matched keys with equal hashes → `ignored` without
   restamping. Exclude technical columns; order by contract position for
   stability.
3. **Contract mapping attributes + generation (#18, S5)** — L.
   `ColumnContract` gains `sourceType`, `transform`, `sensitivity`,
   `businessKey: Boolean`, `incremental: Boolean`; `curated.merge.keys` and
   `incremental.watermark_columns` become derivable from the contract
   (explicit config still wins, mismatch = HDR_017). `ingestion-config-gen`
   JDBC flow gains `INFORMATION_SCHEMA.COLUMNS` introspection
   (via the existing `DriverQueries` machinery) to emit a full 350-column
   contract draft, and gains the schema-contract questions it currently
   lacks (`JdbcQuestionFlow.scala:54-57`).
4. **Atomic curated publish (#19, S2)** — L. Spike: Delta Lake (or Iceberg)
   sink behind the existing `SinkRegistry`/`PublishService` seams —
   `MERGE INTO` replaces the anti-join + `INSERT OVERWRITE` pair, fixing
   atomicity and the full-table rewrite cost together. Requires the
   target-technology business decision (§21); do not start before it.
5. **Retention/purge (#20, S8)** — M. Config-driven purge entry point
   (`retention { raw = 400d, rejects = 90d, audit = 730d, watermarks = keep-last-50 }`)
   dropping raw partitions by `ingest_dt` and deleting aged reject/audit
   rows; dry-run mode; runbook integration.
6. **Misc (#21, S15)** — S each: cache the parsed JDBC config from read
   time so `advanceWatermark` stops re-resolving secrets
   (`JdbcSource.scala:306`); clear `readWindows` entries in a
   failure path (`try/finally` around the pipeline run); curated
   partitioning support or documented "none"; Kafka offset persistence via
   the `WatermarkStore` abstraction if Kafka feeds are in scope.

---

## Cross-cutting

### Sequencing / dependency graph

```
0.1 UTC ──────────────┐
0.2 freshness config ─┼─→ 1.2 freshness merge ─→ 1.8 tests ─→ (3.2 record_hash)
1.1 raw lineage ──────┘         ↑
1.3 dedup determinism ──────────┘
1.4 null keys ─→ 1.8            (needs 2.6 redaction before PHI rollout)
1.5 source clock / NOT NULL      (independent)
1.6 lock ─→ 2.5 legacy path      (independent of merge work)
1.7 watermark gating             (independent; pairs with 1.5)
2.x guardrails                   (after Phase 1; 2.2 needs 1.1's window columns)
```

Phases 1.1–1.4 (merge track), 1.5 (JDBC track), and 1.6–1.7 (pipeline
track) are three independent tracks that can proceed in parallel.

### Compatibility and rollout

- Every behavior change ships behind config with the old behavior as
  default for one release where marked (`upper_bound = MAX_VALUE`,
  freshness WARN-fallback), **except** the safety flips that are the point
  of the work: watermark gating (1.7), mandatory ledger (2.3), and
  reconciliation FAIL default (2.3) change defaults immediately and are
  called out in release notes with the one-line config to restore old
  behavior.
- DDL migrations are additive only (`ADD COLUMNS` on raw, curated, audit
  tables); provide a `ddl/migrations/2026-07-remediation.sql` script.
- New error codes: JDBC_006 (nullable watermark), PIPE_001 (lock
  contention), CUR_001 (null business key), HDR_019 (missing dedup
  ordering) — added to the documented catalogs.

### Test strategy

- Every P0 item lands with its regression test in the same PR (the gap
  analysis showed the dangerous paths are exactly the untested ones:
  pre-created DDL, INCR merge, `--stage raw`, concurrency).
- `CuratedMergeIntegrationSpec` (1.8) becomes the acceptance-criteria
  harness: each §22 criterion maps to a named test.
- Existing tests that codify current-bug behavior get updated, not
  deleted: `IngestFlowIntegrationSpec.scala:182-183` (duplicate survives),
  `CuratedTransformTest` fallback test (asserts degradation is acceptable).

### Suggested delivery order (single engineer, releasable increments)

| Release | Contents | Effort |
|---|---|---|
| R1 | 0.1, 0.2, 1.3, 1.4, 2.1 (hygiene + guardrails, low risk) | ~2 wks |
| R2 | 1.1, 1.2, 1.8 (lineage + freshness merge + test harness) | ~3 wks |
| R3 | 1.5, 1.6, 1.7 (JDBC integrity + lock + gating) | ~3 wks |
| R4 | 2.2–2.6 (ledger, reconciliation, drift, legacy path, redaction) | ~3 wks |
| R5+ | Phase 3 by business priority (deletes and record_hash first) | — |

Parallelizing the three Phase-1 tracks across two engineers compresses
R2+R3 to roughly three weeks combined.
