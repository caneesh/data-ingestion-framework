# SQL Server Raw/Curated Implementation Plan (Prompts 0–12)

**Baseline:** main @ `a78e135` (+ in-flight C9/continuity change set).
**Re-verification:** every assertion of `REQUIREMENTS_GAP_ANALYSIS.md` was
re-checked against this branch by three independent verification passes —
results in `REQUIREMENTS_COMPLIANCE_STATUS.md`. The gap analysis is stale in
the direction the prompts anticipated: most P0 items are already
implemented and tested. This plan records, per prompt, what exists (with
evidence), what remains (the delta), and the ordered work.

**Global constraints honored:** Scala 2.12 / Spark 3.5 / Java 11 unchanged;
no library upgrades; existing config contracts preserved (every new key is
opt-in with legacy defaults); existing tests are never modified to force a
pass; column mapping stays name-based; no credential/PII logging.

---

## Prompt-by-prompt status and plan

### P0 — Baseline (this document)
Done: compliance re-verification (`REQUIREMENTS_COMPLIANCE_STATUS.md`),
execution-flow documentation (`../development/DEVELOPER_GUIDE.md`), performance review
(`PERFORMANCE_REVIEW.md`). No runtime change.

### P1 — Typed ingestion-pattern model — **DELTA: NEW (P1 severity)**
- **Current:** modes live in scattered keys — `source.mode`
  (FULL_TABLE/SELECT_QUERY/CUSTOM_SQL/INCREMENTAL, `JdbcSourceConfig.scala`),
  CLI `--mode FULL|INCR`, `watermark.advance_after`, `--stage curated`
  (RAW_REPLAY equivalent, `IngestPipeline.curatedReplay`), watermark types
  incl. COMPOSITE/ROWVERSION (`WatermarkType`), overlap. No single typed
  pattern; CT/CDC unsupported without a dedicated capability error.
- **Plan:** `config/IngestionPattern.scala` deriving the pattern from
  existing keys (no config break); explicit `ingestion.pattern` override
  key; capability errors CFG_010 for CHANGE_TRACKING/CDC_BATCH; BACKFILL
  (`ingestion.pattern = BACKFILL`) forces watermark-commit=false; validated
  in `FeedCompatibilityValidator` before any read.
- **Tests:** derivation matrix, capability errors, backfill no-commit,
  legacy feeds parse unchanged.

### P2 — Raw lineage metadata — **mostly met; DELTA: 4 opt-in columns + file_id**
- **Current (met):** run_id, load_timestamp (UTC session),
  ingest_dt (config-derived), extract_start/end_ts, source_system/database/
  schema/table, opt-in record_hash (`transform/RawMetadata.scala:25-96`);
  HiveSink FAIL-default metadata guard (`sink/HiveSink.scala:36-67`).
- **Delta:** opt-in stamping of `source_modified_ts` (copy of the
  contract-designated freshness/watermark column), `source_operation`
  (from the soft-delete indicator when configured, else 'I'),
  `source_primary_key` (contract businessKey concat); JDBC `file_id`
  currently a constant `sha2('')` (`RawMetadata.scala:95`) → null for
  non-file sources. `load_status` intentionally NOT a raw column — per-run
  status lives in the mandatory ledger (accepted deviation, documented).
- **Compatibility:** new columns strictly opt-in (`raw.lineage_extended`),
  mandatory set unchanged → no pre-created-DDL breakage; `file_id=null` for
  JDBC verified unused (RawIdempotency is file-feed-only,
  `IngestPipeline.scala:467-469`).

### P3 — Full-load cutoff/seed — **met; DELTA: bound the FULL read**
- **Current (met):** frozen cutoff pre-read, SOURCE_CLOCK
  (`SYSUTCDATETIME()` dialect-owned), seed committed only after success,
  reseed guard, never on failure/dry-run/raw-only (`JdbcSource.scala:110-131`,
  `469-477`; tests `JdbcSourceH2Test:222,292`).
- **Delta:** the FULL_THEN_INCREMENTAL first pull is unbounded — rows
  arriving mid-load land AND get re-picked next run. Bound it with the
  captured cutoff predicate (upper side only). ROWVERSION/CDC_LSN/
  CUSTOM_QUERY upper-bound strategies: ROWVERSION works via watermark
  types today; CDC_LSN/CUSTOM_QUERY deferred with the CT/CDC capability
  gate (P1).

### P4 — Bounded/composite watermarks — **met; DELTA: convention opt-in**
- **Current (met):** frozen boundaries pre-read, composite lexicographic
  tuples, overlap with advance-only commit, tuple components persisted,
  typed literals, partition predicates inside the window, empty windows
  deterministic (JDBC_006).
- **Delta:** convention is `(lower, upper]`; spec prefers `[lower, upper)`
  for source-clock timestamps. Add `incremental.boundary_convention =
  LEGACY (default) | HALF_OPEN` — HALF_OPEN renders `>= lower AND < upper`
  for SCALAR watermarks; composite + HALF_OPEN rejected at parse (JDBC_003)
  until lexicographic >= is implemented. Legacy remains wire-compatible
  default (accepted deviation stands for existing feeds).

### P5 — Freshness-compared upsert — **met; DELTA: none structural**
Freshness contest with tie-breakers, ties keep target, create_timestamp
preserved, disjoint counts, FULL-path hygiene — all tested
(`CuratedMergeIntegrationSpec`). Remaining nuance handled under P6.

### P6 — Key validation/quarantine — **met; DELTA: FAIL_RUN + ordering strictness/direction**
- **Current (met):** null/blank split before merge windows, quarantine with
  lineage + redaction, HDR_019 on missing configured ordering, row_idx
  tie-break, accounting identity.
- **Delta:** `merge.null_handling.policy = QUARANTINE (default from
  drop_null_keys=true) | ALLOW (=false) | FAIL_RUN (new)`; per-column
  direction/null-order on freshness tie-breakers
  (`tie_breakers = ["col desc nulls_last", ...]` extended form);
  `merge.require_ordering = true` opt-in that turns the empty-ordering
  `dropDuplicates` fallback into a hard error (default stays legacy —
  existing tests depend on the fallback and must not be modified).

### P7 — Run locking — **met (best-effort); DELTA: bounded wait**
- **Current (met):** claim-settle-reread lease, expiry, renewal at stage
  boundaries, force release, watermark version CAS as defense in depth;
  non-atomicity documented (`LockService.scala:13-27`).
- **Delta:** `concurrency.wait_ms` bounded wait-and-retry (default 0 =
  fail-fast, current behavior). A transactional lock store (JDBC) is the
  strict-mode fix; deferred with an explicit remaining-risk entry — Hive
  append lease stays best-effort and says so.

### P8 — Idempotency/audit/reconciliation — **met; DELTA: delivery mode + overlap metric**
- **Current (met):** mandatory ledger with run_mode/window columns,
  run_id + window guards + drift detection, `raw.idempotency_key`
  duplicate measurement, reconciliation incl. accounting identity and (this
  change set) `watermark_continuity`; C9 `on_reject_watermark = HOLD` with
  held-window recovery.
- **Delta:** `raw.delivery_mode = AT_LEAST_ONCE_APPEND (default) |
  DEDUPLICATED_APPEND` — deduplicated mode anti-joins incoming rows against
  RAW on the idempotency identity (source PK + version [+ operation]),
  never run_id; overlap-reread measurement (`raw_overlap_reread` count)
  recorded in reconciliation.

### P9 — Delete strategies — **SOFT/IGNORE met; DELTA: absence mode + capability gates**
- **Current (met):** declared IGNORE/SOFT, tombstones through the freshness
  contest (stale deletes lose; reactivation works), disjoint counts, tested.
- **Delta:** explicit capability errors for `CHANGE_TRACKING`/`CDC` modes;
  `FULL_SNAPSHOT_ABSENCE` (FULL runs only, gated on
  `deletes.confirm_complete_extract = true`, refuses partial extracts —
  where/filters/custom SQL configured); IGNORE default now audited (ledger
  message), not only logged. PERIODIC_KEY_RECONCILIATION deferred: needs a
  source-key snapshot channel; recorded as capability interface + risk.

### P10 — Timestamp/timezone — **met; DELTA: drift diagnostic**
UTC session default, clock_zone declaration, DATETIMEOFFSET(7), DST-safe
overlap — all met. Delta: source-vs-driver clock-drift diagnostic at
SOURCE_CLOCK capture (warn past a configurable threshold).

### P11 — Record hash/performance — **met (opt-in); DELTA: versioning + canonicalization opt-in**
- **Current (met):** contract-scoped SHA-256, null≠empty, sorted columns,
  merge no-change skip, empty-increment publish skip, unchanged counts.
- **Delta:** hash recipe version recorded (`ingest.record_hash.version`
  table property) so a future recipe change is detectable; opt-in value
  canonicalization `raw.record_hash_options { trim = true, uppercase =
  false }` (changes hashes → gated, documented); unchanged/rewrite metrics
  logged per run. Full-overwrite benchmark + format migration: documented
  in `PERFORMANCE_REVIEW.md` (F1) — proposal only, no silent migration.

### P12 — SQL Server integration/acceptance — **partial by environment**
Docker-gated Testcontainers suite covers bracket quoting, datetime2,
DATETIMEOFFSET, ROWVERSION, composite predicates, failure classification,
CUSTOM_SQL (`SqlServerIntegrationTest`) — cancels without Docker (this
environment). Delta: acceptance report + traceability matrix
(`SQL_SERVER_RAW_CURATED_ACCEPTANCE_REPORT.md`) with honest verdicts;
additional gated scenarios (seed handoff, soft delete e2e) recorded as
follow-ups runnable only where Docker exists.

---

## Execution order and gating

1. C9 + watermark continuity (in flight) → full suite → merge.
2. P0 doc (this file) — no runtime change.
3. P1 pattern model (validation-only layer).
4. P2+P3 lineage deltas + bounded FULL cutoff.
5. P4+P10 convention opt-in + drift diagnostics.
6. P5+P6 ordering/direction + FAIL_RUN policy.
7. P7+P8 lock wait + DEDUPLICATED_APPEND + overlap metric.
8. P9 delete completeness.
9. P11+P12 hash versioning + acceptance report.

Every step: full reactor suite green before commit; ff-merge to main; new
behavior always behind opt-in keys with legacy defaults; no existing test
modified.
