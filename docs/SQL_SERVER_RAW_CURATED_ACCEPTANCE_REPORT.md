# SQL Server Raw/Curated — Acceptance Report & Traceability Matrix

**Scope:** the 13-prompt implementation program (`SQL_SERVER_RAW_CURATED_IMPLEMENTATION_PLAN.md`)
executed on top of the already-remediated baseline. **Verdicts:** Met /
Partially met / Not met / Accepted deviation — Met is claimed only where
executable test evidence exists in this repository's suite (full reactor,
green at every merge; final count 740+ tests). SQL Server Testcontainers
suites cancel without Docker; they are marked accordingly.

## Traceability matrix

| Requirement (prompt) | Verdict | Code | Tests | Notes |
|---|---|---|---|---|
| P0 baseline & plan | **Met** | `docs/SQL_SERVER_RAW_CURATED_IMPLEMENTATION_PLAN.md`, `REQUIREMENTS_COMPLIANCE_STATUS.md` | n/a (docs) | Gap analysis re-verified against branch before any change |
| P1 typed pattern model | **Met** (CT/CDC = declared capability, **Not met** by design) | `config/IngestionPattern.scala`; validator wiring `FeedCompatibilityValidator.scala`; watermark gate `IngestPipeline` | `IngestionPatternTest` (derivation matrix, CFG_010/011/012), `JdbcPipelineIntegrationSpec` "BACKFILL publishes but never commits" | Legacy feeds parse unchanged (existing suites prove it); BACKFILL/RAW_REPLAY default `watermark_commit=false` |
| P2 raw lineage metadata | **Met** (opt-in) | `transform/RawMetadata.scala` (`ExtendedLineage`, ColumnTypes), `IngestPipeline` wiring | `RawMetadataExtendedTest`, integration "raw.lineage_extended stamps source lineage" | `source_modified_ts`/`source_operation`/`source_primary_key` opt-in via `raw.lineage_extended`; JDBC `file_id` null under it; `load_status` = **Accepted deviation** (per-run status lives in the mandatory ledger, not a raw column) |
| P3 full-load cutoff & seed | **Met** | seed capture/commit (pre-existing, `JdbcSource.scala`); bounded seed pull `JdbcSource`+`QueryBuilder`+`Watermarks.upperBoundPredicate` | `BoundedFullLoadTest`; pre-existing `JdbcSourceH2Test` seed/handoff tests | Bounding is FULL_TABLE-only (SELECT_QUERY rejects incremental blocks at parse — verified); custom-SQL bases unbounded, `allow_null_watermark` unbounded (NULL-exclusion hazard), both documented; CDC_LSN/CUSTOM_QUERY upper strategies gated behind P1 capability errors |
| P4 bounded/composite watermarks | **Met**; legacy `(lower, upper]` = **Accepted deviation** (default) | `BoundaryConvention`, `Watermarks` comparator, parse gates `JdbcSourceConfig` | `HalfOpenConventionTest` (rendering golden, parse gates); pre-existing composite/tie tests (`WatermarkCodecsTest`, `JdbcSourceH2Test`) | `HALF_OPEN` gated to scalar SOURCE_CLOCK windows — with MAX_VALUE the max row would be skipped forever (documented in the error) |
| P5 freshness-compared upsert | **Met** (pre-existing, verified) | `CuratedService.publishIncremental` | `CuratedMergeIntegrationSpec` (:132 stale-ignored, :151 newer-wins, :171 deterministic ties, :204 accounting, :277 uniqueness) | Late-older never overwrites; ties keep target; create_timestamp preserved |
| P6 key validation & ordering | **Met** | `OrderSpec.scala`, `CuratedTransform.deduplicate`, `CuratedService` (policy, require_ordering, tie-breaker directions) | `OrderSpecTest` (direction/null-order/HDR_019/HDR_020), integration CUR_004 + CUR_001 FAIL_RUN tests | `dropDuplicates` fallback retained as default (existing tests depend on it) with `merge.require_ordering=true` as the strict mode — **Accepted deviation** from outright removal |
| P7 run locking | **Met** (best-effort store = **Accepted deviation**) | `LockService` (lease/renewal/expiry pre-existing; `wait_ms` added) | `LockServiceSpec` (pre-existing), integration "wait_ms retries a held lock" | Hive append lease is documented best-effort; watermark version CAS remains defense-in-depth; a transactional lock store is the recorded follow-up (the unwired `JdbcCheckpointStore` shows the pattern) |
| P8 idempotency/audit/reconciliation | **Met** | `raw.delivery_mode` + overlap metric (`IngestPipeline`), CFG_013 (`FeedCompatibilityValidator`); mandatory ledger, window guards, continuity check, HOLD recovery (pre-existing this session) | integration DEDUPLICATED_APPEND + CFG_013 tests; HOLD/continuity tests; pre-existing ledger/resume suites | run_id rejected as identity; overlap measured as `raw_overlap_reread`; `raw_equals_accepted` adjusted for deliberate skips |
| P9 delete strategies | **Met** for IGNORE/SOFT/FULL_SNAPSHOT_ABSENCE; CT/CDC/periodic-reconciliation = **Not met** (explicit capability errors) | `DeleteSpec.SnapshotAbsence`, `publishFull` absence tombstoning, CFG_014/CUR_005 | integration absence + reactivation + CFG_014 tests; pre-existing soft-delete suite (`CuratedMergeIntegrationSpec` :498-:633) | Absence gated on `confirm_complete_extract` + no source-side filtering; counts disjoint (`absenceDeleteCount` outside the accepted-rows identity, ledger reports the total) |
| P10 timestamps/timezones | **Met** | UTC session default, clock_zone gating, DATETIMEOFFSET(7), DST-safe overlap (pre-existing); drift diagnostic `JdbcSource.clockDriftMillis` | `HalfOpenConventionTest` drift cases; pre-existing codec tests | Drift warn threshold `clock_drift_warn_ms` (default 30s, 0 off); diagnostic never fails a run |
| P11 record hash & performance | **Met** for hashing/versioning/metrics; overwrite benchmark = **Not met in this environment** | `RecordHash` options + `recipeVersion`, table property `ingest.record_hash.version` (`IngestPipeline`), unchanged/rewritten metrics log (`CuratedService`) | `RecordHashOptionsTest`; pre-existing no-change-skip test (`CuratedMergeIntegrationSpec` :314) | Canonicalization opt-in (changes hashes; recipe change detected, warned); representative 350-column benchmark requires a cluster — plan in `PERFORMANCE_REVIEW.md` §7; Delta/Iceberg migration proposed there (F1), not silently introduced |
| P12 SQL Server integration | **Partially met** (environment-bound) | `SqlServerIntegrationTest` + `SqlServerContainerSupport` (Testcontainers, Docker-gated) | Covers bracket quoting, datetime2 precision, DATETIMEOFFSET, ROWVERSION, composite predicates, partitioned reads, failure classification, CUSTOM_SQL, TLS/auth config | Suites CANCEL without Docker (this environment) — verdicts above rest on the H2 + embedded-Hive suites, which run everywhere. Follow-ups for a Docker host: seed-handoff e2e, soft-delete e2e, HALF_OPEN window e2e, concurrent-lock chaos, 350-column mapping at scale |

## Cross-cutting acceptance statements

- **No existing test was modified to force a pass.** Two pre-existing tests
  were untouched throughout; every behavior change is opt-in behind a new
  key with the legacy default, verified by the unchanged existing suites.
- **Every merge to main was gated on a full green reactor run** (final:
  742 passing, 0 failures, 8 Docker-gated cancels).
- **JDBC driver availability:** drivers arrive via `--jars` (deployment
  docs); driver-side companion queries load the class explicitly
  (`source.driver`), executors receive it from Spark's jar distribution.
- **Sanitized logging** held throughout: no credentials, no unredacted
  business records in any new log line.

## Remaining risks / follow-ups (honest register)

1. Curated publish remains O(table) per run (`PERFORMANCE_REVIEW.md` F1) —
   the ACID-format migration is proposed, not implemented.
2. Lock store is best-effort on non-transactional Hive (documented);
   strict serialization needs a transactional store.
3. CT/CDC extraction and periodic key-reconciliation deletes are declared
   capabilities that fail fast — implementing them is net-new work.
4. Docker-gated SQL Server scenarios listed under P12 need a Docker host.
5. `HALF_OPEN` covers scalar watermarks only; composite half-open needs a
   lexicographic `>=` renderer.
