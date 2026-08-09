# Performance Review — First Pass (Static Analysis)

Produced by four parallel performance-review agents (source extraction,
Spark transformation/merge code, write paths, runtime/config) over commit
`15a5ef4`. **No code was modified.** No Spark event logs, History Server
metrics, or database execution plans were available in this environment, so
every finding is labeled with its evidence class: **Proven-by-code**,
**Strongly-suspected**, or **Hypothesis** (must be validated by the stated
benchmark). Findings referencing multiple agent IDs were independently
discovered by more than one reviewer.

---

## 1. Executive Summary

**Overall assessment.** Correctness engineering is strong: bounded
watermark windows captured pre-read and committed post-publish, disciplined
cache lifecycle, deterministic dedup tie-breakers, no UDFs, no unbounded
driver collects on the data path, sanitized logging. The performance
posture is much weaker: the pipeline's dominant costs scale with
**cumulative table history, not batch size**, and the default source-read
configuration serializes large extractions.

**Primary scalability limit.** The curated publish is O(table) per run:
every incremental run reads the full curated target ~5 times and rewrites
it twice (staging + `INSERT OVERWRITE`), regardless of how small the delta
is. Combined with per-run scans of the ever-growing RAW table on
non-partition predicates, run time grows without bound as tables age even
when daily volume is constant.

**Top five bottlenecks.**
1. **F1** Curated merge rescans and fully rewrites the entire target every
   incremental run (Critical, proven).
2. **F2** Default JDBC read is one unpartitioned connection at fetchsize
   1000 — a 100M-row extract runs on a single task (Critical, proven).
3. **F3** `captureUpper` bundles `COUNT(1)` with `MAX()`, forcing a full
   source-table scan on the OLTP system every incremental run — before a
   single row is extracted (High, proven).
4. **F4** Up to six per-run scans of the RAW table on `run_id`/window
   predicates that cannot use partition pruning; cost grows with total
   history (High, proven).
5. **F5** `MIN_MAX_QUERY` partition-bound discovery ignores the watermark
   window, so incremental feeds stride the entire historical key range —
   N−1 empty partitions plus one carrying the whole delta (High, proven).

**Expected impact of the highest-priority improvements.** Eliminating the
duplicated target passes and the second full curated write (F1) roughly
halves curated-stage I/O immediately, with O(delta) cost achievable via a
format change later. Window-derived JDBC partitioning (F2/F5) converts
single-threaded extractions into cluster-parallel ones — the largest
single wall-clock lever for big tables. Dropping the per-run `COUNT(1)`
(F3) removes a full source scan per run from the OLTP database.

---

## 2. Architecture Map (verified execution flow)

```
IngestMain#main (ingestion-app IngestMain.scala:19)
  └─ one SparkSession, one --entity per spark-submit; UTC session TZ
     IngestPipeline#run (IngestPipeline.scala:229)
      ├─ Validate  FileIntakeService#stage — list/validate/SHA-256/stage files (driver-side, serial)
      ├─ Raw       JdbcSource#read | FileSource#read
      │              ├─ JdbcHealthCheck#check → captureUpper (MAX+COUNT) → bounded predicate
      │              │  Watermarks#boundedPredicate: (lower[-overlap], capturedUpper]
      │              ├─ QueryBuilder#dbtable — projection+filters+window inlined (pushdown by construction)
      │              └─ buildReader — STATIC_RANGE | MIN_MAX_QUERY | PREDICATES | unpartitioned default
      │            RawMetadata#add (13 withColumns) → RecordHash#stamp → RejectService#split
      │            persist ×2 → RAW guards (run_id/window/drift scans) → HiveSink#write (ORC append, ingest_dt)
      ├─ Curated   CuratedService#process
      │              CuratedTransform casts/derives/audit (foldLeft-withColumn chains)
      │              splitNullKeys → dedup (Window keys/orderBy, row_idx tie-break)
      │              publishIncremental: target scan → semi/anti joins vs incoming keys
      │              → record_hash unchanged-prefilter → freshness window ('T' > 'I' tie)
      │              → PublishService#publish: staging saveAsTable → count/unique/shrink checks
      │              → INSERT OVERWRITE full target → DROP staging
      ├─ Reconcile IngestPipeline: raw_equals_accepted, source_equals_accepted_plus_rejected,
      │              curated_accounts_for_accepted_rows (audit ledger)
      └─ Watermark JdbcSource#advanceWatermark → HiveWatermarkStore#recordIfVersion
                     (captured upper, optimistic version CAS, advance-only, discard-on-failure)
```

- **Entry points:** `IngestMain` (pipeline), `ConfigGeneratorMain` (wizard),
  `RetentionService` via `--stage retention` (see F-BUG below).
- **Watermark lifecycle:** read `latestVersioned` → capture upper pre-read →
  extract bounded window → publish → `recordIfVersion(captured, version)`;
  preserved on any failure. Correct by construction (EXT-P15).
- **Concurrency model:** strictly single-feed-per-JVM; cross-table
  concurrency = multiple spark-submits coordinated only by the per-entity
  Hive `LockService`. No cross-job source-connection budget, no workload
  classes, no FAIR pools, no job groups (RTC-P2/P9).
- **Config hierarchy:** cluster `spark-defaults.conf` → submit script
  (`scripts/run_health_sherpa.sh`: shuffle.partitions=200, caseSensitive,
  partitionOverwriteMode=dynamic, Kryo, maxAppAttempts=1) → code
  (`IngestMain`: caseSensitive again, session TZ) → per-feed HOCON. No
  executor/driver sizing anywhere; `app.spark.appName/master` in
  application.conf are dead config (never read).

**Off-scope functional bug found during review (F-BUG):**
`--stage retention` is dispatched in `IngestMain.scala:47-51` but rejected
by the CLI validator (`Cli.scala:78-82` excludes it from `validStages`) —
retention appears unreachable from the CLI. Needs a one-line fix + test.

---

## 3. Consolidated Findings Table

Severity: C=Critical H=High M=Medium L=Low. Confidence: P=Proven-by-code,
S=Strongly-suspected, Hy=Hypothesis (benchmark required). Risk = implementation risk.

| ID | Sev | Component | Finding | Evidence (verified) | Expected impact | Risk |
|----|-----|-----------|---------|---------------------|-----------------|------|
| F1 | C | Curated publish | ~5 full target reads + 2 full writes per incremental run; O(table) not O(delta) | CuratedService.scala:451,417-418,489-492,518,549-551; PublishService.scala:60,67,72,135 (P) | ~50% curated I/O cut short-term; O(delta) with format change | Med |
| F2 | C | JDBC read | Default read is single-partition, one connection, fetchsize 1000 | JdbcSource.scala:204-212; JdbcSourceConfig.scala:256-259 (P) | Cluster-parallel extraction of large tables | Mod |
| F3 | H | Incremental capture | `MAX + COUNT(1)` full source scan every run; 300s hardcoded timeout × 3 retries = repeated scans of a struggling source | JdbcSource.scala:353,286; DriverQueries.scala:44 (P) | Removes one full OLTP scan per run | Safe |
| F4 | H | RAW guards | Up to 6 per-run RAW scans on non-partition predicates; grows with history | IngestPipeline.scala:581-598,614-627,463-491; RawIdempotency.scala:26-32 (P) | Bounded, constant-time guards | Low-Med |
| F5 | H | Partitioning | MIN_MAX_QUERY bounds ignore watermark window → near-total stride skew for incremental feeds | JdbcSource.scala:230; QueryBuilder.scala:79 (P) | Partitioning works where configured | Safe |
| F6 | H | Window semantics | In-flight-commit gap at captured upper; overlap has no default; unprotected TIMESTAMP watermark only WARNs | Watermarks.scala:57-65; JdbcSourceConfig.scala:429-444 (S) | Correctness: prevents silent permanent row loss | Mod |
| F7 | H | Deployment | No executor/driver sizing or workload classes anywhere; cluster defaults for every feed | run_health_sherpa.sh:7-21 (P; impact Hy) | Right-sized jobs; enables all other tuning | Low |
| F8 | H | Concurrency | No cross-job source budget: N feeds × (numPartitions+~3) uncoordinated DB connections | Cli.scala:31-75; JdbcSource.scala:90-99; LockService scope (P) | Prevents source-DB exhaustion at fleet scale | Med |
| F9 | H | Contract validation | 2 extra full source parses before the persist point (content + nullability scans on unpersisted lineage) | IngestPipeline.scala:390-391,449; ContentValidator.scala:50; SchemaValidator.scala:255-260 (P) | Read phase ÷3 for contract feeds | Low |
| F10 | H | Output sizing | No file-size control on any write; no compaction; control tables accumulate 1-row files (audit/lock/watermark/registry) | HiveSink.scala:69-72; PublishService.scala:60; AuditService.scala:213; LockService.scala:133-134; RetentionService (drops only) (P) | Bounded file counts; NameNode + read-amp relief | Low-Med |
| F11 | M | Wide tables | foldLeft-withColumn chains: quadratic Catalyst analysis at 350+ columns | CuratedTransform.scala:16-76,163-179 (P pattern; Hy magnitude) | Driver planning minutes → seconds on wide feeds | Low |
| F12 | M | Merge joins | Full-target key dedup before join; no broadcast hints; `<=>` may block broadcast planning | CuratedService.scala:416-421,475-491,521 (P shape; S dominance) | Smaller shuffles per merge | Med |
| F13 | M | File intake | Serial driver-side SHA-256 of every landed file before any Spark work | FileIntakeService.scala:110-148; FsUtils.scala:54-66 (P) | Intake wall-clock ÷ threads | Mod |
| F14 | M | Caching | 5 overlapping wide persists resident simultaneously; earliest held to end-of-run | IngestPipeline.scala:449-452,89; CuratedService.scala:158,465,531 (P; pressure S) | Less eviction/recompute churn | Low |
| F15 | M | Driver collects | Unbounded growth: file-registry checksums + lock-table claims collected whole every run | FileIntakeService.scala:401-412; LockService.scala:113-122 (P) | Constant-time intake/locking | Low |
| F16 | M | Metastore | ~25-40 metastore round-trips/run (per-event ensure/DDL, listTables sweep, always-write TBLPROPERTIES); HiveSink session conf never restored | AuditService.scala:140-149; PublishService.scala:154; SchemaVersions.scala:66; HiveSink.scala:26-27 (P) | Seconds of fixed overhead removed per run | Low |
| F17 | M | Watermark store | Full history scan twice per commit; 1 file per commit forever; best-effort CAS | WatermarkStore.scala:66-113 (P) | Constant-time watermark ops | Mod |
| F18 | M | Dialect fetch | One global fetchsize: no-op on SQL Server, OOM risk on MySQL (no useCursorFetch), win on Oracle | JdbcSourceConfig.scala:139; JdbcDialect.scala:180-203 (S) | Prevents MySQL executor OOM | Safe |
| F19 | M | Diagnostics | `skew_metrics=true` re-executes the entire extraction (pre-persist `df.rdd` pass) | JdbcSource.scala:163,255; IngestPipeline.scala:449 (P) | Halves source load when enabled | Safe |
| F20 | M | Predicates | Bounded composite window rendered as `NOT(OR-chain)` — seek-unfriendly on SQL Server | Watermarks.scala:65,84-92 (Hy) | Index seeks instead of scans | Safe |
| F21 | M | File source | Repeated-header FAIL probe scans every row; trailer/skip window shuffles whole dataset with a known split-order hazard | FileSource.scala:153,287-302 (P) | One fewer full pass; removes hazard | Mod |
| F22 | M | Codegen | 350-column sha2/concat + reject rules evaluated 4× per row; whole-stage codegen fallback risk | RecordHash.scala:36-38; RejectService.scala:151-176 (Hy) | 3-10× row throughput if fallback confirmed | Low |
| F23 | M | Window skew | Hot business keys serialize dedup/freshness windows into one task; AQE cannot split windows | CuratedTransform.scala:127-131; CuratedService.scala:529-531 (Hy) | Straggler elimination if proven | Med |
| F24 | M | Speculation | Speculation unset; if cluster defaults enable it, dynamic-partition appends risk duplicate task commits | HiveSink.scala:26-27,71-72; no spark.speculation anywhere (Hy) | Correctness insurance | Safe |
| F25 | L | Partition guard | No cardinality guard on configurable raw partition keys (nonstrict dynamic partitions, no max guard) | Partitioning.scala:11-43; HiveSink.scala:26-27 (P) | Prevents partition explosion misconfig | Safe |
| F26 | L | Timestamp bounds | Timestamp columns unusable for range partitioning; MIN_MAX_QUERY on one dies with NumberFormatException | JdbcSourceConfig.scala:258-259; JdbcSource.scala:238 (P) | Natural partition column usable; clean error | Mod |
| F27 | L | Observability | JdbcMetrics never exported; no bytes/spill/duration metrics; YARN app name generic (dead appName config) | JdbcMetrics.scala:33; application.conf:2-5; script (P) | Fleet visibility | Low |
| F28 | L | Serialization | Kryo flag effectively idle (DataFrame-only pipeline); closure hygiene verified clean; no action | script:13; grep (P) | None — informational | — |

Unwired packages (`jdbc/extraction`, `core/raw`, `core/curated`,
`core/reconcile`) add zero runtime cost today; noted risks: they duplicate
the unpooled-connection and fetchsize patterns (would inherit F3/F18 if
wired), `SnapshotStrategy` writes a full source image per run with no
retention hook, and `BatchIdempotencyGuard` would repeat F4's scan pattern.

---

## 4. Detailed Findings (top items)

### F1 — Curated publish is O(table) per run
- **Severity:** Critical. **Confidence:** Proven-by-code (scan/write count);
  wall-clock magnitude Strongly-suspected.
- **Affected:** `CuratedService#publishIncremental`
  (CuratedService.scala:436-551), `PublishService#publish`
  (PublishService.scala:41-137).
- **Current behavior:** one incremental run reads the full target for the
  audit join (:417-418), contested-key count (:492), target-key hash
  (:518), the `unchanged` branch at staging write (:549→PublishService:60),
  and the shrink-guard count (:135) — then writes the entire table twice
  (staging `saveAsTable` + `INSERT OVERWRITE`). A 1-row delta costs ~5 full
  reads + 2 full writes.
- **Why slow:** cost scales with accumulated history; daily incremental
  feeds degrade linearly forever. The second full write buys atomicity a
  metadata operation could provide.
- **Recommended change (sequence):** (1) persist `contested` and reuse for
  count/hash/union; derive shrink-guard count from counts already in hand —
  removes ~3 target reads, Safe. (2) Replace the second full copy with a
  location/generation swap (`ALTER TABLE ... SET LOCATION` or two-table
  view flip) — halves write bytes, Medium risk (atomicity mechanism
  changes; reader exposure comparable to INSERT OVERWRITE). (3) Strategic:
  ACID/MERGE format (Delta/Iceberg) for O(delta) — the code itself
  anticipates this (CuratedService.scala:75).
- **Compatibility:** all steps Scala 2.12 / Spark 3.5 / Java 11; step 2
  Hive-metastore-only.
- **Functional risk:** reconciliation counts must stay arithmetically
  identical (`curated_accounts_for_accepted_rows`,
  IngestPipeline.scala:736-744).
- **Validation:** benchmark 100M×350 target, 0.1% delta; count target-scan
  nodes in the SQL tab; assert identical CuratedResult counts; measure HDFS
  bytes written (expect ~50% cut from step 2).
- **Rollback:** each step independently revertible; step 2 gated by config.

### F2 — Default JDBC read is single-partition
- **Severity:** Critical (large tables). **Confidence:** Proven-by-code.
- **Affected:** `JdbcSource#buildReader` (JdbcSource.scala:204-212),
  `JdbcSourceConfig` (:256-259).
- **Current behavior:** unless a feed explicitly configures all of
  partitionColumn/bounds/numPartitions, the read falls through to a
  single-connection, single-task load.
- **Recommended change:** for INCREMENTAL feeds, synthesize PREDICATES
  sub-ranges from the watermark window already captured (lower, upper, and
  the in-scope count `captureUpper` already fetches):
  `numPartitions = ceil(countInScope / targetRowsPerPartition)` with
  `targetRowsPerPartition = fetchsize × configured round-trips-per-task`,
  capped by `max_partitions`. For FULL loads above a probed row-count
  threshold, warn or fail on unpartitioned reads.
- **Functional risk:** predicate generation must be dialect-quoted and
  window-exact (mutually exclusive, collectively exhaustive slices).
- **Validation:** >10M-row table, unpartitioned vs 8/16/32 predicates;
  wall-clock + executor utilization + source-DB session count.
- **Rollback:** config flag to disable derived partitioning per feed.

### F3 — Per-run COUNT(1) full scan in captureUpper
- **Severity:** High. **Confidence:** Proven-by-code.
- **Affected:** `JdbcSource#captureUpper` (JdbcSource.scala:327-372),
  `DriverQueries#firstRow` (DriverQueries.scala:21-44).
- **Current behavior:** `SELECT MAX(col), COUNT(1)` every incremental run;
  MAX alone is an index seek, COUNT(1) touches every in-scope row. The
  count exists only to disambiguate empty-source vs NULL-watermark — a
  rare-path concern paid every run. 300s hardcoded timeout × 3 retries can
  hammer a struggling source for ~15 minutes.
- **Recommended change:** query `MAX` alone; issue COUNT only on the
  NULL-MAX branch (as the composite path already does at :370-372). Make
  the companion-query timeout configurable and classify `SQLTimeoutException`
  as retry-once.
- **Validation:** DB-side plan/duration for both forms on an indexed table;
  existing incremental suite must stay green.
- **Rollback:** trivial revert.

### F4 — Growing RAW-table scans per run
- **Severity:** High. **Confidence:** Proven-by-code (scans); growth cost
  Strongly-suspected.
- **Affected:** IngestPipeline.scala:581-598, 614-627, 463-491;
  RawIdempotency.scala:26-32.
- **Recommended change:** bound every guard scan by `ingest_dt >=
  date(run_start)` (rows for a run can only exist in partitions written
  since it started — ingest_dt is stamped at write time); consolidate the 3
  guard scans into one pass and rawCount+duplicateVersions into another; or
  serve the answers from the (small) audit ledger.
- **Functional risk:** guards are correctness features — predicate addition
  only, never removal; needs a replay-safety test.
- **Validation:** RAW at 10/100/400 daily partitions; guard-phase input
  bytes and duration.

### F6 — Boundary-commit gap (correctness)
- **Severity:** High (silent data loss). **Confidence:** Strongly-suspected
  (inherent design property; mitigations exist but are opt-in).
- **Current behavior:** a transaction that began before upper-capture with
  `ts <= upper` but commits after the reads finish is permanently skipped;
  `overlap` (no default) or ROWVERSION/COMPOSITE watermarks mitigate;
  unprotected TIMESTAMP configs only WARN.
- **Recommended change:** flip `on_unprotected_watermark` default to FAIL
  for TIMESTAMP + MAX_VALUE/SOURCE_CLOCK; require per-feed overlap derived
  from max source transaction duration (config-gen should ask). Validate
  the held-transaction scenario on a test DB (hypothesis benchmark
  specified by the extraction agent).

### F9 — Triple source parse for contract feeds
- **Severity:** High. **Confidence:** Proven-by-code.
- **Current behavior:** ContentValidator agg + nullability agg run on the
  unpersisted source lineage; the persist happens later, so contract feeds
  parse every 350-column CSV three times.
- **Recommended change:** persist before `validateContractBeforeRaw` (and
  unpersist once `withMeta` materializes), or merge the two aggregations
  into one pass — both are single-`agg` designs, trivially concatenable.
- **Validation:** FileScan job count over source paths per run; target 1.

### F11 — Wide-table plan growth
- **Severity:** Medium (driver latency). **Confidence:** pattern proven;
  magnitude is a hypothesis and must be validated using the specified
  benchmark.
- **Recommended change:** collapse each foldLeft-withColumn chain
  (castConfigured, applyContractTransforms, applyTransforms, normalizeKeys)
  into a single `df.withColumns(map)` (Spark ≥3.3) or one `select`
  projection, mirroring what `align` already does at its final select.
- **Validation:** synthetic 350-column contract; measure driver time to
  first action and analyzed-plan depth before/after.

Remaining findings (F5, F7, F8, F10, F12–F28) carry their full detail in
the consolidated table above and the per-agent evidence; each has a stated
validation method and none requires architecture replacement.

---

## 5. Configuration Recommendations

| Configuration | Current | Recommended | Reason | Side effects | Benchmark |
|---|---|---|---|---|---|
| `spark.sql.shuffle.partitions` | 200 (script; = default) | Derive per workload class: shuffle-write bytes ÷ ~128MB; AQE coalesces downward | 200 too low for XL merges, decorative otherwise | Spill/OOM if too low | Yes |
| `spark.executor.instances/cores/memory`, `spark.driver.memory` | absent (cluster defaults) | Introduce S/M/L/XL workload classes; size from audit-ledger volumes | One-size-fits-nothing today | Queue contention | Yes |
| `spark.dynamicAllocation.*` | absent | Enable with per-class `maxExecutors` cap if many short jobs share a queue | Fleet utilization | Shuffle-tracking needed | Yes |
| AQE flags | absent (3.5 defaults ON) | Verify cluster doesn't override; do not rely on AQE for source partitioning | Defaults are correct | — | No |
| `spark.sql.autoBroadcastJoinThreshold` | default 10MB | Raise only with measured `ik` key-frame sizes; prefer explicit gated hints (F12) | Merge joins currently AQE-dependent | OOM if oversized | Yes |
| `spark.speculation` | unset | Pin `false` explicitly | Duplicate task commits on dynamic-partition appends; double source reads | Straggler mitigation lost | No |
| `spark.serializer` Kryo | set, unconfigured | Keep; no registration needed (DataFrame-only) | Marginal either way | — | No |
| `spark.yarn.maxAppAttempts` | 1 | Keep (deliberate: resume via `--resume`) | Documented rationale | Manual re-run on driver crash | No |
| `spark.sql.caseSensitive` | false ×2 (script+code) | Keep one | Hygiene | — | No |
| `app.spark.appName/master` | dead config | Pass `--name ingest-${ENTITY}` in script; delete dead keys | RM UI shows only class name across fleet | — | No |
| JDBC `fetchsize` | 1000 global default | Move default into dialects; MySQL adds `useCursorFetch=true` | No-op on SQL Server, OOM risk on MySQL, win on Oracle | MySQL server-side cursors | Yes |
| Companion-query timeout | 300s hardcoded | Configurable; retry-once on timeout | Retry storms vs struggling source | — | No |
| ORC compression | unset (snappy) vs DDL ZLIB | One explicit config-exposed codec per layer | Mixed estate today (hypothesis — verify with orc-tools) | CPU/storage trade | Yes |

---

## 6. Proposed Change Sequence (no code modified in this pass)

Each change is independent, minimal, Scala 2.12 / Spark 3.5 / Java 11
compatible, and separately benchmarkable:

1. F3: `MAX`-only capture; COUNT on NULL branch. (Safe)
2. F9: persist before contract validation / merge the two aggs. (Safe)
3. F1-step1: persist `contested`; derive counts from frames in hand. (Safe)
4. F4: `ingest_dt`-bound + consolidated RAW guard scans. (Low-Med)
5. F5: thread the watermark predicate into `discoverBounds`. (Safe)
6. F19: skew metrics from the persisted frame post-persist. (Safe)
7. F16: cache ensure-state; skip no-op TBLPROPERTIES; prefix-scoped
   listTables; restore session confs in HiveSink. (Low)
8. F10: repartition/coalesce before writes sized bytes÷blocksize; batch
   control-table appends per stage boundary; schedule retention+compaction.
   (Low-Med)
9. F11: `withColumns` collapse of the foldLeft chains. (Low)
10. F2: window-derived PREDICATES partitioning for incremental feeds. (Mod)
11. F1-step2: location-swap publish replacing the second full write. (Med)
12. F6: FAIL default for unprotected TIMESTAMP watermarks + required
    overlap. (Mod — deliberate config break)

Priority ordering used: `expected benefit × confidence ÷ implementation
risk`; items 1–8 are Phase-1/2 material (high confidence, low risk).

---

## 7. Benchmark Plan

- **Representative tables:** one file feed (350 columns, 5–50M rows/batch),
  one JDBC incremental feed (100M–1B-row source, 0.1–1% daily delta,
  indexed timestamp + key), one JDBC full load (100M rows).
- **Curated targets:** 100M-row current-state table for merge benchmarks.
- **Environment:** fixed YARN queue; record cluster config; Docker SQL
  Server (Testcontainers, already in the repo) for source-side query-plan
  and connection-count assertions; Derby-metastore local Hive for layout
  tests.
- **Protocol:** 1 warm-up + 5 measured runs per variant; report median and
  worst case.
- **Metrics:** wall-clock per stage (derivable from the audit ledger),
  rows/sec, HDFS bytes read/written, files per partition, shuffle
  read/write, spill, executor peak memory (History Server), source-DB
  session count and query plans, Spark job count per run.
- **Success criteria:** each change ships only if median improves with no
  worst-case regression and all 650+ tests stay green, reconciliation
  equalities intact.

---

## 8. Prioritized Implementation Plan

- **Phase 1 — low-risk, high-confidence fixes:** items 1–7 above
  (redundant scans, capture COUNT, guard bounding, metastore chatter,
  skew-metrics double read).
- **Phase 2 — configuration & partition tuning:** workload classes with
  derived executor sizing (F7); dialect fetchsize defaults (F18); explicit
  speculation/codec settings (F24, ORC); output-size control + compaction
  scheduling (F10); observability (F27: JdbcMetrics export, per-stage
  durations, `--name`).
- **Phase 3 — join, merge & skew:** merge-join reshaping + gated broadcast
  (F12); wide-table projection collapse (F11); window-skew probe and, only
  if proven, two-phase aggregation dedup (F23); composite-predicate
  envelope (F20).
- **Phase 4 — concurrency & large-table scalability:** window-derived JDBC
  partitioning (F2, F5, F26); source connection budgets and per-source
  coordination (F8); parallel intake hashing (F13); registry/lock read
  bounding (F15, F17).
- **Phase 5 — architectural:** location-swap publish then ACID-format
  MERGE for O(delta) curated (F1); transactional watermark/checkpoint store
  (F17 — the unwired `JdbcCheckpointStore` already implements true CAS);
  decide the unwired-framework wiring question alongside.
