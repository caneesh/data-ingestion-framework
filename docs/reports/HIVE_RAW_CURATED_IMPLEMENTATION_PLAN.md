# Hive Source Implementation Plan (bstar_raw → bluestar_raw → bluestar_current)

**Baseline:** main @ `a4ab516`.
**Reviewed:** three independent verification passes on 2026-09-30 —
citation accuracy, pipeline integration gaps, refactor/test feasibility —
against this branch; every finding is folded into the items below.
**Pre-implementation gate:** `HIVE_SOURCE_SCENARIO_MATRIX.md` — scenario
matrix, regression safeguards, and two decisions (§0) that amend this plan.
**Goal:** ingest two existing partitioned Hive tables (`bstar_raw.priv_addr`,
`bstar_raw.sub_prem_det`, partitioned `inc_ful_flag=I|F / file_date=yyyy-MM-dd
/ file_time=HH.mm.ss`) through the framework pipeline into
`bluestar_current.<table>` by keyed merge. The source is the raw layer
(`raw.mode = SOURCE`, H11); `bluestar_raw` hosts only the control tables.
Each run picks up partitions newer than a stored watermark over
`(file_date, file_time)` minus a lookback. A physical-copy mode (`COPY`)
remains the framework default for feeds that need their own RAW.

**Decisions already taken (2026-09-29 discussion):**
- Incremental selection: watermark on partition values, not a date range
  from Control-M and not a full re-read.
- `inc_ful_flag` is informational — `I` and `F` partitions merge identically.
- Data lands in a framework RAW table first (replay, retention, lineage), then
  curated. Not straight-to-curated.
- **2026-09-30:** partition **lookback is in scope by default** — each run
  re-reads partitions newer than `(watermark − N)`, JDBC's `overlap` pattern
  applied to partitions. Chosen over a strict watermark (loses late files for
  an older `file_date`) and over a processed-partition set ledger (breaks
  `watermark_continuity`, forgoes the reset/retention procedures). See
  `HIVE_SOURCE_SCENARIO_MATRIX.md` §0.1.
- **2026-09-30:** `raw.mode = SOURCE` — the Hive source *is* the raw layer
  for the bstar feeds; nothing is copied into `bluestar_raw`, which hosts
  only the control tables. Both modes are framework capabilities: `COPY`
  (default, today's behavior) and `SOURCE` (H11). Chosen because the same
  team owns both databases, so a copy would buy isolation nobody needs.
- **2026-09-30:** the bstar source tables are loaded **once a day, in the
  morning**. That sizes `lookback` and the Control-M cadence for these two
  feeds; it is **not** a framework assumption — the source must keep
  handling several partitions per day, late and out-of-order deliveries,
  and `F` snapshots whenever they arrive.
- **2026-10-01:** `cdc_src_actn_cd` vocabulary is `DL` (delete), `PT`
  (insert), `UP` (update) — `deletes.mode = SOFT` with
  `indicator_values = ["dl"]`. Partitions are **never rewritten in place**;
  lookback exists for late files, not for rewrites.
- **2026-10-01:** `bstar_raw` is a **Sqoop landing from the Bluestar DB2
  database**: the `cdc_*` columns come from DB2 change capture, `F`
  partitions are full Sqoop extracts, `I` partitions the daily deltas, and
  the DB2 `CHAR` padding and microsecond `TIMESTAMP` precision arrive
  through Sqoop unchanged. The framework reads the landing rather than DB2
  directly — one extraction, one source of truth that consumers already
  query, no second load on DB2. Consequence: a Sqoop run that never lands
  a file is invisible to every in-run check (a zero-partition run is a
  healthy run) — see the H10 monitoring note.

**Global constraints honored:** Scala 2.12 / Spark 3.5 / Java 11 unchanged;
no library upgrades; every new config key is opt-in; existing tests never
modified to force a pass; no credential/PII logging; docs, reference config
and DDL land in the same change as the code.

**Open question carried into H10 (gating):** the owner has confirmed the
source loads once a day in the morning (sizes `lookback` at a few days),
that partitions are **never rewritten in place**, the action-code
vocabulary (`DL` / `PT` / `UP`), and both business keys (from the CDC key
markers). Nothing gating remains. Inside the lookback window late
partitions are handled by re-reading; beyond it, `--stage reconcile` is
the detector.

---

## Target configuration

What the feed looks like when done. Everything outside `source {}` is
existing, unchanged framework config.

```hocon
feeds {
  bstar_<table> {
    entity = "bstar_<table>"
    mode   = "INCR"

    include required("bstar-<table>-schema.conf")   # mandatory — see H9

    source {
      type     = "hive"
      system   = "bstar"              # -> raw.source_system
      database = "bstar_raw"          # -> raw.source_database
      table    = "<table>"            # -> raw.source_table
      # Optional extra scoping. Partition columns ONLY (see H3).
      # where  = "inc_ful_flag IN ('I','F')"
      incremental {
        watermark_columns = ["file_date", "file_time"]   # partition cols, in order
        initial_value     = "1900-01-01|00.00.00"        # WatermarkValue serialization
        # Re-read partitions newer than (watermark − N) every run, so a late
        # file for an older file_date, a partition registered before its
        # files landed, or an in-place rewrite inside the window all land.
        # N comes from the source owner: "how late can a file arrive?"
        lookback          = { days = 3 }                 # or { partitions = N }
        # bstar loads once a day in the morning; 3 days covers a late or
        # re-run delivery with margin. The framework does not assume a
        # cadence — several partitions a day are handled identically.
        watermark_formats = ["yyyy-MM-dd", "HH.mm.ss"]   # strict: the length guard alone lets 99.99.99 through
        watermark_store { type = "hive", database = "bluestar_raw" }
      }
    }

    raw {
      mode = "SOURCE"            # the Hive source IS the raw layer; nothing is
                                 # written (H11). COPY (default) writes a
                                 # physical RAW table as every feed does today.
      record_hash      = true    # stamped on the in-memory frame; curated keeps it
      lineage_extended = true
      # Under SOURCE: database/table are FORBIDDEN (nothing to name);
      # delivery_mode / idempotency_key / partitioning do not apply (nothing
      # is written); retention.raw is rejected (CFG_028). Lookback re-reads
      # simply re-feed curated, where freshness absorbs them.
      #
      # COPY-mode equivalent, for a feed that needs its own physical RAW:
      #   mode = "COPY"
      #   database = bluestar_raw;  table = <table>
      #   delivery_mode = "DEDUPLICATED_APPEND"
      #   idempotency_key = ["<key>", "file_date", "file_time"]
      #   partitioning { keys = ["ingest_dt"]; derive { ingest_dt = "date_format(current_timestamp(), 'yyyy-MM-dd')" } }
    }

    curated {
      database = bluestar_current
      table    = <table>
      merge {
        # Confirmed 2026-10-01 from the source CDC key markers (DB2 PK):
        #   priv_addr:    corp_ent_cd, acct_grp_nbr, sub_seq_nbr, mem_nbr, addr_seq_nbr
        #   sub_prem_det: corp_ent_cd, acct_nbr, ben_agmt_nbr, sub_seq_nbr,
        #                 sub_prm_hst_sq_nbr, sub_prm_eff_sq_nbr, sub_prm_det_sq_nbr
        keys = ["<per table, above>"]
        # CDC feed: the capture timestamp is the freshness. Samples show
        # yyyy-MM-dd HH:mm:ss.SSSSSS (fixed width), so compare_as = timestamp
        # and string tie-breaks are both safe. The partition tuple breaks
        # ties: later delivery wins.
        freshness {
          column       = "cdc_src_last_updt_ts"
          compare_as   = "timestamp"
          tie_breakers = ["<table>_lcts", "file_date", "file_time"]
        }
        # Source deletes are signaled by cdc_src_actn_cd — owner-confirmed
        # vocabulary (2026-10-01): DL = delete, PT = insert, UP = update.
        # The match is lower(trim(value)), so the CHAR(2) padding and case
        # are irrelevant; the value must be lowercase here.
        deletes {
          mode             = "SOFT"
          indicator_column = "cdc_src_actn_cd"
          indicator_values = ["dl"]
        }
      }
      dedup { order_by = ["cdc_src_last_updt_ts", "<table>_lcts"] }
    }

    # audit / rejects / concurrency / reconcile / retention / notifications:
    # identical to docs/examples/smartiq_pdp/params/feed-smartiq-pdp.conf
  }
}
```

The source partition columns (`inc_ful_flag`, `file_date`, `file_time`)
arrive in RAW as ordinary data columns; RAW is partitioned by `ingest_dt`
only, so retention partition-drops and `--resume-ingest-dt` keep working.
They must be declared in the contract so the change-detection hash ignores
them and curated alignment keeps them (H9 has the why):

```hocon
schema {
  columns = [
    { name = "file_date",    type = "string", required = true, category = "audit" },
    { name = "file_time",    type = "string", required = true, category = "audit" },
    { name = "inc_ful_flag", type = "string", required = true, category = "audit" },
    # ... business columns ...
  ]
}
```

---

## What already exists and is NOT touched

Cited so the reviewer can confirm nothing here is a change.

- **Source contract.** `Source { sourceType; read(spark, conf) }` and the
  `SourceRegistry` (`source/Source.scala:10-39`). Registration is one line in
  `IngestMain.registerConnectors` (`ingestion-app/.../IngestMain.scala:410-415`).
- **Watermark contract.** `WatermarkAdvancing { advanceWatermark; lastWindow;
  discardWindow }` (`source/WatermarkAdvancing.scala`). The *pipeline* owns the
  policy — advance only after a curated publish (or `watermark.advance_after =
  RAW`), hold on `rejects.on_reject_watermark = HOLD`, discard on
  BACKFILL/RAW_REPLAY and on failure (`pipeline/IngestPipeline.scala:364-415`,
  `:82`). A source implements three methods and inherits all of it.
- **Execution context.** `entity` and `run_id` are injected into the source
  config before `read` (`IngestPipeline.scala:480-482`); the read is
  `source.read(spark, effectiveSource)` for non-file sources (`:668-685`).
- **Extract window → ledger.** `lastWindow` feeds `audit.setExtractWindow`
  (`:511-519`) → `window_start`/`window_end` on every run-audit row → the
  `watermark_continuity` reconciliation check runs unchanged.
- **Lineage.** `source_system/database/schema/table` come from
  `source.system/database/source_schema|schema/table` (`:528-536`); the
  config above sets them. `source_operation` is `raw.source_operation_default`
  (default `UPSERT`), *not* derived from the pattern (`:574-581`) — no change.
- **RAW + CURATED.** `RawMetadata` stamping, `HiveSink`, `CuratedService`
  merge / freshness / dedup / publish valves — untouched. `compare_as` accepts
  any Spark cast type (`stage/CuratedService.scala:473`), so `"string"` is
  valid for lexicographic partition values.
- **Retention.** `watermarks_keep_last` reads
  `source.incremental.watermark_store.{type,database,table}`
  (`retention/RetentionService.scala:127-139`). With the config shape above it
  trims the Hive feed's watermark history with zero changes — one of two
  reasons H1 recommends reusing the existing store.
- **Test infrastructure.** The in-memory catalog handles `PARTITIONED BY`,
  `SHOW PARTITIONS` and `DROP PARTITION` (proven by
  `ingestion-app/src/test/.../RetentionServiceSpec.scala`); no Hive metastore
  is needed for integration tests.

## What blocks a `hive` source today

- No `hive` source type (`FileSource`, `JdbcSource`, `KafkaSource` only).
- The existing curated-only read path hard-requires an `ingest_dt` partition
  and `--resume-ingest-dt` (`stage/RawStageRunner.scala:62-77`); the external
  table has neither.
- Watermark primitives live in `ingestion-jdbc`
  (`jdbc/watermark/Watermarks.scala:15-38` for `WatermarkValue`,
  `jdbc/watermark/WatermarkStore.scala` for the store); core modules cannot
  depend on jdbc.
- `IngestionPattern.derive` sends every non-jdbc source to `FULL_SNAPSHOT`
  with `extractionMode = FILE` (`config/IngestionPattern.scala:74-89`), which
  mislabels an incremental Hive feed and will trip `CFG_011` on any explicit
  `ingestion.pattern`.
- `--stage reconcile` instantiates
  `com.hcsc.generic.ingest.jdbc.reconcile.SourceReconciliationService`
  unconditionally (`IngestMain.scala:351-352`), which parses a JDBC config —
  it throws for a Hive source. The Control-M RECON job would fail.
- The pipeline has no way to treat the source as the raw layer: `runRaw`
  always writes through the sink, the decoupled curated reader always reads
  a RAW slice by `run_id` (`IngestPipeline.scala:154`), and the
  `raw_equals_accepted` / run-id idempotency checks assume a physical
  write. A `SOURCE` mode (H11) is new capability.

---

## Work items

### H0 — Decision: where the Hive watermark store lives

Two precedents exist in the codebase and they disagree.

**Option A (recommended) — move the jdbc-agnostic watermark types to core.**
`WatermarkValue`, `WatermarkStore`, `VersionedWatermark`,
`WatermarkCommitDetail`, `WatermarkConflictException`, `HiveWatermarkStore`,
`InMemoryWatermarkStore` have no JDBC coupling: `WatermarkValue` is
`Seq[String]` with escaped-pipe serialization; `HiveWatermarkStore` already
reaches back into core for `HiveTables.ensure` (`WatermarkStore.scala:98`).
Only `WatermarkStores.from(cfg: WatermarkConfig, ...)` (`:152-162`) and the
SQL-predicate builder `Watermarks` depend on jdbc types, and both stay put.
- Operators keep ONE watermark table (`ingest_watermarks`), one history
  query, one reset procedure (`OPERATIONS_RUNBOOK.md` §"Resetting a
  watermark", §2 "Watermark operations") — the runbook applies to the Hive
  feed as written.
- Retention's `watermarks_keep_last` works with no change (above).
- Optimistic CAS on commit (`recordIfVersion`, JDBC_005) comes for free as a
  second line of defense behind the entity lock.
- Cost: a mechanical package move + import updates across `ingestion-jdbc`;
  the 15 existing JDBC test suites are the regression gate.

**Option B — Kafka precedent: a self-contained ledger in `ingestion-hive`.**
`KafkaSource` implements `WatermarkAdvancing` with its own
`KafkaOffsetLedger` (`ingestion-kafka/.../KafkaOffsetLedger.scala:21-43`) —
`HiveTables.ensure` + append, no versioning, relies on the entity lock; the
module depends only on core. Zero blast radius on jdbc, but a second
watermark table, a forked runbook procedure, and retention trimming would
need its own hook.

Take A. B is the fallback only if the H1 move surfaces coupling this
analysis missed.

### H1 — Move watermark primitives to `ingestion-core`

**Status: DONE 2026-09-30** on branch `h1/watermark-to-core` — full reactor
green at 890 tests / 0 failures (baseline 884 + 6 new), fat-jar layout
verified, no reference to the old package remains. Lessons recorded below
and in the scenario matrix (R-05): config-gen was outside the inventory;
never resume a cross-module move with `-rf` (core resolves from `~/.m2`).

- New package `com.hcsc.generic.ingest.watermark` in core with:
  `WatermarkValue` (split out of `Watermarks.scala` — it is currently
  co-located with jdbc predicate code), `WatermarkStore` trait,
  `VersionedWatermark`, `WatermarkCommitDetail`, `WatermarkConflictException`,
  `HiveWatermarkStore`, `InMemoryWatermarkStore`.
- `ingestion-jdbc` keeps `Watermarks` (predicates), `WatermarkConfig`, and a
  thin `WatermarkStores.from(cfg, spark)` adapter that delegates to the core
  types. jdbc imports updated; **no behavior change**.
- Error-code text stays (`JDBC_003` identifier checks, `JDBC_005` conflict)
  so existing operator docs and log greps remain valid. Do not renumber.
- **Blast radius (verified, then corrected in execution):** 3 `ingestion-jdbc`
  main files (`Watermarks.scala`, `WatermarkStore.scala`, `JdbcSource.scala`),
  **plus 1 `ingestion-config-gen` main file** —
  `confgen/validate/DryRunValidator.scala:6` imports `WatermarkValue`;
  config-gen depends on jdbc and the inventory missed it (caught by the
  full-reactor compile, not by the module-scoped gate). 11 `ingestion-jdbc`
  test files and 4 `ingestion-app` test files reference the moved types —
  all import-path rewrites. `WatermarkCodecsTest` exercises
  `Watermarks.predicate/compare` with dialects, not `WatermarkValue`; it
  stays in jdbc.
- **Tests:** two `WatermarkValue` round-trip tests already exist and **move
  with the type** — `WatermarkCodecsTest:89` ("round-trips values containing
  the delimiter and backslashes", plus legacy unescaped at :102-103) and
  `WatermarksTest:103` ("serialization round-trips composite values"); the
  rest of those two files stays in jdbc. `JdbcSourceH2Test:203, :206` name
  the moving types **fully qualified** and must be rewritten. Four files
  assert on `JDBC_003`/`JDBC_005` message text (`HiveWatermarkStoreSpec:70-74,
  :86`, `JdbcSourceH2Test:184`, `PartitionStrategiesH2Test:80`,
  `BoundedFullLoadTest:49`) — the strings are the contract. Gate = the full
  `ingestion-jdbc` suite green with import rewrites only, plus
  `HiveWatermarkStoreSpec` and `HiveWatermarkDuplicateVersionSpec` in
  `ingestion-app`. Untested today and worth one new test each while the
  code is open: `WatermarkStores.from` dispatch and its `JDBC_003` paths;
  and the legacy 5-column `ingest_watermarks` shape (`HiveTables.ensure` is
  `CREATE IF NOT EXISTS` only, so a store created before
  `lower_value`/`query_hash` fails positional `insertInto` — pre-existing,
  not hive-specific).

### H2 — New module `ingestion-hive`

**Status: H2–H4 DONE 2026-09-30** on branch `h2-5/hive-source`, commit
`8df162f` — module, `HiveSource`, `HiveSourceConfig`, `PartitionPredicate`,
42 tests including the source-table invariant; full reactor 932 / 0,
every pre-existing module unchanged; fat jar carries the package.

- Mirror `ingestion-file/pom.xml` exactly (parent, `ingestion-core`,
  `scala-library`, `spark-sql`, scalatest; scala-maven / scalatest / surefire
  plugins). No new third-party dependency.
- Root `pom.xml`: add `<module>ingestion-hive</module>` after
  `ingestion-kafka`; add the `<dependencyManagement>` entry beside
  `ingestion-file` (`pom.xml:65-69`).
- `ingestion-app/pom.xml`: add the dependency beside `ingestion-kafka`
  (`:29-32`). The assembly is `jar-with-dependencies` (`:82-108`) — no
  descriptor change.
- `IngestMain.registerConnectors`: `HiveSource.register()`.
- Copy the per-module `SharedSparkSession` test mixin as `ingestion-file` and
  `ingestion-jdbc` do.

### H3 — `HiveSource.read`

**Status: DONE** — see H2. Implemented as specified, with `readWindow`
for H11 and the fingerprint carried in `WatermarkCommitDetail.queryHash`
under a `pk:` prefix.

`object HiveSource extends Source with WatermarkAdvancing`,
`sourceType = "hive"`. Same `readWindows: ConcurrentHashMap[(entity,runId)]`
pattern as `JdbcSource` (`JdbcSource.scala:36-44`) and `KafkaSource`
(`KafkaSource.scala:79`). Copy JDBC's, not Kafka's: **`ingestion-kafka` has
no test directory at all**, so the Kafka path is a shape, not verified
behavior.

1. **Config** (`HiveSourceConfig.parse`): `database`, `table` (SQL
   identifiers, `ConfigUtils.requireSqlIdentifier`), optional `where`,
   `incremental { watermark_columns, initial_value, watermark_formats?,
   watermark_store }`. `watermark_columns` is explicit — no fallback to the
   contract's `incremental = true` flags (a JDBC convenience,
   `JdbcSourceConfig.scala:418-427`; `CFG_023` enforces this). `entity`,
   `run_id` and `run_mode` come from the pipeline-injected keys (**not**
   `mode` — `source.mode` is JDBC's extraction-mode key; see H5).
   `--validate-only` reads through `source.read` **without** injecting them
   (`IngestPipeline.scala:245`, vs `:480-482` for a real run), so a missing
   `entity` is **validate mode**, not an error: run steps 2–6, skip the
   watermark load, return a schema-only frame. `HIVE_001` fires only for
   `run_id` present with `entity` absent (a pipeline bug, never a user path).
2. **Resolve the table.** `spark.catalog.tableExists` else `HIVE_002`.
   Partition columns via `spark.catalog.listColumns(...).filter(_.isPartition)`;
   every `watermark_columns` entry must be one of them **and STRING-typed**
   → `HIVE_003` (a non-string partition column would compare numerically
   and break the lexicographic contract). Record the full partition-key set
   in the first commit's detail and WARN when it changes later — a new
   partition column silently widens what "greater than the watermark"
   selects. Read-time checks; `FeedCompatibilityValidator` cannot see the
   catalog.
3. **Load the watermark.** `HiveWatermarkStore.latestVersioned(entity)`;
   absent → `WatermarkValue.deserialize(initial_value)`. Arity must equal
   `watermark_columns.size` → `HIVE_005`. Validate mode uses `initial_value`.
   **`HIVE_007` gate here:** `run_mode = FULL` with a stored watermark
   present is refused (see H5).
4. **Build ONE composite predicate**, strictly greater than the **rewound**
   watermark, lexicographic over the tuple in `watermark_columns` order:
   `(c1 > v1) OR (c1 = v1 AND c2 > v2) …`, AND-ed with `where`. The rewind
   is `lookback`: `{ days = N }` sets the lower bound to
   `(minus_days(wm.file_date, N), "")` — every partition on or after that
   date; `{ partitions = N }` walks N enumerated tuples back from the
   watermark (`initial_value` if fewer exist). The rewound bound is used
   **only** for selection; the window recorded on the ledger is the stored
   watermark (step 7), exactly as JDBC records its overlap
   (`IngestPipeline.scala:1037-1044`), so `watermark_continuity` is
   untouched. Re-read rows re-append into RAW under the new `run_id` and the
   curated freshness merge absorbs them; `raw_overlap_reread` records the
   volume; `DEDUPLICATED_APPEND` (target config) removes even the RAW
   duplicates. `where` may
   reference partition columns only (`HIVE_004`): if it ran on rows after
   the read, the committed upper could come from a partition the filter
   excluded and the next run would skip real data. Built once as a Spark
   `Expression`, used twice (steps 5 and 8).
5. **Enumerate matching partitions on the driver.**
   `spark.sessionState.catalog.listPartitionsByFilter(TableIdentifier(table,
   Some(database)), Seq(predicate))` → structured `spec: Map[String,String]`
   without touching data. On HMS the shim pushes `=, <, >, <=, >=` and
   `And/Or` on STRING and integral partition columns, so the tuple
   comparison prunes server-side; the in-memory catalog serves the same API
   client-side. Fall back to `listPartitions` + driver filter on exception.
   Two production settings for the runbook:
   `spark.sql.hive.metastorePartitionPruningFallbackOnException` defaults to
   `false` (a pushdown failure throws rather than degrading), and
   `hive.metastore.limit.partition.request` can reject an unbounded
   `listPartitions` on a large table — the filtered call is what keeps this
   cheap. Structured specs suit the three-key layout better than
   `RetentionService`'s `SHOW PARTITIONS` string parsing (`:146`). Empty
   result → log "no new partitions", record a window of `(lower,
   Some(lower))`, return an empty frame with the table's schema; the
   pipeline records a clean zero-row run and `advanceWatermark` is a no-op.
6. **Format guard.** For each watermark column, all selected values must
   have equal length (`HIVE_006`) — the cheap proxy for "zero-padded":
   `2025-01-07` vs `6.24.07` fails it. Optional `watermark_formats` makes it
   a strict `DateTimeFormatter` parse instead.
7. **Capture the window.** `readWindows.put((entity, runId),
   ReadWindow(lower = the stored watermark — never the rewound bound,
   upper = max selected tuple, version))`. The upper comes
   from the driver-side enumeration, **not** from the accepted frame — a
   partition whose every row is rejected still advances (subject to
   `rejects.on_reject_watermark`) instead of being re-read forever.
8. **Read exactly the selected partitions.** `spark.table(db.table)
   .filter(predicate)` — the same expression as step 4, so partition pruning
   at the catalog opens no file of an unselected partition. One pruned scan;
   never a per-partition union. Pin `spark.sql.files.ignoreMissingFiles`
   explicitly for this read and log every selected partition that returned
   zero rows: a registered partition whose files were deleted — or have not
   landed yet — must be visible, because the upper still advances past it.
9. **Apply the schema contract.** Contract resolution is *source-internal*,
   not a pipeline step: `JdbcSource.applyContract` (`JdbcSource.scala:496-515`)
   runs `SchemaValidator.validateHeaders` → `enforce` → applies
   `resolution.renames` via `withColumnRenamed` → adds missing
   `optionalColumns` with defaults; `FileSource` does the same
   (`FileSource.scala:123, 192-258`). The pipeline's
   `validateContractBeforeRaw` (`IngestPipeline.scala:499`) runs *after* and
   assumes canonical names. `HiveSource` must do exactly this or any
   contract with aliases fails `HDR` on every run. Copy JDBC's ~20-line
   sequence; extracting a shared core helper is optional cleanup, not a
   prerequisite (File already has a divergent copy).
10. **Never** stamp framework columns here (`RawMetadata` does that and
    `RAW_003` guards collisions). `source_file` is left to `RawMetadata`
    (`file_id` is nulled for non-file sources at `IngestPipeline.scala:586`).

**`--mode` is ignored for partition selection.** `mode` never reaches a
source today (`IngestPipeline.scala:43-44` feeds it only to `RunContext`); it
decides whether curated is a keyed merge or a full-overwrite publish
(`CuratedService.scala:210`). That is the smartiq trap the runbook already
documents for JDBC (`OPERATIONS_RUNBOOK.md:1010-1016`): a mid-life `FULL`
publishes *just the watermark delta* as the entire curated table. Selection
here is watermark-only under both modes, and `HIVE_007` refuses `FULL` unless
the store is empty for the entity.

### H4 — `HiveSource` commit path

**Status: DONE** — see H2. The ledger fallback declines to re-commit when
the store is already at or beyond the ledger `window_end`, so `--resume`
and `--pending` are idempotent.

- `advanceWatermark`: remove the window; `recordIfVersion(entity, upper,
  runId, version, WatermarkCommitDetail(lower = Some(lower.serialized),
  queryHash = None))`. A `WatermarkConflictException` propagates — the run
  has already published, and the pipeline's ownership check
  (`ensureOwnership`, `IngestPipeline.scala:368`) means this is a genuine
  race worth failing loudly. **When the in-memory window is absent**
  (`--resume`, `--stage curated --pending`, any fresh JVM) do not silently
  skip as JDBC does today: fall back to the ledger's persisted `window_end`
  for this `run_id` (an `audit.lastRawWindow`-style query on
  `ingest_run_audit`) and commit that. Otherwise every recovery path leaves
  the watermark behind and the next run re-reads the window — a limitation
  JDBC still carries.
- `lastWindow`: `(lower.serialized, Some(upper.serialized))` from the
  captured window — lands as `window_start`/`window_end` so
  `watermark_continuity` works and the reset hint at
  `IngestPipeline.scala:1106-1119` (which compares against
  `source.incremental.initial_value`) applies verbatim.
- `discardWindow`: remove the entry. Called by the pipeline on failure, HOLD
  and intent-override paths — nothing else to do.

### H5 — Validation and pattern derivation

**Status: DONE 2026-09-30** on branch `h2-5/hive-source` — pattern branch,
`CFG_023/024/025/027`, `run_mode` + `audit_database`/`audit_run_table`
injection (never `mode`), exit-code classes, docs rows. Golden tests for
the two previously untested `IngestionPattern` branches were written
before the change and pass after it. Review finding fixed in the same
change: the `HIVE_` → CONFIGURATION rule matched `ARCHIVE_…` paths; now
letter-bounded, with a regression test. Full reactor 949 / 0 (core 372,
jdbc 256, app 175, hive 42; others unchanged).

- `config/IngestionPattern.derive`: add `case "hive" => "HIVE"` to
  `extractionMode` (`:74-78`) and a derived pattern
  `PARTITION_INCREMENTAL` (new constant, added to `all`) when
  `source.incremental` is present, else `FULL_SNAPSHOT` (`:80-89`).
  `watermarkStrategy = "PARTITION"`. **Override `upperBoundStrategy`:**
  `derive` stamps `MAX_VALUE` for any `incremental` block (`:70-72`) — a JDBC
  concept — so the hive branch sets `"PARTITION"`. `watermarkCommitAllowed`
  logic unchanged. Without the branch an explicit `ingestion.pattern` on a
  Hive feed fails `CFG_011` and provenance records the wrong shape.
- `config/FeedCompatibilityValidator` (static, config-only):
  - `CFG_023` — `source.type = hive` with feed `mode = INCR` requires
    `source.incremental.watermark_columns` (non-empty and **explicit** — no
    fallback to the contract's `incremental = true` flags, which is a JDBC
    convenience at `JdbcSourceConfig.scala:418-427`) and `initial_value`.
  - `CFG_024` — `source.incremental.overlap`, `watermark_type`,
    `upper_bound`, `clock_zone` are JDBC concepts with no effect on a Hive
    source; reject them (mirrors `CFG_008` for file, `:86-87`).
  - `CFG_025` — `initial_value` arity must equal `watermark_columns.size`
    (the parse-time half of `HIVE_005`).
  - `CFG_027` — `source.incremental.lookback` must name exactly one of
    `days` | `partitions`, a positive integer; `days` requires the first
    watermark column to parse with the first `watermark_formats` pattern.
    Absent `lookback` = strict (no rewind), with a WARN naming the late-file
    risk.
  - Ensure the existing `CFG_009` (jdbc-incremental-into-state-deriving
    curated) is scoped to `jdbc` only and does not fire for hive.
- **`HIVE_007` — the `FULL` gate (read-time; needs the store).** `run_mode =
  FULL` on `hive` with `source.incremental` is refused unless the watermark
  store holds **no** row for the entity (first light). This turns the
  runbook's manual "watermark table must be empty" check
  (`OPERATIONS_RUNBOOK.md:394-420`) into a hard stop. Precedent: JDBC's
  `FULL_TABLE` reseed guard (`JdbcSource.scala:111-128`). It cannot be a
  `CFG_` rule — the static validator has no catalog access — and no source
  can see the run mode today (`IngestPipeline.scala:43-44`), so **inject it
  beside `entity`/`run_id` at `:480-482` under the key `run_mode`** —
  never `mode`: `source.mode` already carries JDBC's extraction mode
  (`FULL_TABLE | SELECT_QUERY | CUSTOM_SQL | INCREMENTAL`, read at
  `IngestionPattern.scala:64` and by `JdbcSourceConfig`), and the smartiq
  feed sets `source.mode = "INCREMENTAL"`; overwriting it would silently
  turn every incremental JDBC feed into a full-table extract. One line; the
  only edit to `IngestPipeline.scala` in this plan. Regression test: a JDBC
  feed with `source.mode = INCREMENTAL` still derives
  `TIMESTAMP_INCREMENTAL` afterwards.
- **Exit-code classification** (`runtime/FailureClass.scala`): `HIVE_001–007`
  → CONFIGURATION; HMS/catalog connectivity failures (`MetaException`,
  `TException`, connection refused during `listPartitionsByFilter` /
  `tableExists`) → TRANSIENT, so Control-M retries instead of paging.
  Without this every Hive failure exits unclassified and the runbook's
  retry table does not apply to the new source.
- **Golden tests before the change** (`IngestionPatternTest`): the two
  branches H5 rewrites have no test today — the `case _ => FILE`
  fallthrough and the `upperBoundStrategy = MAX_VALUE` default (`:70-72`).
  Pin both for `file`/`kafka` fixtures first, then add the `hive` branch.
- New error namespace `HIVE_001–007` (none in use; `KAF_` precedent).
  Register in the runbook error catalog (H8).

### H6 — `--stage reconcile` for a Hive source

**Status: DONE 2026-09-30** — commit `58ee3a6` on `main`: `SourceReconciler`
trait and `ReconcileCheck` moved to core, `HiveSourceReconciliation`
(`left_anti` on the whole source under `source.where`), dispatch by source
type in `IngestMain.runReconcile` **before** the lock (`CFG_026` for an
unsupported type). Full reactor 962 / 0.

`IngestMain.runReconcile` (`:344-352`) hard-instantiates the JDBC service.
For Hive both sides are Spark tables, so the key comparison is a
`left_anti` join — the same Tier 1/Tier 2 checks as
`jdbc/reconcile/SourceReconciliationService.scala:94-123`, minus the JDBC
projection.

- Dispatch on `source.type` in `runReconcile`: `jdbc` → existing service;
  `hive` → `HiveSourceReconciliation` (in `ingestion-hive`); anything else →
  `CFG_026` "reconcile is not supported for source.type = <t>" (today it
  fails with a JDBC parse error, which is the wrong message).
- Same `ReconcileCheck` names (`source_curated_cardinality`,
  `source_keys_present_in_curated`, `curated_keys_absent_from_source`) so
  the ledger, the runbook sections and the Control-M RECON job are reused
  unchanged. Source side honors the `where` scoping from H3 for the same
  reason the JDBC version honors its non-watermark filters.
- Keep `reconcile.on_mismatch = REPORT|FAIL` semantics.
- **No test drives `--stage reconcile` through `IngestMain` today** — lock
  acquire/release, heartbeat, the `reconcile` ledger rows, `notifyFailure`,
  the FAIL-throw and the `finally` are all unverified, and this item
  rewrites exactly that dispatch. Add one `IngestMain`-driven test per
  source type (`jdbc` = regression, `hive` = new) **before** touching
  `runReconcile`.

### H7 — Tests

**Status: DONE 2026-10-01** on branch `h7-9/bstar-feeds-docs` — unit and
golden coverage landed with H2–H6/H11; this item added
`HivePipelineScenarioSpec` (14 `IngestMain`-driven scenarios: HIVE_002/
003/004/007 exits, missing required column, volume floor, late partition
inside/outside the lookback + reconcile REPORT, reject HOLD/ADVANCE and the
reject ceiling, cross-JVM `--resume`/`--pending`, watermark retention) and
`BstarConfigTest` (14 canaries over the four bstar feeds, the DDL and the
hive reference config). Full reactor **1032 run / 0 failed / 0 canceled /
0 ignored / 0 pending** (core 388, file 26, jdbc 256, hive 46, config-gen
78, app 238); fat jar built.
Two findings surfaced by the scenarios, resolved 2026-10-01: (1) **a bug
in our own change, fixed** — `IngestPipeline` (`:496`) handed
`advanceWatermark` the un-injected source config, so in a NEW JVM
`--resume` published curated but never committed the watermark
(`HiveSource.recoverFromLedger` had no `audit_database`/`audit_run_table`);
it now receives `effectiveSourceConf(...)`, and the cross-JVM resume test
asserts the commit. (2) **intended guard, documented** — an EMPTY first
light exits 20 (`allow_empty = false`, nothing to build) while later empty
windows are no-ops; pinned by test and stated in the runbook's hive
first-light variant.

**Unit (`ingestion-hive`, in-memory catalog):**
- Composite predicate: single column; two columns; equal-first-column
  tie-break; watermark equal to the newest partition → empty selection.
- Lexicographic edge: `file_time = "06.24.07"` vs `"10.05.54"`; the
  equal-length guard rejecting `"6.24.07"` (`HIVE_006`).
- `where` on partition columns only; `HIVE_004` on a data column.
- `HIVE_003` when a watermark column is not a partition column; `HIVE_005`
  on initial-value arity mismatch.
- Window capture: upper is the max *enumerated* tuple, not max of returned
  rows; `discardWindow` clears; `lastWindow` shape.
- CAS: two runs, stale version → `WatermarkConflictException`.
- `WatermarkValue` escaped-pipe codec round-trip (lands in core with H1;
  no such test exists today).
- Validate mode: `read` with no `entity` enumerates and format-checks but
  never touches the store.
- `HIVE_007`: `run_mode = FULL` with a stored watermark is refused; with an
  empty store it proceeds.
- `run_mode` injection: a JDBC feed with `source.mode = INCREMENTAL` still
  derives `TIMESTAMP_INCREMENTAL`; a capturing test source asserts it
  received `entity`, `run_id`, `run_mode`.
- `IngestionPattern` goldens for the `case _ => FILE` fallthrough and the
  `MAX_VALUE` upper-bound default, added **before** the hive branch.
- `HIVE_003` rejects a non-STRING watermark partition column; a
  partition-key-set change → WARN, not error.
- `advanceWatermark` with no in-memory window commits the ledger's
  `window_end` for the `run_id`.
- `listPartitionsByFilter` throws → the fallback path is taken and logged.
- Lookback rewind: `{ days = N }` across a month boundary; `{ partitions = N }`
  with fewer than N prior partitions falls to `initial_value`; the recorded
  window lower is the stored watermark, never the rewound bound.

**Integration (`ingestion-app`, full pipeline, in-memory catalog):**
- Fixture: `PARTITIONED BY (inc_ful_flag STRING, file_date STRING,
  file_time STRING)` with a handful of `I` and `F` partitions.
- First run from `initial_value` reads everything; RAW row count, lineage
  columns (`source_database = bstar_raw`), `window_start/end` on the ledger.
- Second run with no new partitions: zero rows, watermark unchanged,
  `watermark_continuity` passes.
- Add two partitions, run: exactly those rows land; watermark = newest.
- A partition whose rows all fail a reject rule: watermark advances under
  `ADVANCE`, holds under `HOLD`.
- Curated merge by key with `freshness` on `(file_date, file_time)`: a key
  re-delivered in a later partition wins; an older re-delivery is ignored.
- `--stage reconcile` against the fixture: all three checks recorded.
- `--stage retention` with `watermarks_keep_last`: Hive feed history trimmed.
- RAW DDL missing one of `file_date`/`file_time`/`inc_ful_flag`: the run
  **fails** (contract `required = true`), not a WARN-and-drop
  (`sink/HiveSink.scala:57-63`).
- `record_hash` ignores the partition columns when the contract tags them
  `category = "audit"`: an identical row re-delivered under a new
  `file_time` hashes equal.
- One fixture on `enableHiveSupport()` with a temp warehouse (the
  `CuratedPartitioningSpec:43` / `SourceReconciliationSpec:58` pattern) to
  exercise the `listPartitionsByFilter` pushdown path. Everything else on
  the in-memory catalog, which does register and enumerate partitions —
  `RetentionServiceSpec:47-50, :97` already asserts `SHOW PARTITIONS` counts
  on it. No Hive metastore required.
- **Invariant on every scenario:** `bstar_raw`'s partition set and row count
  are unchanged afterwards — the source is never written, altered or
  dropped.
- RAW DDL with `file_time` typed DATE (any non-STRING) fails loudly, not
  silently through positional `insertInto`.
- A selected partition whose files are missing behaves per the pinned
  `ignoreMissingFiles` setting and is named in the log.
- Empty window: `ingest_watermarks` row count and version unchanged; no
  `allow_empty` error from the empty curated frame.
- `--stage reconcile` driven through `IngestMain` for `jdbc` (regression)
  and `hive` (new): lock held and released, `reconcile` ledger rows,
  notification on mismatch, FAIL-throw honored.
- `--resume --run-id` and `--stage curated --pending` in a **fresh JVM**
  advance the watermark from the ledger.
- Lookback: a late partition for an older `file_date` inside N lands on the
  next run; one beyond N is reported by `--stage reconcile`; a re-read run
  records `raw_overlap_reread`, passes `watermark_continuity`, and with
  `DEDUPLICATED_APPEND` adds no duplicate RAW rows.
- Verify (low): nothing is written to the ledger after `advanceWatermark`;
  heartbeat renewals continue during a slow (mocked) partition enumeration.
- Existing suites: `ingestion-jdbc` (H1 gate), `ingestion-core`,
  `ingestion-app` all green.

### H8 — Documentation and reference config (same change as the code)

**Status: DONE 2026-10-01** on branch `h7-9/bstar-feeds-docs` —
`feed-hive-reference.conf`; DEVELOPER_GUIDE Step 4 hive subsection + Step 5
`raw.mode`; CONFIG_REFERENCE "Source — Hive" + "Hive Incremental",
`source.type`/`source.mode`/`retention.raw` rows; CONFIGURATION_MODEL hive
example; runbook: REPORT-exits-0 sentence, §1.3 `HIVE_001–008` catalog
(absorbing the H11 `HIVE_008` section) with the MSCK and late-partition
notes, hive first-light variant, §2 tuple-serialization note and corrected
store path, `BSTAR_INGESTION`/`BSTAR_AUDIT` Control-M design (8 jobs, two
freshness monitors); root README pointers.

- `docs/development/DEVELOPER_GUIDE.md` §Step 4: new subsection
  "### Hive (partitioned table, partition-value watermark)" after Kafka
  (`:240`).
- `docs/architecture/CONFIGURATION_MODEL.md`: Hive example; incompatibility
  matrix rows for `CFG_023–026`; note that `--stage reconcile` supports
  `jdbc` and `hive`.
- `docs/operations/OPERATIONS_RUNBOOK.md`: §1 error catalog gains
  "### 1.3 HIVE_001 – HIVE_007"; §2 watermark operations gains one line
  stating the Hive feed's watermark is the serialized partition tuple and
  the reset procedure is identical; §"Initial load (first light)" gains the
  Hive variant (set `initial_value` below the oldest partition) and cites
  `HIVE_007` as the reason a mid-life `FULL` is refused rather than merely
  warned against. Three small notes: RAW's `file_type` is stamped `I|F`
  from `--mode` (`IngestPipeline.scala:44`) and is unrelated to the source's
  `inc_ful_flag`; `--explain-mapping` renders only for managed-folder feeds
  (`:235`) and is a no-op for hive; the two partition-pruning settings from
  H3 step 5. Also (found while pinning H6's golden): under
  `reconcile.on_mismatch = REPORT` a failing check records the `reconcile`
  ledger stage as **FAILED while the job exits 0** — pre-existing and
  deliberate (the ledger tells the truth, the scheduler is not paged), but
  the exit-code table must say a 0 can sit beside a FAILED `reconcile` row.
  Also: the source owner's obligation to register partitions
  (`MSCK REPAIR TABLE` or explicit `ADD PARTITION`) before files count as
  delivered — unregistered files are invisible, and a later partition
  advances the watermark past them; changing `watermark_columns` mid-life
  requires a watermark reset (`HIVE_005` otherwise); an override of
  `initial_value` is a no-op once a watermark row exists; and the exit-code
  class of each `HIVE_` code and of metastore connectivity failures, added
  to the classification table.
- `docs/examples/feed-hive-reference.conf`: full catalog of the `source {
  type = "hive" }` options, in the style of `feed-jdbc-reference.conf`.
- `README.md`: module table row and source list entry.
- `docs/QUICK_REFERENCE.md`: nothing unless the minimum-viable-feed block
  should show a second source type — judgment call at review.

### H9 — The bstar feed itself

**Status: DONE 2026-10-01** on branch `h7-9/bstar-feeds-docs` —
`docs/examples/bstar/params/` (two feeds, two contracts: 24 and 30 columns,
`raw.mode = SOURCE`, confirmed keys, `dl` soft delete, override example),
`lower-env/` (two e2e feeds + contract copies, synthetic
`source_test_data.sql`, `curated_ddl_e2e.sql`, `LOWER_ENV_TEST_PLAN.md`),
`PROD_PROMOTION.md`, README operator pointer; `scripts/run_bstar.sh`,
`bstar.env.example` (gitignored `bstar.env`), `check_source_freshness.sh`
(`SHOW PARTITIONS`, metadata only). All four feeds validate clean
(`BstarConfigTest`). No real sample value anywhere.

- `docs/examples/bstar_<table>/params/feed-bstar-<table>.conf` and
  `lower-env/` variant, cloned from the smartiq layout (control tables in
  `bluestar_raw`, explicit table names, retention, notifications).
- `ddl/curated_ddl.sql` for `bluestar_current.priv_addr` and
  `bluestar_current.sub_prem_det` — **written** (`docs/examples/bstar/ddl/`,
  committed `f9ecf10`) and handed to consumers ahead of the pipeline.
  Unpartitioned, latest-per-key, every framework audit column verified as
  stamped. **No RAW DDL for the bstar feeds** — they run `raw.mode = SOURCE`
  (H11). For a future `COPY`-mode hive feed the RAW DDL must carry the
  source partition columns as STRING data columns: `HiveSink`'s pre-created
  path drops any non-framework column absent from the target with only a
  WARN (`sink/HiveSink.scala:57-63`) and writes by positional `insertInto`
  with no cast (`:90-93`); plus the `RawMetadata.ColumnTypes` set
  (`transform/RawMetadata.scala:48-67`). That guidance lives in the
  reference config, not in a bstar file.
- **Business keys: confirmed 2026-10-01** against the source's CDC
  replication definition (key-marked columns = DB2 primary key); both match
  the proposals in the DDL. `priv_addr`: `corp_ent_cd, acct_grp_nbr,
  sub_seq_nbr, mem_nbr, addr_seq_nbr`. `sub_prem_det`: `corp_ent_cd,
  acct_nbr, ben_agmt_nbr, sub_seq_nbr, sub_prm_hst_sq_nbr,
  sub_prm_eff_sq_nbr, sub_prm_det_sq_nbr` — `tier_agrgt_seq_nbr`,
  `corp_tier_nbr`, `mem_nbr` are not keys; `addr_type_cd` is not a key
  either (owner-confirmed). Both key sets are final.
- **Sample rows (PROD, 2026-10-01) settled the formats and overturned one
  assumption.** Timestamps (`cdc_src_last_updt_ts`, `*_lcts`) are
  `yyyy-MM-dd HH:mm:ss.SSSSSS`; dates (`addr_eff_dt`, `addr_end_dt`) are
  `yyyy-MM-dd`; `file_date` / `file_time` are `yyyy-MM-dd` / `HH.mm.ss`,
  zero-padded — `watermark_formats` and `compare_as = timestamp` stand as
  configured. Changes captured around 23:00 land in the next morning's
  file (~05:00), matching the once-daily load. `cdc_src_actn_cd` is a
  two-letter code, not I/U/D as first assumed — owner-confirmed on
  2026-10-01 as `DL` (delete), `PT` (insert), `UP` (update), so
  `deletes.indicator_values = ["dl"]`; the former first-light blocker is
  closed.
  `mem_nbr` is NULL in `sub_prem_det` samples — excluded from that key;
  `tier_agrgt_seq_nbr` varied with `sub_prm_det_sq_nbr` and may belong in
  it. Sample values are PII and are not stored in the repo; only these
  derived facts are.
- `params/bstar-<table>-schema.conf` — a schema contract is **mandatory**
  here, not the optional nicety it is for smartiq, for two reasons.
  (1) Without one, `record_hash` covers every non-framework column
  (`transform/RecordHash.scala:52-53`) and `raw.record_hash_options` offers
  only `trim`/`uppercase` (`IngestPipeline.scala:866-870`), so `file_time`
  makes every re-delivery look changed; with a contract only
  `category = "business"` columns hash (`:49-51`; valid categories at
  `schema/SchemaContract.scala:299`) — tag the three partition columns
  `category = "audit"`. (2) `required = true` fails the run if a partition
  column ever goes missing from the source — and, for a `COPY`-mode feed,
  turns `HiveSink`'s WARN-and-drop into a failure.
- `watermark_formats = ["yyyy-MM-dd", "HH.mm.ss"]` is **set**, not left
  optional: the equal-length guard alone admits `99.99.99`, which sorts past
  every real time and jumps the watermark.
- **Equal-tuple duplicates** (same key, same `file_date`/`file_time` — two
  rows inside one partition) have no winner under the freshness rule.
  Identify a deterministic source column (load sequence, source timestamp)
  for `freshness.tie_breakers`; if `bstar_raw` has none, state in the config
  header that such ties resolve by row order and that `enforce_unique_keys`
  cannot catch them (the in-batch dedup has already collapsed them).
- `scripts/run_bstar.sh` or a generalization of `run_smartiq.sh`.
- Decision recorded in the config header: freshness on the partition tuple
  vs. a source column, if one exists.

### H10 — Rollout

- Confirmed with the source owner: both primary keys (from the CDC key
  markers), action codes `DL`/`PT`/`UP`, partitions never rewritten in
  place, daily morning load, `addr_type_cd` not a key. The only open
  detail, non-blocking: the largest `file_date`-to-landing gap if known
  more precisely than "once a day" (sets `lookback` margin). Record every
  answer in the feed config header.
- Lower-env: first light from `initial_value`, then an empty run, then a
  two-partition delta, then `--stage reconcile` and `--stage retention
  --dry-run`.
- Control-M: the source loads **once a day in the morning**, so INCR runs
  **once daily after the load window** (not the smartiq 30-minute cadence);
  MONITORING's freshness threshold is ~36h rather than 26h; RECON daily
  off-peak; PURGE weekly (rejects/audit/watermarks only — no RAW under
  `SOURCE`). In the `TIDLAK_MBRSHP_DATALAKE_*` folder (avoid the
  duplicate-definition situation seen on ORDER_CAPTURE_PDP). The cadence
  is a scheduling fact, not a framework assumption: a second morning file,
  a late afternoon re-delivery, or an `F` snapshot on any day are handled by
  the same run. **MONITORING must check the source side too:** the newest
  `file_date` partition in `bstar_raw` must be no older than N days. A
  Sqoop run that never lands leaves curated's latest SUCCESS fresh — the
  INCR run finds no new partition and succeeds with zero rows — so only
  the source-side age reveals it. Add `audit.reconciliation.min_accepted_rows
  = 1` only if the owner confirms a day can never have zero changes.
- Production first light per the runbook's "Initial load" section.

### H11 — `raw.mode = SOURCE`: the Hive source as the raw layer

**Status: DONE 2026-09-30** — commit `b5ba544` on `main`: `rawTarget:
Option[(db, table)]`, no sink write under `SOURCE`, `raw_count =
accepted_count`, `AuditService.rawWindow` + `WindowReplayable` for replay
from the ledger's window, `--resume-ingest-dt` → `CFG_028`, `--stage raw`
kept for DECOUPLED. R-17 goldens (`HivePipelineIntegrationSpec`, COPY cases
written before the change) unchanged. Full reactor 1004 / 0.

New capability, `hive` sources only. `COPY` stays the default and is the
unchanged path every existing feed runs; `SOURCE` is new code behind the
flag — that is what keeps the regression story intact.

- **Config.** `raw.mode = COPY | SOURCE` (default `COPY`). `CFG_028`:
  `SOURCE` requires `source.type = hive`; under `SOURCE`, `raw.database`,
  `raw.table`, `raw.delivery_mode`, `raw.idempotency_key`,
  `raw.partitioning` and `retention.raw` are rejected (nothing is written,
  so each would be a silent no-op). `record_hash` and `lineage_extended`
  remain valid — they stamp the in-memory frame and flow to curated.
- **Raw stage** (`IngestPipeline.runRaw`): under `SOURCE` skip the sink
  write; still run contract validation, reject routing, accepted/rejected
  counting and `RawMetadata` stamping; record the raw `SUCCESS` ledger row
  with its partition window (`window_start`/`window_end`). The ledger row
  is the checkpoint, not the table — dry-run already records a row while
  writing nothing (`IngestPipeline.scala:364`), so the precedent exists.
  `raw_count` records the accepted count (what would have been written) so
  `raw_count`-based expectations stay meaningful.
- **Replay** (`RawStageRunner.readFromRaw` and the `--pending` reader at
  `IngestPipeline.scala:154`): under `SOURCE`, instead of
  `spark.table(raw).filter(run_id)`, call a new
  `HiveSource.readWindow(lower, upper)` that re-reads the *source* for the
  window that run recorded. `--resume-ingest-dt` is rejected under `SOURCE`
  (there is no `ingest_dt`); `--pending`, `--replay-from/to` and
  `--resume --run-id` work via the recorded window. **`HIVE_008`** when a
  recorded window's partitions no longer exist at the source (purged since
  the run) — fail loudly; this is the one coupling cost of `SOURCE`, and it
  is the source owner's retention promise.
- **Accounting:** `raw_equals_accepted` and the run-id idempotency guard
  (`IngestPipeline.scala:597-633`) are skipped under `SOURCE`;
  `source_equals_accepted_plus_rejected`, `curated_accounts_for_accepted_rows`
  and `watermark_continuity` are unchanged. `raw_overlap_reread` is still
  recorded — it measures the lookback re-read, which exists in both modes.
- **Curated** keeps whichever lineage columns its DDL declares (the bstar
  DDL carries `inc_ful_flag`, `file_date`, `file_time`, `record_hash` and
  the framework audit set); alignment drops the rest as today.
- **Retention:** rejects, audit and `watermarks_keep_last` unchanged;
  `retention.raw` rejected. Reconcile unchanged — it already compares source
  to curated.
- **Tests (H7 additions):** `SOURCE` first light writes no RAW table and
  records the ledger row with the window; `--pending` in a fresh JVM
  re-reads the source by the recorded window and publishes; `HIVE_008` when
  a window partition is gone; `CFG_028` for each forbidden key and for a
  non-hive source; **golden:** a `COPY`-mode hive feed and the existing
  JDBC/file integration specs behave byte-identically before and after
  (R-17).
- **Cost:** the one item that touches the pipeline core. Its own PR, after
  H2–H5 (it needs `HiveSource.readWindow`) and before the bstar feed
  configs (H9).

---

## Out of scope (explicit)

- **Late partitions beyond the lookback window.** Lookback itself is in
  scope (H3 step 4). Anything arriving later than N is detected by
  `--stage reconcile` (`source_keys_present_in_curated`) and recovered by a
  watermark rewind, as for JDBC. No automatic recovery beyond N; an
  in-place rewrite beyond N is undetectable by key comparison.
- **`--mode FULL` as a watermark bypass** (read every partition, overwrite
  curated, seed-commit the max — JDBC's `FULL_THEN_INCREMENTAL` shape).
  Coherent on its own, but it would make `FULL` mean different things for
  `jdbc` and `hive`, which is a runbook hazard. `HIVE_007` gates it instead;
  revisit only if a scheduled full rebuild becomes a requirement.
- **`ingestion-config-gen` wizard support** for `hive`. The wizard knows
  JDBC/file/Kafka; adding a fourth source is its own change.
- **Non-partitioned Hive sources.** A table without partitions has no
  watermark; `FULL_SNAPSHOT` every run is possible but not this feed's need.
  `CFG_023` rejects `mode = INCR` on such a table; `mode = FULL` is allowed
  and reads the whole table.
- **A physical RAW copy for the bstar feeds.** Decided against — same owner,
  same warehouse, source retention under our control; `raw.mode = SOURCE`
  (H11) instead. `COPY` remains available for feeds that need isolation.

## Backlog (pre-existing, found during H7–H9)

Not caused by this work and not fixed by it; recorded so they are not
rediscovered.

- **`RetentionService.trimWatermarkHistory` trims every entity in the
  shared `ingest_watermarks` table** to the running feed's
  `watermarks_keep_last` (`Window.partitionBy(entity)` with one `keepLast`
  for all), so the most recently run purge wins for every entity sharing the
  table. Harmless for the bstar pair (both 10); a hazard for a shared
  control database with differing policies. Matrix R-18.
- **`scripts/sync_artifacts.sh` and `scripts/build_bundle.sh` are
  smartiq-only** (hard-coded file lists and `SMARTIQ_*` settings).
  `run_bstar.sh` carries its own repo-copy drift warning instead; a generic
  bundle/sync would take a feed manifest.

## Execution order and gating

1. **H0 → H1** first, alone, as its own PR. Gate: the **full reactor**
   compiles (config-gen depends on jdbc — a module-scoped compile missed
   it), `ingestion-jdbc` suite green with import-only test edits, plus
   `HiveWatermarkStoreSpec` and `HiveWatermarkDuplicateVersionSpec` in
   `ingestion-app`. Nothing else starts until this merges — every later
   item imports from the new package.
2. **H2 + H3 + H4 + H5** together (the source is not usable without its
   validation). Includes the one-line `run_mode` injection in
   `IngestPipeline.scala` (H5). Gate: H7 unit tests, with the
   `IngestionPattern` goldens and the `IngestMain`-driven reconcile test
   landed **first** so the "before" is pinned.
3. **H6** may run in parallel with (2) once H1 is in; it depends on
   `HiveSourceConfig` from H3 for the `where` scoping, so land after (2).
4. **H11** as its own PR after (2) — it needs `HiveSource.readWindow` and
   it is the only item that touches the pipeline core, so it carries the
   R-17 golden tests and a full-reactor gate of its own.
5. **H7 integration + H8 docs + H9 feed config** in one PR — the repo rule
   is docs in the same change as code; the reference config is the
   documentation.
6. **H10** is not a code item; it gates on the business keys and the
   source-owner confirmation of the daily-load timing.
