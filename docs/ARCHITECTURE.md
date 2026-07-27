# Architecture

Structural overview of the Data Ingestion Framework: modules, data flow,
extension points, and the invariants the code enforces. Companion docs:
[DEPLOYMENT.md](DEPLOYMENT.md), [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md).

---

## 1. Module map

Multi-module Maven build (Scala 2.12.18 / Spark 3.5.0 / Java 11). Common root
package `com.hcsc.generic.ingest`.

| Module | Package | Responsibility |
|--------|---------|----------------|
| `ingestion-core` | `com.hcsc.generic.ingest` | Framework core: the pipeline orchestrator (`pipeline/IngestPipeline`), the `Source`/`Sink` traits and their registries, schema-contract validation (`schema/`), file intake and folder lifecycle (`files/`), audit/reject/reconciliation, transactional publish (`publish/PublishService`), and the CLI model (`model/Cli`). |
| `ingestion-file` | `…ingest.file` | File source connector: reads CSV/text (local/HDFS), normalizes headers, validates against the schema contract, applies aliases/positional mapping, handles repeated headers and trailer/skip rows. |
| `ingestion-jdbc` | `…ingest.jdbc` | JDBC source connector: dialect registry (`sqlserver`/`postgresql`/`oracle`/`db2`/`mysql`/`generic`), four+ read modes, partitioning strategies, secret providers, bounded watermark incremental loading with optimistic version control, health checks and retries. |
| `ingestion-kafka` | `…ingest.kafka` | Kafka source connector: batch reads with JSON/string value parsing, key/value/metadata extraction. |
| `ingestion-app` | `…ingest.app` | Application entry point (`IngestMain`): registers all connectors, parses CLI, loads feed config, drives the pipeline. Builds the fat jar. |

---

## 2. Data flow

The orchestrator is `ingestion-core/.../pipeline/IngestPipeline.scala`. A
normal run proceeds through these stages, in order:

```
landing ─▶ VALIDATE ─▶ RAW ─▶ CURATED ─▶ PUBLISH ─▶ WATERMARK COMMIT
             │           │        │          │            │
     header/intake   source   transform  staging→     advance-only,
     validation,     read +   + derive   INSERT       version-checked
     quarantine      guard +  columns    OVERWRITE    (JDBC feeds)
     (audit-first)   split
                     accept/
                     reject
```

1. **VALIDATE** — file intake discovers files (landing + any `inprogress`
   leftovers), validates physical headers per the contract, and quarantines
   invalid files (audit written **before** the move). Never skipped on
   `--resume` because intake is idempotent.
2. **RAW** — the registered `Source` reads the data; a **hard guard** verifies
   all required contract columns exist before any write; content/data
   validation runs; rows are split into accepted/rejected; accepted rows are
   persisted to the RAW table via the registered `Sink`, stamped with
   `run_id`.
3. **CURATED** — type casting, derived columns, and audit fields are applied
   (`CuratedStageRunner`).
4. **PUBLISH** — curated output is materialized into a per-run staging table,
   validated **before** the target is touched, then swapped in with a single
   `INSERT OVERWRITE`; on failure the staging table is dropped and the target
   is untouched (`publish/PublishService`).
5. **WATERMARK COMMIT** — for incremental JDBC feeds, the watermark is
   advanced **only after RAW and PUBLISH have both succeeded**, via the
   `WatermarkAdvancing.advanceWatermark` hook (`IngestPipeline` calls it after
   publish).

Then file lifecycle completion (move/archive) and cross-stage reconciliation
run.

### Resume / validate-only

- `--resume --run-id <id>`: a stage recorded `SUCCESS` in stage audit is
  skipped; VALIDATE is never skipped. If RAW already succeeded, curated
  replays from the RAW `run_id` slice rather than re-reading the source.
- `--validate-only`: header/mapping inspection only — no RAW/CURATED write, no
  file moves, and the audit service is structurally disabled for the run.

---

## 3. Extension points

Every connector, dialect, secret source, and watermark store is discovered at
runtime through a registry, so new implementations add themselves without
touching the framework. Register during connector bootstrap (e.g.
`IngestMain.registerSources()`).

| Registry | Trait | Register | To add |
|----------|-------|----------|--------|
| `SourceRegistry` (`source/Source.scala`) | `Source` | `register(source: Source): Unit` | Implement `sourceType: String` + `read(spark, sourceConf): DataFrame`; call `SourceRegistry.register(this)`. |
| `SinkRegistry` (`sink/Sink.scala`) | `Sink` | `register(sink: Sink): Unit` | Implement `sinkType: String` + `write(spark, df, sinkConf): Unit`; call `SinkRegistry.register(this)`. |
| `DialectRegistry` (`jdbc/dialect/JdbcDialect.scala`) | `JdbcDialect` | `register(dialect: JdbcDialect): Unit` | Extend `JdbcDialect` (name, defaultDriver, urlPrefix, quoting, `selectTopOne`, …); call `DialectRegistry.register(this)`. Pre-registered: sqlserver/postgres/oracle/db2/mysql/generic. |
| `SecretProviders` (`jdbc/auth/SecretProvider.scala`) | `SecretProvider` | `register(p: SecretProvider): Unit` | Implement `name: String` + `resolve(conf): String`; call `SecretProviders.register(this)`. Pre-registered: inline/env/sysprop/file/cyberark. This is where Azure Key Vault / Databricks scopes plug in. |
| `WatermarkStores` (`jdbc/watermark/WatermarkStore.scala`) | `WatermarkStore` | selected via `WatermarkStores.from(...)` | Extend `WatermarkStore` implementing `latestVersioned` and the compare-and-set `recordIfVersion`; e.g. a transactional control-table store for strict serialization. Pre-implemented: Hive (append-only) and InMemory (tests). |

### Trait signatures

```scala
trait Source {                                  // source/Source.scala
  def sourceType: String
  def read(spark: SparkSession, sourceConf: Config): DataFrame
}

trait Sink {                                    // sink/Sink.scala
  def sinkType: String
  def write(spark: SparkSession, df: DataFrame, sinkConf: Config): Unit
}

trait WatermarkAdvancing { self: Source =>      // source/WatermarkAdvancing.scala
  def advanceWatermark(spark: SparkSession, sourceConf: Config,
                       entity: String, runId: String, accepted: DataFrame): Unit
}
```

`JdbcSource` mixes in `WatermarkAdvancing`; `FileSource`/`KafkaSource` are
plain `Source`s.

---

## 4. Key invariants

These are enforced in code, not by convention. Break one and a run fails
rather than corrupting data.

1. **Never null a required column (hard guard before RAW write).**
   `IngestPipeline.runRaw` checks that every required contract column is
   present in the DataFrame before writing RAW; a missing required column
   throws `SchemaContractViolationException` (`ViolationKind.MissingColumn`,
   surfaced as HDR_001). Schema alignment refuses to auto-create a required
   business column as null; optional columns get their configured default;
   only non-contract technical/audit columns may be null-filled.

2. **Bounded watermark windows.** For incremental JDBC reads,
   `JdbcSource.read` captures the source's upper watermark on the driver
   **before** extraction; the extraction predicate is
   `> lower AND <= captured_upper` (`watermark/Watermarks.boundedPredicate`).
   On success, the **captured** upper is committed — never the max of the
   extracted rows — so rows arriving mid-extraction can't corrupt the window.
   The commit is advance-only (never regresses) and version-checked
   (`recordIfVersion`; conflict → JDBC_005).

3. **Audit before move.** When a file fails validation,
   `IngestPipeline.handleContractFailure` writes the `ingest_header_audit`
   record **first**, then quarantines the file. If the audit write fails, the
   move is skipped and the file stays `inprogress` (restart-safe); if the move
   fails, the audit already records the intent. The audit table is always the
   authoritative "why".

4. **Advance-after-publish / idempotent RAW.** The watermark advances only
   after RAW and CURATED succeed. RAW writes are guarded by `run_id`:
   re-running the same `run_id` skips the write if rows already exist
   ("idempotent replay"), so replay is at-least-once into RAW and deduplicated
   by the curated merge.

5. **Transactional publish.** Curated data lands in a per-run staging table,
   is validated before the target is touched, and swaps in atomically; on
   failure the target is untouched and the staging table is dropped (crashed
   leftovers cleaned only after a 24h age gate, protecting concurrent runs).

---

## 5. Where to look

| Concern | File |
|---------|------|
| Pipeline orchestration, stages, resume | `ingestion-core/.../pipeline/IngestPipeline.scala` |
| Schema contract & HDR codes | `ingestion-core/.../schema/SchemaValidator.scala`, `SchemaContract.scala`, `ContentValidator.scala` |
| File intake, folder lifecycle, batch policy | `ingestion-core/.../files/FileIntakeService.scala`, `FolderLayout.scala` |
| Transactional publish, staging cleanup | `ingestion-core/.../publish/PublishService.scala` |
| JDBC read, partitioning, watermark advance | `ingestion-jdbc/.../JdbcSource.scala`, `JdbcSourceConfig.scala` |
| Bounded windows, codecs | `ingestion-jdbc/.../watermark/Watermarks.scala`, `WatermarkStore.scala` |
| Dialects, retries, secrets | `ingestion-jdbc/.../dialect/`, `read/RetryPolicy.scala`, `auth/` |
| CLI | `ingestion-core/.../model/Cli.scala` |
| Entry point | `ingestion-app/.../app/IngestMain.scala` |
