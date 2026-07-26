# Data Ingestion Framework

A configuration-driven, multi-source data ingestion framework built with Scala 2.11 and Spark 2.3.

## Architecture

This is a multi-module Maven project designed for extensibility:

```
data-ingestion-framework/
├── ingestion-core/      # Core abstractions, utilities, and shared components
├── ingestion-file/      # File-based source connector (CSV, JSON, Parquet, etc.)
├── ingestion-jdbc/      # JDBC source connector (SQL Server, DB2, Oracle, etc.)
├── ingestion-kafka/     # Kafka streaming source connector
└── ingestion-app/       # Main application entry point
```

### Modules

| Module | Description |
|--------|-------------|
| `ingestion-core` | Source/Sink traits, config utilities, partitioning, transforms, stage runners |
| `ingestion-file` | File source: CSV, JSON, Parquet with header handling, aliases, trailer removal |
| `ingestion-jdbc` | JDBC source: SQL Server, DB2, Oracle, PostgreSQL with partitioned reads |
| `ingestion-kafka` | Kafka source: batch reads with JSON/string parsing |
| `ingestion-app` | Application entry point bundling all connectors |

## Features

### Source Connectors
- **File**: CSV, JSON, Parquet, ORC with configurable delimiters, headers, multiline
- **JDBC**: Any JDBC-compliant database with parallel partition reads
- **Kafka**: Batch consumption with JSON/string value parsing

### Core Capabilities
- Configuration-driven pipeline definition (Typesafe Config / HOCON)
- **Schema contract management** with per-feed drift policies (see below)
- Header alias mapping for vendor file format changes
- Positional column assignment for headerless or unstable files
- Trailer row removal by marker text or position
- RAW metadata enrichment: `source_file`, `row_idx`, `load_timestamp`, `file_type`, `file_id`
- Dynamic partition column derivation
- Curated layer with type casting, derived columns, and audit fields
- FULL overwrite and INCR upsert-union-dedup merge strategies

## Schema Contract Management

Every feed can declare a formal, versioned schema contract:

```hocon
feeds.my_feed.schema {
  version = "2.1"
  compatibility = "BACKWARD"

  # Drift policies: FAIL | WARN | IGNORE
  on_missing_column        = "FAIL"
  on_extra_column          = "WARN"
  on_type_change           = "FAIL"
  on_order_change          = "WARN"
  on_duplicate_header      = "FAIL"
  on_nullability_violation = "FAIL"
  on_version_mismatch      = "WARN"

  columns = [
    {
      name = "subscriber_id"
      type = "string"
      nullable = false
      aliases = ["subscriber id", "subscriberid"]
      position = 0
    },
    {
      name = "hios_id"
      type = "string"
      nullable = false
      aliases = ["plan_hios_id", "hios id"]
      position = 1
    }
  ]
}
```

The framework detects and applies the configured policy to:

| Drift | Detected by |
|-------|-------------|
| Missing required columns | Header validation against contract |
| Unexpected (added) columns | Header validation against contract |
| Renamed columns | Alias resolution (logged, then mapped to canonical name) |
| Column-order changes | Actual index vs. declared `position` |
| Duplicate headers | Two headers resolving to the same column |
| Data-type changes | DataFrame schema vs. declared `type` |
| Nullability violations | Runtime null scan of non-nullable columns |
| Schema version mismatch | Contract `version` vs. the version stored on the RAW table (`ingest.schema.version` table property) |

Positional mapping is never applied silently: it requires `header = false`
or an explicit `force_columns_by_position = true`, always validates the
column count against the contract, and with headers present the headers are
still validated first so drift is surfaced before positional recovery.

Feeds without a `schema` block keep the legacy behavior
(`source.columns` / `source.header_aliases`). Legacy options still work but
log a deprecation warning — migrate to a schema contract for validated
header handling.

### Header Change Protection

The core safety rule: **the framework aliases known header changes, fails
unknown required-header changes, and never silently substitutes null for a
missing required source column.**

Headers are normalized centrally (trim, lowercase, non-alphanumerics to a
single `_`, edge underscores stripped) before any comparison, so
`" Plan HIOS-ID "`, `"plan hios id"` and `"PLAN_HIOS_ID"` all resolve
identically. Matching order: canonical name, then approved alias, then (only
under the fallback strategy) guarded positional mapping — otherwise the file
fails validation.

Matching strategies (`schema.header_validation.strategy`):

| Strategy | Behavior |
|----------|----------|
| `STRICT_NAME` | Canonical names only; aliases are ignored |
| `NAME_WITH_ALIASES` (default) | Canonical names plus approved aliases |
| `NAME_ALIAS_POSITION_FALLBACK` | Names/aliases first; if required columns are still missing, a guarded positional fallback maps by declared `position` (exact column count enforced, content validation recommended). Only safe when the vendor guarantees stable column order. |

Per-column contract options: `required` (default true), `default` (applied
when an optional column is missing, cast to the contract type), `category`
(business/audit/generated/deprecated), and content rules (`regex`,
`min_length`, `max_length`, `allowed_values`, `nonblank`) evaluated by the
content validator (`content_validation`: SAMPLE/FULL mode with a failure
percentage threshold) to catch swapped or mismapped columns.

Guarantees enforced in code, not convention:

- A hard guard runs before every RAW write: all required canonical columns
  must exist, for every source type.
- Schema alignment refuses to auto-create a required business column as
  null; optional columns get their configured default; only non-contract
  technical/audit columns may be null-filled.
- Header validation failures are persisted to `ingest_header_audit` with
  stable error codes (`HDR_001` missing required … `HDR_007` content
  validation failure) — audit is written before any file is moved — and the
  staged files move to quarantine when
  `header_validation.quarantine_on_failure = true`.

See `application.conf` for a complete Health Sherpa example where
`"Plan HIOS ID"` maps to canonical `hios_id`.

## Operational Reliability Features

All features below are opt-in per feed via config blocks; feeds without them
behave exactly as before. See the commented examples in `application.conf`
and the table schemas in `ddl/ingest_audit.sql`.

### File Validation and Quarantine (`source.folders` + `source.validation`)
Managed folder lifecycle: `landing -> inprogress -> processed` (with optional
`archive` copy). Configurable pre-read checks — filename pattern, extension
whitelist, strict encoding, min/max size, checksum sidecar verification,
header column count/names, required trailer marker. Invalid files move to
`quarantine` with every failed check recorded in the file audit.

### Content-Based Idempotency (`idempotency`)
Every staged file's SHA-256 content checksum is checked against the
`ingest_file_registry` Hive table (written only after a fully successful run,
so restarts are safe). Duplicate content — even under a new file name — is
handled per `duplicate_policy`: `SKIP` (move to processed without loading),
`REJECT` (quarantine), or `REPROCESS_WITH_APPROVAL` (left in landing until an
operator re-runs with `--force-reprocess`).

### Record Reject Handling (`rejects`)
Configurable SQL reject rules (plus rules derived from the schema contract's
non-nullable columns) split records into accepted and rejected. Rejected rows
are persisted with `run_id`, `file_id`, `source_file`, `row_idx`, the full
`raw_record` as JSON, `error_code`, `error_message`, `reject_category` and a
timestamp. `max_reject_count` / `max_reject_percent` fail the run when
exceeded. RAW receives accepted records only.

### Audit and Reconciliation (`audit`)
File-level events (validated, quarantined, skipped-duplicate, processed) and
stage-level runs (validate/raw/curated with STARTED/SUCCESS/FAILED/SKIPPED)
are persisted with source/raw/accepted/rejected/insert/update/delete counts
and an optional `control_total_expr`. After each run, cross-stage
reconciliation checks (e.g. source = accepted + rejected) are persisted to
`ingest_reconciliation`; mismatches WARN or FAIL per
`audit.reconciliation.on_mismatch`.

### Transactional Publishing (`curated.publish`)
Curated data is materialized into a per-run staging table, validated BEFORE
the target is touched (non-empty unless `allow_empty`, plus an optional
`validation_query` with a `{table}` placeholder where any returned row fails
the publish), then swapped in with a single `INSERT OVERWRITE`. On any
failure the staging table is dropped and the target is untouched. Stale
staging tables from crashed runs are cleaned up on the next publish.

### Stage Restart and Replay (CLI)

| Flag | Behavior |
|------|----------|
| `--run-id <id>` | Pin the run identifier (also stamped on RAW rows as `run_id`) |
| `--resume` | Re-run a failed run: stages already SUCCESS are skipped; curated replays from the RAW `run_id` slice without re-appending |
| `--file-id <id>` | Restrict processing to one file (name or checksum prefix) |
| `--dry-run` | Validate and audit everything, write and move nothing |
| `--force-reprocess` | Override duplicate detection for approved reprocessing |

File intake is restart-safe: files left in `inprogress` by a crashed run are
picked up automatically, and the checksum registry is only written after full
success.

## Configuration

All pipelines are defined in `application.conf`:

```hocon
feeds {
  my_feed {
    source {
      type = "file"          # file | jdbc | kafka
      path = "hdfs:///data/input/*.csv"
      # ... source-specific options
    }
    
    raw {
      database = "raw_db"
      table = "my_table"
      path = "hdfs:///warehouse/raw/my_table"
      partitioning {
        keys = ["ingest_dt"]
        derive { ingest_dt = "date_format(current_timestamp(), 'yyyy-MM-dd')" }
      }
    }
    
    curated {
      enabled = true
      database = "curated_db"
      table = "my_table"
      merge { keys = ["business_key"] }
    }
  }
}
```

## Build

```bash
mvn clean package
```

This produces:
- `ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar`

## Run

```bash
spark-submit \
  --class com.hcsc.generic.ingest.app.IngestMain \
  --master yarn \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity my_feed \
  --mode FULL \
  --conf-path /path/to/application.conf
```

### CLI Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--entity` | Yes | Feed name from config |
| `--mode` | Yes | `FULL` or `INCR` |
| `--conf-path` | No | Path to config file (defaults to classpath) |
| `--stage` | No | `all`, `raw`, `curated` |
| `--raw-flag` | No | Override file_type partition value |
| `--resume-ingest-dt` | No | Resume curated from specific RAW partition |

## Adding a New Source Connector

1. Create a new module (e.g., `ingestion-s3`)
2. Implement the `Source` trait:

```scala
object S3Source extends Source {
  override def sourceType: String = "s3"
  
  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    // Implementation
  }
  
  def register(): Unit = SourceRegistry.register(this)
}
```

3. Register in `IngestMain.registerSources()`
4. Add module dependency to `ingestion-app`

## Validation

Test in your target HDP/Spark environment before production deployment. Verify:
- Hive metastore connectivity
- HDFS paths and permissions
- JDBC driver availability (for JDBC sources)
- Kafka client compatibility (for Kafka sources)
