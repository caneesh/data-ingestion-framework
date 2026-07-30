# Data Ingestion Framework

A configuration-driven, multi-source data ingestion framework built with Scala 2.12, Spark 3.5 and Java 11.

## Documentation

- [docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md) — step-by-step guide to using the framework: build, feed definition, schema contracts, sources, RAW/CURATED, validation, local runs, cluster submit, operations.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module map, data flow, extension points, and enforced invariants.
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — build/assembly, spark-submit examples, JDBC driver and secret-provider deployment, Azure SQL notes.
- [docs/OPERATIONS_RUNBOOK.md](docs/OPERATIONS_RUNBOOK.md) — production support: HDR/JDBC error catalog, watermark and quarantine recovery, restart/resume, stale-state cleanup.
- [docs/CONFIG_GENERATOR.md](docs/CONFIG_GENERATOR.md) — interactive wizard that generates and dry-run-validates feed configurations (HOCON/JSON/YAML) for JDBC, file and Kafka sources.
- [docs/RUNNING_CONFIG_GENERATOR.md](docs/RUNNING_CONFIG_GENERATOR.md) — step-by-step guide to running the generator: build, interactive/draft/non-interactive modes, using the output with spark-submit, git hygiene, troubleshooting.
- [docs/SQL_SERVER_AUTH_AUDIT.md](docs/SQL_SERVER_AUTH_AUDIT.md) — evidence-based audit of SQL Server authentication: connection sites, credential resolution chain, CyberArk/Conjur invocation conditions, runtime config precedence, security posture and verification commands.

## Architecture

This is a multi-module Maven project designed for extensibility:

```
data-ingestion-framework/
├── ingestion-core/      # Core abstractions, utilities, and shared components
├── ingestion-file/      # File-based source connector (CSV, JSON, Parquet, etc.)
├── ingestion-jdbc/      # JDBC source connector (SQL Server, DB2, Oracle, etc.)
├── ingestion-config-gen/  # Interactive feed configuration generator (CLI wizard)
├── ingestion-kafka/     # Kafka streaming source connector
└── ingestion-app/       # Main application entry point
```

### Modules

| Module | Description |
|--------|-------------|
| `ingestion-core` | Source/Sink traits, config utilities, partitioning, transforms, stage runners |
| `ingestion-file` | File source: CSV, JSON, Parquet with header handling, aliases, trailer removal |
| `ingestion-jdbc` | JDBC subsystem: dialect registry (Azure SQL Server, PostgreSQL, Oracle, DB2, MySQL), four read modes, secret providers, watermark-based incremental loading, health checks, retries |
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

### Multi-File Batch Safety

Every physical file in a managed feed is validated **independently before
any Spark read**: the physical header is extracted with a quote-aware parser
(so duplicate physical headers are caught before Spark silently renames
them), normalized, resolved against the contract, and fingerprinted
(SHA-256 over the canonical column order). Alias-equivalent files share a
fingerprint and group together; different layouts are read separately and
unioned by canonical name — a mixed-schema batch can never contaminate one
DataFrame. Batch behavior is configurable via
`header_validation.batch_policy`: `FILE_ATOMIC` (default — invalid files
quarantine individually, valid files continue) or `BATCH_ATOMIC` (one
invalid file fails the batch). Header-only files follow
`header_only_policy` (`WARN_AND_SKIP` default / `FAIL`); repeated header
rows inside data follow `repeated_header_policy`
(`FAIL` / `REJECT_ROW` / `DROP_WITH_WARNING`).

### Error-Code Catalog

| Code | Meaning |
|------|---------|
| HDR_001 | Missing required header |
| HDR_002 | Duplicate physical header |
| HDR_003 | Duplicate normalized header |
| HDR_004 | Ambiguous alias mapping |
| HDR_005 | Multiple source columns map to one canonical column |
| HDR_006 | Unexpected header |
| HDR_007 | Column-count mismatch |
| HDR_008 | Invalid positional mapping |
| HDR_009 | Suspected delimiter mismatch |
| HDR_010 | Blank/empty header |
| HDR_011 | Malformed quoted header |
| HDR_012 | Unsupported encoding |
| HDR_013 | Header-only file |
| HDR_014 | Schema-version mismatch |
| HDR_015 | Content validation failure |
| HDR_016 | Repeated header row detected |
| HDR_017 | Contract configuration collision (startup) |
| HDR_018 | Required output column missing before CURATED publish |

## JDBC Ingestion

The JDBC subsystem is dialect-pluggable (`sqlserver`, `postgresql`,
`oracle`, `db2`, `mysql`, `generic`; register new engines via
`DialectRegistry`). Azure SQL Server enforces encrypted connections by
default. Four read modes: `FULL_TABLE`, `SELECT_QUERY` (projection + WHERE
pushed into the database), `CUSTOM_SQL`, and `INCREMENTAL`.

**Credentials** come from the secret-provider abstraction
(`env` / `file` / `sysprop` / `inline` / `cyberark` / `conjur` /
`azure_keyvault`); plaintext inline secrets log a warning. URLs are masked
in every log line. The `cyberark` provider retrieves accounts from a
CyberArk Central Credential Provider (CCP) — one vault object serves both
the user id (`attribute = "UserName"`) and the password (`Content`, the
default) from a single cached CCP call. Client-certificate authentication
to CCP uses the standard JVM TLS keystore settings
(`spark.driver.extraJavaOptions -Djavax.net.ssl.keyStore=...`); TLS
verification is never disabled, and every vault provider (`cyberark`,
`conjur`) rejects plain-http URLs unless `allow_insecure_http = true` is
set for isolated development. Provider caches are TTL-bounded
(`cache_ttl_ms`, default 5 minutes; 0 disables) so long-lived JVMs pick up
rotated secrets. `auth.user` accepts the same provider references as
`auth.password`.

**Incremental loading (bounded windows)**: `TIMESTAMP`, `NUMERIC`, or
`COMPOSITE` (lexicographic multi-column) watermarks. Every incremental read
is **bounded**: the source's upper watermark is captured on the driver
before extraction, the predicate is `> lower AND <= captured upper`, and on
success the *captured* upper is committed — a reproducible window that is
immune to rows arriving mid-extraction. The overlap window widens only the
lower edge. Watermark values are type-validated on read (JDBC_004) so a
corrupt stored value can never become SQL.

The watermark **advances only after the entire run — RAW write, CURATED
publish — has succeeded** (`WatermarkAdvancing` pipeline hook), with an
**optimistic version check**: a commit verifies the version observed at
read time and fails with `JDBC_005` if a concurrent run advanced the same
entity (the Hive store gives best-effort compare-and-set; a transactional
control table can be plugged in via a custom `WatermarkStore`). Advance-only
semantics mean overlap re-reads can't regress the position, and the full
history stays in `ingest_watermarks`. A failed publish means the next run
re-extracts the same window: restart-safe, at-least-once into RAW,
deduplicated by the curated merge.

A timestamp-only watermark with no overlap can miss equal-timestamp rows;
the framework warns by default and rejects it when
`incremental.on_unprotected_watermark = "FAIL"` — prefer a `COMPOSITE`
watermark with a primary-key tie-breaker.

### JDBC retry model (three layers)

| Layer | Covers | Mechanism |
|-------|--------|-----------|
| Driver-side retry | Health checks, schema fetch, upper-watermark capture, watermark-store ops | `RetryPolicy`: bounded exponential backoff + jitter, retrying **only transient categories** classified by `SqlFailureClassifier` (SQLState, Azure SQL vendor codes, cause chain). Auth, syntax, missing-object and unknown errors fail immediately. |
| Executor partition reads | Connection resets / throttling during extraction | Spark task retry (`spark.task.maxFailures`) — **not** the framework wrapper; wrapping `DataFrameReader.load()` cannot retry executor reads. |
| Whole-run restart | Anything else | Pipeline restart: the watermark never advanced, so replay re-extracts the same bounded window idempotently. |

Note: the driver health check proves driver→database connectivity only;
executors open their own connections, so firewall rules must cover executor
egress as well.

**Hardening**: atomic partition configuration (all of
`numPartitions`/`partitionColumn`/bounds or none, `lower < upper`, capped by
`max_partitions`), `fetchsize` control, sanitized logging (query hash by
default; full SQL only under `diagnostics.log_sql = true`), and schema drift
detection through the same schema-contract system files use.

### Contract-Based Query Model

Read modes: `FULL_TABLE` (bare table; configured projections/predicates are
rejected rather than silently ignored), `SELECT_QUERY`, `CUSTOM_SQL`,
`SQL_TEMPLATE`, `INCREMENTAL`. Configuration renders through the dialect —
never raw string concatenation:

```hocon
columns = [
  { source = "Member ID", target = "member_id" },              # -> [Member ID] AS [member_id]
  { expression = "CONVERT(varchar(10), [dt], 23)", target = "effective_date" }
]
filters = [
  { column = "state", operator = "=", value = "IL" },          # whitelisted ops, typed literals
  { column = "amount", operator = ">=", value = "100", type = "NUMBER" }
]
parameters = [                                                  # SQL_TEMPLATE :name placeholders
  { name = "region", type = "STRING", value = "WEST" },
  { name = "floor", type = "NUMBER", from = { provider = "env", key = "MIN_AMOUNT" } }
]
```

Identifier quoting is segment-aware (`dbo.member` → `[dbo].[member]`);
literals validate through a typed round-trip (STRING/NUMBER/TIMESTAMP/DATE/
BOOLEAN). Custom and templated SQL must be a single `SELECT`/`WITH`
statement — semicolons, multi-statements and DDL/DML are rejected at
startup. `expression` fields and legacy `where` strings remain the trusted
administrator escape hatch, surfaced in the audited query hash. Legacy
string `columns` render exactly as before.

### Azure Authentication

`auth.type` (pluggable via `JdbcAuthenticationProviders.register`):
`SQL_PASSWORD` (default), `MANAGED_IDENTITY` (+`client_id`),
`ENTRA_SERVICE_PRINCIPAL` (`client_id` + secret-backed `client_secret`),
`ENTRA_PASSWORD`, `ENTRA_DEFAULT` (DefaultAzureCredential chain),
`ENTRA_INTEGRATED` (Kerberos/Windows), `ACCESS_TOKEN` (secret-backed) —
legacy alias names remain accepted. Incompatible fields are rejected and
provider-resolved secrets must be non-blank.
Azure modes map onto Microsoft JDBC driver properties
(`authentication=ActiveDirectoryMSI` etc. — the driver performs the token
flows; no Azure SDK required) and are sqlserver-only. Prefer MSI/service
principal for long extractions: a pre-fetched access token can expire
mid-job. Azure Key Vault / Databricks scopes plug in by registering a
`SecretProvider`.

### Partitioning Strategies

| `partition_strategy` | Behavior |
|----------------------|----------|
| `STATIC_RANGE` (default) | All four of `numPartitions`/`partitionColumn`/`lowerBound`/`upperBound`, atomic |
| `MIN_MAX_QUERY` | Bounds discovered per run via a driver-side `MIN/MAX` query (honors `where`); degenerate ranges fall back to an unpartitioned read |
| `PREDICATES` | `partition_predicates = [...]` — one partition per predicate, for skewed or non-numeric keys |

`skew_metrics = true` logs rows-per-partition and a skew ratio (costs one
extra pass) and warns above 3x. `health_check.executor_probe = true` runs a
rate-limited connectivity check from executor hosts (the driver health check
only proves the driver's own network path). Watermark codecs cover
`TIMESTAMP`, `NUMERIC`, `DATE`, `DATETIMEOFFSET` (offset-aware) and
`ROWVERSION` (unsigned hex) — usable in composite watermarks.

Metrics counters (`jdbc_connection_attempt_total`, `jdbc_retry_total`,
`jdbc_watermark_commit_total`, `jdbc_watermark_conflict_total`,
`jdbc_schema_drift_total`, ...) accumulate in-process via `JdbcMetrics`;
wire `JdbcMetrics.snapshot` into your metrics agent at the deployment
boundary.

**Deployment**: the Microsoft JDBC driver must be present on the driver AND
executors (cluster library, `--jars`, or bundled per policy) — a missing
driver fails fast with `JDBC_001`. SQL Server-specific integration tests
(datetime2 precision, real AAD flows, encryption handshakes) require a
Testcontainers/Azure environment and run outside `mvn test`.

JDBC error codes: `JDBC_001` connection failure, `JDBC_002` authentication /
secret resolution failure, `JDBC_003` invalid configuration, `JDBC_004`
invalid watermark value, `JDBC_005` watermark version conflict (concurrent
run — the losing run fails; re-run it to extract from the new committed
watermark).

### Validate-Only Mode

```bash
spark-submit ... --entity my_feed --mode FULL --validate-only --explain-mapping
```

Discovers files, validates physical headers and canonical mappings, and
prints a human-readable mapping explanation — with **no RAW write, no
CURATED write and no file moves**. Safe for production support.

### Troubleshooting (production support)

```
Error: HDR_001 — Required canonical header not found
Action:
  1. Run --validate-only --explain-mapping to see the proposed mappings
  2. Confirm the vendor header change with the source system
  3. Obtain approval for the new header name
  4. Add the approved alias to schema.columns[].aliases
  5. Re-run --validate-only to confirm the mapping resolves
  6. Move the quarantined file back to landing and re-run
```

For HDR_002/003 (duplicates) fix the vendor extract; for HDR_009 check the
configured delimiter; for HDR_015 inspect the failing column values against
the configured content rules (a swap usually shows 100% failure); HDR_017
is a configuration bug — the error message names the colliding columns.

```
Situation: BATCH_ATOMIC run failed on one bad file
Effect: the bad file is quarantined; files staged earlier in the same run
        remain in inprogress/ (restart-safe by design)
Action:
  1. Fix or remove the cause (usually add an approved alias, or leave the
     bad file in quarantine)
  2. Re-run the feed — inprogress leftovers are re-staged automatically
  3. If the batch must be abandoned, move inprogress/ files back to landing/
     manually; nothing was written to RAW or CURATED
```

### Migration Guide (legacy feeds)

Migration is **incremental, not mandatory** — legacy options keep working
with a deprecation warning. Old:

```hocon
source {
  header = true
  columns = ["subscriber_id", "hios_id"]
  header_aliases { "plan_hios_id" = "hios_id" }
}
```

New (recommended):

```hocon
schema {
  version = "2.0"
  columns = [
    { name = "subscriber_id", required = true, aliases = ["subscriber id", "subscriberid"] },
    { name = "hios_id", required = true, aliases = ["plan_hios_id", "plan hios id", "hios id"] }
  ]
  header_validation {
    strategy = "NAME_WITH_ALIASES"
    on_missing_required = "FAIL"
    on_extra_columns = "WARN"
    on_duplicate_columns = "FAIL"
    quarantine_on_failure = true
  }
}
```

Aliases may also be objects with governance metadata
(`{ value = "plan_hios_id", effective_from = "2026-07-01", valid_until = "2027-01-31", approval_reference = "CHG123456" }`);
expired aliases are excluded with a warning.

**Known limitations:** compressed archives (`.gz`/`.zip`) are not validated
inside the archive; there is no metrics backend (counts are logged);
`PARTITION_ATOMIC` batch policy and `CAPTURE` extra-column policy are not
implemented; multi-line quoted header fields are treated as malformed;
schema version is resolved from feed configuration only (not
filename/manifest).

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
staging tables from crashed runs are cleaned up on the next publish — but
only once they are older than 24 hours, so a concurrent run's live staging
table is never dropped.

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
