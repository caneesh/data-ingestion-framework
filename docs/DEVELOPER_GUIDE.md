# Developer Guide

> A styled standalone rendering of this guide is available at
> [DEVELOPER_GUIDE.html](DEVELOPER_GUIDE.html) (open locally in a browser —
> GitHub shows it as source). This Markdown file is the source of truth.

A step-by-step guide to using the framework: from source system to curated
Hive table. You describe a feed in HOCON — where the data lives, what its
schema contract is, how it lands in RAW and merges into CURATED — and the
pipeline handles validation, idempotency, watermarks, rejects, audit, and
recovery.

**Stack:** Scala 2.12 · Spark 3.5 · Java 11 · Hive/ORC · Typesafe Config (HOCON) · Maven

**Pipeline at a glance:**

```
Source            Validate                RAW                 CURATED              Commit
file|JDBC|Kafka → contract, rejects,   →  append-only ORC, →  merge / dedup,    →  audit → reconcile
                  quarantine              stamped              atomic publish       → watermark
```

## Contents

1. [How the framework works](#how-the-framework-works)
2. [Step 1 — Build the fat jar](#step-1--build-the-fat-jar)
3. [Step 2 — Define a feed](#step-2--define-a-feed)
4. [Step 3 — Add a schema contract](#step-3--add-a-schema-contract)
5. [Step 4 — Configure the source](#step-4--configure-the-source)
6. [Step 5 — RAW and CURATED layers](#step-5--raw-and-curated-layers)
7. [Step 6 — Validate before running](#step-6--validate-before-running)
8. [Step 7 — Run locally](#step-7--run-locally)
9. [Step 8 — Submit to the cluster](#step-8--submit-to-the-cluster)
10. [Step 9 — Operate and recover](#step-9--operate-and-recover)
11. [Step 10 — Generate configs with the wizard](#step-10--generate-configs-with-the-wizard)

---

## How the framework works

Six Maven modules make up the build. `ingestion-core` holds the pipeline
engine, schema contracts, transforms, publish/audit/reject services, and
locking. Three connector modules — `ingestion-file`, `ingestion-jdbc`,
`ingestion-kafka` — register sources. `ingestion-config-gen` is an
interactive wizard that writes feed configs for you, and `ingestion-app`
bundles everything into the runnable fat jar with `IngestMain` as the entry
point.

One invariant drives the whole design: **nothing advances until everything
downstream of it succeeded**. The incremental watermark is captured *before*
the read (so the window is bounded), but committed only after RAW write,
CURATED publish, audit, and reconciliation have all completed. A failed run
leaves the watermark untouched — re-running re-reads the same window, and
idempotency guards make the replay safe.

---

## Step 1 — Build the fat jar

You need JDK 11+ and Maven. Build the whole reactor from the repository
root — individual modules do not build standalone because the parent POM is
not installed:

```bash
mvn clean package
```

The deployable artifact is produced by `ingestion-app`:

```
ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

It bundles all connectors and dependencies. Spark itself is *provided* by
the cluster — never add a second Spark to the jar.

> **JDK 17+ note:** the test POMs already carry the `--add-opens` flags
> Spark needs on modern JDKs. If you run Spark outside Maven on JDK 17+,
> pass those flags yourself.

---

## Step 2 — Define a feed

Every pipeline is a named entry under `feeds { }` in a HOCON file — either
the bundled `application.conf` or a standalone file you pass with
`--conf-path`. A feed has four parts: identity, `schema` (the contract),
`source`, and the `raw` / `curated` targets.

```hocon
feeds {
  health_sherpa_member {
    entity = "health_sherpa_member"
    mode   = "FULL"            # FULL | INCR

    schema  { ... }            # step 3 — contract & drift policies
    source  { ... }            # step 4 — file | jdbc | kafka
    raw     { ... }            # step 5 — append-only landing table
    curated { ... }            # step 5 — merged, consumer-facing table
  }
}
```

Two fully-commented reference configurations catalog every supported
option — copy from them rather than memorizing keys:

- [examples/feed-file-reference.conf](examples/feed-file-reference.conf) — file ingestion (managed folders, pre-read validation, contract, rejects, idempotency)
- [examples/feed-jdbc-reference.conf](examples/feed-jdbc-reference.conf) — SQL ingestion (all four read modes, every secret provider, watermarks, partitioning)

Optional blocks add production hardening as you need it:

| Block | Purpose |
|-------|---------|
| `idempotency` | SHA-256 file registry with a `SKIP \| REJECT \| REPROCESS_WITH_APPROVAL` duplicate policy |
| `rejects` | Record-level rules routed to a reject table with count/percent thresholds |
| `audit` | Control totals and reconciliation |
| `lock` | Run-level entity locking (`concurrency` block). Two providers: `HIVE` (default) is **best-effort** — claim/settle/re-read shrinks but does not close the race window; fine for dev/test or a single scheduler. `JDBC` (`concurrency { provider = JDBC, jdbc { url = ... } }`) is an **atomic** compare-and-set on a relational control table — use it in production. A daemon heartbeat renews the lease every `lease/3`; a run that loses ownership aborts with PIPE_001 before the curated publish and the watermark advance. |

---

## Step 3 — Add a schema contract

The contract is the safety core of a feed: it names every column
canonically, maps vendor aliases, pins positions and types, and declares —
per drift type — whether the run fails, warns, or ignores. The framework's
rule: **alias known header changes, fail unknown required-header changes,
never silently substitute null for a missing required column.**

```hocon
schema {
  version       = "1.0"
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
    { name = "subscriber_id", type = "string", nullable = false,
      required = true, position = 0,
      aliases = ["subscriber id", "subscriberid"] },
    { name = "hios_id", type = "string", nullable = false,
      required = true, position = 1,
      aliases = ["plan_hios_id", "hios id"],
      regex = "^[0-9]{5}[A-Z][0-9]{4}$" }   # catches swapped columns
  ]

  header_validation {
    strategy = "NAME_WITH_ALIASES"  # or STRICT_NAME / NAME_ALIAS_POSITION_FALLBACK
    on_missing_required   = "FAIL"
    quarantine_on_failure = true
  }

  content_validation { enabled = true, mode = "SAMPLE", sample_rows = 1000 }
}
```

Feeds without a `schema` block fall back to legacy `source.columns` /
`source.header_aliases` handling (still supported, logs a deprecation
warning). Positional mapping is never applied silently — it requires
`header = false` or an explicit `force_columns_by_position = true`, and
headers are still validated first so drift surfaces before positional
recovery.

---

## Step 4 — Configure the source

### File (CSV, JSON, Parquet, ORC)

```hocon
source {
  type      = "file"
  file_type = "csv"
  header    = true
  delimiter = ","

  # Managed lifecycle: landing -> inprogress -> processed (+ archive);
  # invalid files -> quarantine. Replaces a static `path` glob.
  folders {
    landing    = "hdfs:///data/membership/health_sherpa/landing"
    inprogress = "hdfs:///data/membership/health_sherpa/inprogress"
    processed  = "hdfs:///data/membership/health_sherpa/processed"
    quarantine = "hdfs:///data/membership/health_sherpa/quarantine"
  }

  # Pre-read validation: name pattern, size, encoding, checksum sidecar,
  # expected header, trailer marker — failures quarantine with a reason.
  validation { filename_pattern = "member_.*[.]csv", allowed_extensions = ["csv"] }
}
```

### JDBC (SQL Server, PostgreSQL, Oracle, DB2, MySQL)

The dialect is inferred from the URL. Four read modes: `FULL_TABLE`,
`SELECT_QUERY`, `CUSTOM_SQL`, and `INCREMENTAL` with a watermark.
Credentials always come from a secret provider — `env`, `file`, `sysprop`,
`inline`, `cyberark` (CCP, can serve both user and password), `conjur`, or
`azure_keyvault`. Secrets never appear in logs or error messages.

```hocon
source {
  type = "jdbc"
  url  = "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=ClaimsDB"
  # sqlserver dialect: encrypt=true, trustServerCertificate=false by default

  auth {
    user     = "svc_ingest"
    password = { provider = "env", key = "SQLSERVER_PWD" }
  }

  mode  = "INCREMENTAL"
  table = "dbo.claims"
  where = "state IN ('IL','TX')"        # pushed down into the database

  incremental {
    watermark_type    = "TIMESTAMP"     # TIMESTAMP | NUMERIC | COMPOSITE
    watermark_columns = ["modified_ts"]
    initial_value     = "1900-01-01 00:00:00"
    overlap           = "300"           # seconds re-read behind the watermark
    watermark_store   = { type = "hive", database = "ingest_audit" }
  }

  fetchsize = 5000
  numPartitions = 10, partitionColumn = "claim_id"
  retry { max_attempts = 3, backoff_ms = 2000 }
  health_check { enabled = true }
}
```

> **Bounded windows:** the upper watermark is captured on the driver
> *before* the read, so every run extracts a half-open window
> `(previous, captured]`. Rows arriving mid-read never smear the boundary;
> a `TIMESTAMP` watermark without `overlap` or a composite tie-breaker logs
> a warning because same-timestamp late inserts can be missed.

### Kafka (batch)

```hocon
source {
  type = "kafka"
  "bootstrap.servers" = "kafka1:9092,kafka2:9092"
  topic = "user-events"
  startingOffsets = "earliest", endingOffsets = "latest"
  value.format = "json"          # plus value.schema for typed parsing
}
```

---

## Step 5 — RAW and CURATED layers

**RAW** is the append-only landing zone: ORC, partitioned (typically by a
derived `ingest_dt`), business data untouched, stamped with lineage
metadata — `source_file`, `row_idx`, `load_timestamp`, `file_type`,
`file_id`, run and batch identifiers.

```hocon
raw {
  database = "membership_raw"
  table    = "health_sherpa_member"
  path     = "hdfs:///data/warehouse/membership/raw/health_sherpa_member"
  format   = "orc"
  partitioning {
    keys = ["ingest_dt"]
    derive { ingest_dt { kind = "expr", expr = "date_format(current_timestamp(), 'yyyy-MM-dd')" } }
  }
}
```

**CURATED** is the consumer-facing current state: typed columns, derived
fields, audit columns (`create_timestamp`, `last_modified_ts`,
`last_modified_op`), and a merge strategy. `FULL` mode overwrites; `INCR`
mode does an upsert-union-dedup keyed merge. Publishing is transactional —
data is staged, validated, then swapped in with `INSERT OVERWRITE`, so
consumers never see a half-written table.

```hocon
curated {
  enabled  = true
  database = "claims_curated"
  table    = "claims_data"

  column_types { claim_id = "string", amount = "decimal(18,2)" }
  transform { derive = [ { name = "create_timestamp", expr = "current_timestamp()" } ] }

  merge {
    keys = ["claim_id"]                # business keys for INCR upsert
    freshness {                        # REQUIRED for keyed merges (CUR_008
      column = "modified_date"         # fails closed without it): the
                                       # source version column — highest wins
      tie_breakers = ["seq desc as bigint"]  # optional; direction + LOGICAL type
      # compare_as = "timestamp"       # LOGICAL comparison type: storage keeps
      # compare_format = "M/d/yyyy"    # its physical type (e.g. all-string);
                                       # every comparison uses the declared type.
                                       # Unparseable values FAIL, never lose as NULL.
    }
    null_handling { drop_null_keys = true, treat_blank_as_null = true }
  }
  dedup { order_by = ["ingest_seq"] }  # EXTRA ordering after freshness
}
```

Legacy last-write-wins (run order decides, unsafe for current-snapshot
tables) requires an explicit double opt-in and is never generated by the
wizard: `merge { strategy = "LAST_WRITE_WINS", allow_unsafe_legacy_merge = true }`.

---

## Step 6 — Validate before running

Never debug a feed by running it. Two flags catch config and mapping
problems without touching any table:

```bash
# Static validation of the feed config + contract
... IngestMain --entity claims_feed --mode INCR --validate-only

# Also print exactly how source headers map to canonical columns
... IngestMain --entity claims_feed --mode INCR --validate-only --explain-mapping

# Full read + validation with NO writes at all
... IngestMain --entity claims_feed --mode INCR --dry-run
```

Recommended order for a new feed: `--validate-only --explain-mapping`
first, then `--dry-run`, then a real run.

---

## Step 7 — Run locally

The test suite is the fastest local loop — the full suite includes
end-to-end pipeline runs against a real embedded Hive metastore (Derby)
and an in-memory JDBC source:

```bash
mvn test        # always from the repo root — the full reactor
```

With a Docker daemon present, the SQL Server integration suite additionally
runs against a real `mssql/server:2022` Testcontainer (bracket quoting,
datetime2 precision, DATETIMEOFFSET, ROWVERSION, failure classification).
Without Docker those tests cancel cleanly — the suite stays green either
way.

To run the actual app locally against a disposable SQL Server:

```bash
docker run -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='YourStrong!Pass1' \
  -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

spark-submit \
  --class com.hcsc.generic.ingest.app.IngestMain \
  --master 'local[*]' \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.warehouse.dir=/tmp/local-warehouse \
  --driver-java-options "-Dconfig.file=local.conf" \
  ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity my_feed --mode FULL
```

> **Local container TLS:** the throwaway container presents a self-signed
> certificate, so set `connection_properties { encrypt = "false" }` and
> `allow_insecure_tls = true` in the local config only. Production feeds
> keep the strict dialect defaults (`encrypt=true`, certificate verified).

---

## Step 8 — Submit to the cluster

The reference invocation is `scripts/run_health_sherpa.sh`. The config file
ships alongside the job via `--files` and is selected with `-Dconfig.file`;
JDBC drivers ride in on `--jars`:

```bash
spark-submit \
  --class com.hcsc.generic.ingest.app.IngestMain \
  --master yarn --deploy-mode cluster \
  --conf spark.sql.caseSensitive=false \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.yarn.maxAppAttempts=1 \
  --jars /opt/jdbc/mssql-jdbc-<ver>.jar \
  --files /etc/ingest/application.conf \
  --driver-java-options "-Dconfig.file=application.conf" \
  --conf "spark.executor.extraJavaOptions=-Dconfig.file=application.conf" \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity claims_feed --mode INCR --run-id 2026-07-26T00
```

### CLI reference

| Flag | Meaning |
|------|---------|
| `--entity` | Feed name under `feeds { }`. Required. |
| `--mode` | `FULL` (overwrite) or `INCR` (watermark + keyed merge). Default `FULL`. |
| `--conf-path` | Standalone HOCON file instead of the bundled `application.conf`. |
| `--stage` | `all` (default), `raw`, `retention`, or `curated` / `curated-only` / `c` to rebuild CURATED from existing RAW. |
| `--run-id` | Stable run identifier (letters, digits, `_`, `-`); reaches staging tables and audit records. |
| `--resume` | Resume a failed run; requires `--run-id` of the run to resume. |
| `--resume-ingest-dt` | Resume scoped to a specific RAW ingest date. |
| `--file-id` | Restrict processing to one registered file. |
| `--raw-flag` | Override the RAW file-type flag (defaults: `F` for FULL, `I` for INCR). |
| `--dry-run` | Read and validate everything, write nothing. Not combinable with batch selectors (a dry-run replay would checkpoint unpublished batches). |
| `--force-reprocess` | Override the duplicate-file registry decision. |
| `--validate-only` | Static config/contract validation, no Spark reads. |
| `--explain-mapping` | With `--validate-only`: print the header-to-column mapping. |

Batch selectors (`--stage curated` only; mutually exclusive with each
other and with `--run-id`/`--resume-ingest-dt`) drive the decoupled
curated batch driver — each selected RAW batch replays under the governed
per-run path and checkpoints via its own curated SUCCESS ledger row:

| Flag | Selects |
|------|---------|
| `--pending` | RAW-successful batches not yet curated (the normal decoupled curated job). |
| `--replay-failed` | Pending batches with at least one failed curated attempt. |
| `--replay-last N` | Last N RAW-successful batches regardless of curated state (forced rebuild; content-idempotent under the freshness merge). |
| `--replay-from D` / `--replay-to D` | RAW-success date range (`yyyy-MM-dd`, on ledger event time). |
| `--replay-source-system X` | Filter by RAW `source_system` lineage; composable with `--pending` or a date range. |

Set the session timezone explicitly with `app.spark.session_time_zone` if
you cannot use the default — all framework-stamped timestamps are UTC
unless overridden.

---

## Step 9 — Operate and recover

Failures are designed to be re-runnable. Because the watermark commits
last, a failed incremental run simply re-extracts the same bounded window
on retry; file-level (SHA-256 registry), run-level (`run_id`), and
batch-level guards prevent double loads. Files that fail validation land in
quarantine with a recorded reason, not in RAW.

- **Retry a failed run:** re-run with the same `--run-id` and `--resume`.
- **Rebuild CURATED only:** `--stage curated --run-id <id>` replays one
  batch from existing RAW under the same entity lock; `--stage curated
  --pending` drains every un-curated batch; `--replay-failed` retries the
  failed ones.
- **Reprocess a known duplicate:** `--force-reprocess` (or the
  `REPROCESS_WITH_APPROVAL` policy).
- **One process or two (Control-M):** declare `ingestion.execution =
  COUPLED` (default, one job: `scripts/run_ingest.sh`) or `DECOUPLED`
  (two independent jobs: `scripts/run_raw.sh` with
  `watermark { advance_after = RAW }`, plus
  `scripts/run_curated_pending.sh`). CFG_016 validates the combination
  and a DECOUPLED feed refuses `--stage all` so a scheduler cannot
  double-process batches. See docs/DECOUPLING_DESIGN.md and the
  runbook's Control-M section.
- **Batch status:** query the `ingest_batch_control` view
  (`ddl/ingest_batch_control_view.sql`) — one row per batch;
  `curated_done` is the checkpoint truth, `retry_count` counts failures.

Every failure carries a coded, greppable prefix:

| Prefix | Domain |
|--------|--------|
| `HDR_*` | Schema contract and header validation |
| `JDBC_*` | JDBC config, dialects, SQL safety, watermark conflicts |
| `EXT_*` | Extraction planning and checkpoints |
| `RAW_*` / `CUR_*` | RAW write and CURATED strategy rules |
| `REC_*` / `CFG_*` | Reconciliation and feed-config compatibility |

The full catalog, plus watermark and quarantine recovery procedures, lives
in [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md).

---

## Step 10 — Generate configs with the wizard

Instead of hand-writing HOCON, `ingestion-config-gen` interviews you and
emits a validated feed config (HOCON, JSON, or YAML) — for JDBC sources it
can introspect the database schema to propose the contract:

```bash
alias confgen='java -cp ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.hcsc.generic.ingest.confgen.ConfigGeneratorMain'

confgen --source-type jdbc --output-dir conf/feeds          # interactive
confgen --source-type jdbc --draft ~/drafts/claims.json     # resumable draft
```

Draft mode saves every answer as you go — re-running the same command
resumes where you stopped. Generated configs are dry-run validated before
they're written, and the output plugs straight into `--conf-path`. Keep
draft files out of git if you chose the `inline` secret provider.

---

## Companion documentation

| Document | Contents |
|----------|----------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Module map, data flow, extension points, enforced invariants |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Assembly, spark-submit, JDBC drivers, secret providers |
| [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md) | Error catalog, watermark/quarantine recovery, restart/resume |
| [CONFIG_GENERATOR.md](CONFIG_GENERATOR.md) / [RUNNING_CONFIG_GENERATOR.md](RUNNING_CONFIG_GENERATOR.md) | Feed-config wizard |
| [SQL_SERVER_AUTH_AUDIT.md](SQL_SERVER_AUTH_AUDIT.md) | Credential resolution chain, security posture |
