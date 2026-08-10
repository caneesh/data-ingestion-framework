# Deployment Guide

How to build, package, and deploy the Data Ingestion Framework. Companion
docs: [../architecture/ARCHITECTURE.md](../architecture/ARCHITECTURE.md), [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md).

---

## 1. Platform requirements

Pinned in the root `pom.xml`:

| Component | Version |
|-----------|---------|
| Scala | 2.12.18 (binary `2.12`) |
| Apache Spark | 3.5.0 (`spark-sql`, `spark-hive`) |
| Java | 11 (`maven.compiler.source`/`target = 11`) |
| Typesafe Config | 1.4.3 |

Deploy against a cluster whose Spark and Scala binary versions match
(Spark 3.5.x / Scala 2.12). Running on a Spark built for Scala 2.13 or a
different 3.x minor is unsupported.

---

## 2. Build and assembly jar

```bash
mvn clean package
```

The `ingestion-app` module produces a fat jar via `maven-assembly-plugin`
(`jar-with-dependencies` descriptor, `Main-Class` set to
`com.hcsc.generic.ingest.app.IngestMain`):

```
ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

This bundles all connectors (core/file/jdbc/kafka) and their dependencies.
Spark itself is provided by the cluster; do not add a second Spark to the jar.

---

## 3. Entry point and CLI

Main class: `com.hcsc.generic.ingest.app.IngestMain`.
Arguments are parsed by `ingestion-core/.../model/Cli.scala` (`CliParser`):

| Flag | Required | Meaning |
|------|----------|---------|
| `--entity <name>` | Yes | Feed name from config |
| `--mode <FULL\|INCR>` | Yes | Load mode (validated: must be FULL or INCR) |
| `--conf-path <path>` | No | Path to `application.conf` (else classpath) |
| `--stage <all\|raw\|curated\|curated-only\|c>` | No | Stage to run (default `all`) |
| `--raw-flag <value>` | No | Override the `file_type` partition value |
| `--resume-ingest-dt <dt>` | No | Resume curated from a specific RAW partition |
| `--run-id <id>` | No | Pin the run id (stamped on RAW rows as `run_id`) |
| `--resume` | No | Re-run a failed run; skips stages already SUCCESS (requires `--run-id`) |
| `--file-id <id>` | No | Restrict to one file (name or checksum prefix) |
| `--dry-run` | No | Validate and audit; write and move nothing |
| `--force-reprocess` | No | Override duplicate detection for approved reprocessing |
| `--validate-only` | No | Validate headers/mappings only; no writes, no moves, no audit |
| `--explain-mapping` | No | With `--validate-only`, print the resolved mapping |

Enforced at parse time: `--entity` required; `--mode` ∈ {FULL, INCR};
`--stage` in the allowed set; `--resume` requires `--run-id`.

---

## 4. Where the configuration lives

The `.conf` files are **operational configuration, not application code**.
They are not baked into the jar, so a feed can change without a rebuild.

| Location | What goes there | Who owns it |
|---|---|---|
| An edge/gateway node directory, e.g. `/etc/ingest/<feed>/` | the feed `.conf` and any `include`d schema `.conf` | platform/ops, under change control |
| Git (this repo, `docs/examples/<feed>/`) | the reviewed template the deployed copy is derived from | the feed's developer |
| The YARN container working directory | a per-run COPY, placed there by `--files` | nobody — it is recreated every run and discarded |
| Environment variables at submit time | host, database, table, and **all** credentials | the scheduler (Control-M) / secret store |

Rules that follow from this split:

- **Never put a credential in the `.conf`.** Passwords come from a secret
  provider (`env`, CyberArk, Key Vault, Conjur) resolved at run time.
- **In cluster mode, `export` does not reach the driver.** The driver is a
  YARN container with its own environment. Both the `env` secret provider
  and HOCON `${?VAR}` overrides read *that* environment, so every variable
  must be forwarded with
  `--conf spark.yarn.appMasterEnv.<NAME>=<value>` (the wrapper script does
  this from `INGEST_ENV_VARS`), or the job must run
  `--deploy-mode client`. The two halves fail **asymmetrically**: a missing
  secret raises `JDBC_002`, but a missing `${?VAR}` override silently falls
  back to the config's default — no error, wrong target. Verify the
  driver's `[Jdbc] url=... table=...` line on any run whose connection
  details come from the environment.
- A password forwarded via `--conf` is visible in `ps` on the submitting
  host and in the YARN launch context. Acceptable for a lower-environment
  service account; for production use CyberArk / Key Vault / Conjur, which
  the driver fetches at run time under its own identity.
- **Never point `--conf-path` at an HDFS path or a submit-side absolute
  path in cluster mode.** The driver runs in a container that has neither.
  Ship with `--files` and reference the basename (`./feed.conf`).
- **Ship every included file.** `--files feed.conf,feed-schema.conf` — the
  include resolves against the directory holding the feed config, which in
  a container is where `--files` landed both.
- Keep the deployed copy and the repo template in sync deliberately; the
  repo copy is the reviewable artifact, the deployed copy is what runs.

---

## 4b. Control tables in a shared database

The framework keeps eight control tables — run ledger, file audit,
reconciliation, header audit, rejects, file registry, watermarks and run
locks. It creates each one on first write with `CREATE TABLE IF NOT
EXISTS`; nothing needs pre-creating.

**Where they go is entirely config.** There is no hard-coded `ingest_audit`:

| Table | Key | Default name |
|---|---|---|
| run ledger | `audit.database` + `run_table` | `ingest_run_audit` |
| file audit | `audit.database` + `file_table` | `ingest_file_audit` |
| reconciliation | `audit.database` + `reconciliation_table` | `ingest_reconciliation` |
| header audit | `audit.database` + `header_table` | `ingest_header_audit` |
| rejects | `rejects.database` + `table` | `ingest_rejects` |
| file registry | `rejects.database` + `registry_table` | `ingest_file_registry` |
| watermarks | `source.incremental.watermark_store.database` + `table` | `ingest_watermarks` |
| run locks | `concurrency.database` (else `audit.database`) + `table` | `ingest_run_locks` |

`watermark_store.database` is **required** for the hive store — it does not
inherit `audit.database`, and its absence raises `JDBC_003`.

### Sites that cannot create databases

Point every key above at a database the job already has write access to —
commonly the raw database. The framework issues `CREATE DATABASE` **only
when the database is genuinely absent**: `IF NOT EXISTS` is not enough on
its own, because an authorizer (Ranger, SQL-standard auth) evaluates the
CREATE privilege before reaching it, and would reject a statement that had
nothing to do. So no database-creation privilege is needed for an existing
target.

### Sharing a database with other pipelines

The default names are `ingest_`-prefixed but not otherwise namespaced.
When the database also holds other teams' tables, **state every table name
explicitly in the feed config** rather than relying on defaults — what the
framework will create should be visible, not implied — and rename any that
would collide. `docs/examples/smartiq_pdp/` shows the full shape.

---

## 5. spark-submit examples

### Ready-made launchers

`scripts/` holds the submit wrappers. Each takes the feed config, entity and
mode, and forwards any further flags to the application:

| Script | Runs |
|---|---|
| `run_ingest.sh` | COUPLED — raw then curated in one job (the usual case) |
| `run_raw.sh` | DECOUPLED job 1 of 2 — raw only |
| `run_curated_pending.sh` | DECOUPLED job 2 of 2 — drain pending curated batches |
| `run_smartiq.sh` | the SmartIQ feeds, with site settings in one file and preflight checks |

They share `ingest_submit_common.sh`, configured by environment:

| Variable | Meaning |
|---|---|
| `INGEST_DEPLOY_MODE` | `cluster` (default) or `client` |
| `INGEST_JAR` | **the application** assembly jar |
| `INGEST_JARS` | **the vendor JDBC driver(s)**, comma-separated |
| `INGEST_EXTRA_FILES` | files the feed config `include`s, comma-separated |
| `INGEST_ENV_VARS` | variable NAMES forwarded to a cluster-mode driver |

`INGEST_JAR` and `INGEST_JARS` differ by one letter and mean different
things; the wrapper rejects an `INGEST_JAR` that looks like a JDBC driver
rather than letting the run fail later with no main class.

The wrapper validates before submitting: every path in `--files` and
`--jars` must exist, and empty elements from a stray comma
(`"a.conf,"`, `"a.conf,,b.conf"`) are dropped. Left in, they reach YARN as
an empty path and fail inside `prepareLocalResources` with *"Can not create
a Path from an empty string"* — an error naming neither the option nor the
file.

A feed-specific launcher like `run_smartiq.sh` is worth writing per pipeline:
it keeps site values in one gitignored settings file, prompts for the
credential rather than storing it, and refuses to submit when a prerequisite
is missing — naming the cause instead of letting it surface minutes later as
a symptom.

The reference invocation is `scripts/run_health_sherpa.sh`:

```bash
spark-submit \
  --class com.hcsc.generic.ingest.app.IngestMain \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.caseSensitive=false \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.sql.shuffle.partitions=200 \
  --files "$CONF_FILE" \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity health_sherpa_member \
  --mode FULL \
  --conf-path "./$(basename "$CONF_FILE")"
```

Notes on the pattern:

- The HOCON config is shipped with `--files "$CONF_FILE"` and located with
  **`--conf-path`**. Only the DRIVER reads the feed config, so no executor
  option is needed.
- **Split configs** (a feed using `include required("<name>-schema.conf")`)
  must ship *every* file: `--files "$CONF_FILE,$SCHEMA_FILE"`. HOCON
  resolves an include relative to the including file's directory, so the
  included file has to sit beside the feed config in the container working
  directory — which is exactly where `--files` puts it.
- `-Dconfig.file=<basename>` is also supported, including for split
  configs — the framework absolutises it before Typesafe Config reads it.
  Prefer `--conf-path` anyway: it is explicit, it fails with a named error
  (CFG_018) when the file was not shipped, and it does not depend on
  driver JVM options reaching the container. **A jar built before
  2026-08-06 cannot resolve includes from a bare `-Dconfig.file` name.**
- A **missing config file** is a hard error (CFG_018), not a silent
  empty config; a **missing include** names the directory searched and
  lists what is in it. Both are driver-side and happen before Spark work
  begins, so they cost seconds, not a full run.
- `spark.yarn.maxAppAttempts=1` is deliberate: the pipeline is restart-safe by
  design (see the runbook), but a blind YARN re-attempt can race a
  half-finished run. Prefer an explicit `--resume --run-id` re-run.
- `spark.sql.caseSensitive=false` matches the framework's case-insensitive
  header handling.

A JDBC feed uses the same invocation with `--mode INCR` for incremental loads:

```bash
spark-submit --class com.hcsc.generic.ingest.app.IngestMain --master yarn \
  --deploy-mode cluster \
  --jars /opt/jdbc/mssql-jdbc-<ver>.jar \
  --files /etc/ingest/application.conf \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity my_jdbc_feed --mode INCR --run-id 2026-07-26T00 \
  --conf-path ./application.conf
```

---

## 6. JDBC driver deployment (driver AND executors)

The JDBC driver class is loaded reflectively on the **driver** (health check /
metadata / watermark capture, `health/JdbcHealthCheck.scala` →
`Class.forName(cfg.driver)`) **and independently on each executor** (Spark's
partition reads open their own JDBC connections). Therefore:

> The JDBC driver must be present on the driver **and** every executor.

Deployment options (any one, per cluster policy):

- `spark-submit --jars /path/to/driver.jar` (distributes to driver + executors).
- A cluster-wide JDBC library / classpath entry.
- Bundling the driver into the app jar (only if licensing/policy allows —
  the Microsoft JDBC driver is not bundled by default).

A missing driver **fails fast with `JDBC_001`** (`ClassNotFoundException`
caught in the health check) before any Spark read starts — so a missing
executor-side driver surfaces as a partition-read failure, not a driver-side
one. Deploy to both.

Default driver class per dialect (`dialect/JdbcDialect.scala`):

| Dialect | Default driver |
|---------|----------------|
| `sqlserver` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| `postgresql` | `org.postgresql.Driver` |
| `oracle` | `oracle.jdbc.OracleDriver` |
| `db2` | `com.ibm.db2.jcc.DB2Driver` |
| `mysql` | `com.mysql.cj.jdbc.Driver` |
| `generic` | none — set `source.driver` explicitly |

---

## 7. Azure SQL: firewall (driver vs. executor egress) and encryption

- **Firewall / egress:** the driver health check proves only the **driver's**
  network path to the database. Executors open their own connections during
  extraction, so the Azure SQL firewall (and any NSG / egress rule) must allow
  **both the driver host and every executor host**. A driver-only firewall
  rule passes the health check and then fails at partition-read time.
- **Encryption by default:** the `sqlserver` (Azure SQL) dialect enforces
  encrypted connections by default (`encrypt=true`,
  `trustServerCertificate=false`). Override only via
  `source.connection_properties` if you have a specific, reviewed reason.
- **Auth types** (`JdbcSourceConfig.scala`; Azure modes are sqlserver-only and
  map onto Microsoft JDBC driver properties — no Azure SDK required):

| `auth.type` | Requires | Driver property |
|-------------|----------|-----------------|
| `SQL_PASSWORD` (default) | `user`, `password` | user/password |
| `AZURE_MANAGED_IDENTITY` | optional `client_id` | `authentication=ActiveDirectoryMSI` (+ `msiClientId`) |
| `AZURE_SERVICE_PRINCIPAL` | `client_id`, secret-backed `client_secret` | `authentication=ActiveDirectoryServicePrincipal` |
| `ENTRA_ID_PASSWORD` | `user`, `password` | `authentication=ActiveDirectoryPassword` |
| `ACCESS_TOKEN` | secret-backed token | `accessToken` |

  Prefer MSI or service principal for long extractions — a pre-fetched
  `ACCESS_TOKEN` can expire mid-job.

---

## 8. Secret provider deployment

Credentials resolve through the `SecretProvider` abstraction
(`auth/SecretProvider.scala`, `auth/CyberArkSecretProvider.scala`). Referenced
as `{ provider = "<type>", ... }` for `auth.user` and `auth.password`.

| Provider | Config | Deployment notes |
|----------|--------|------------------|
| `inline` | `{ provider = "inline", value = "..." }` | Dev/test only; logs a warning for passwords. Do not use in production. |
| `env` | `{ provider = "env", key = "DB_PASSWORD" }` | Set the env var on driver and executors (executors need it too if used for the connection). Missing → JDBC_002. |
| `sysprop` | `{ provider = "sysprop", key = "db.password" }` | Pass via `-D` in driver/executor java options. Missing → JDBC_002. |
| `file` | `{ provider = "file", path = "/etc/secrets/db" }` | File must exist and be readable on the resolving host (UTF-8, trimmed). Missing/unreadable → JDBC_002. |
| `cyberark` | see below | CyberArk Central Credential Provider (CCP). |

**CyberArk CCP** retrieves the account from a CCP endpoint; one vault object
serves both user id (`attribute = "UserName"`) and password (`Content`,
default) from a single cached call:

```hocon
auth {
  user     = { provider = "cyberark", url = "https://ccp.example.com",
               app_id = "APP_Ingest", safe = "DB_Safe", object = "svc-acct",
               attribute = "UserName" }
  password = { provider = "cyberark", url = "https://ccp.example.com",
               app_id = "APP_Ingest", safe = "DB_Safe", object = "svc-acct" }
}
```

Required: `url`, `app_id`, `safe`, `object`. Optional: `folder`, `params`,
`connect_timeout_ms` (5000), `read_timeout_ms` (10000), `cache_ttl_ms`
(300000; 0 disables). Client-certificate auth to CCP uses standard JVM TLS
keystore settings passed in driver java options, e.g.:

```
--conf "spark.driver.extraJavaOptions=-Djavax.net.ssl.keyStore=/etc/pki/ccp.jks -Djavax.net.ssl.keyStorePassword=..."
```

TLS verification is never disabled. CCP HTTP errors (401/403, missing
attribute) map to JDBC_002.

**Azure Key Vault / Databricks scopes** are not built in but plug in by
registering a custom `SecretProvider` (no extra SDK unless your custom
provider needs one). See [../architecture/ARCHITECTURE.md](../architecture/ARCHITECTURE.md#extension-points)
for the registration contract.

---

## 9. Pre-production validation checklist

Before the first production run in a new environment, verify:

- Hive metastore connectivity (audit, watermark, registry, staging tables).
- HDFS paths and permissions for the feed's `source.folders`
  (landing/inprogress/processed/quarantine/archive) and RAW/curated warehouse.
- JDBC driver present on **driver and executors** (§6); run a
  `--validate-only` or a small `--dry-run` first.
- Executor egress to the source database / Azure SQL firewall (§7).
- Secret provider resolves on the hosts that need it (§8).
- Kafka client compatibility for Kafka feeds.

Metrics counters accumulate in-process via `JdbcMetrics`
(`jdbc_connection_attempt_total`, `jdbc_retry_total`,
`jdbc_watermark_commit_total`, `jdbc_watermark_conflict_total`,
`jdbc_schema_drift_total`, …). There is no built-in metrics backend — wire
`JdbcMetrics.snapshot` into your metrics agent at the deployment boundary.

## Production Acceptance Checklist

- [ ] All Spark and Scala artifacts use the `_2.12` binary suffix; Java 11 build and runtime verified
- [ ] Microsoft JDBC driver present on driver AND executors; missing driver fails fast (JDBC_001)
- [ ] Partial partition configuration fails during startup (all-four-or-none, bounds, `max_partitions`)
- [ ] Permanent SQL errors (auth, syntax, missing objects, unknown) are never retried
- [ ] Transient Azure SQL errors use bounded exponential backoff with jitter
- [ ] Documentation does not claim `DataFrameReader.load()` retries executor reads (three-layer model)
- [ ] Incremental queries use a captured, stable upper watermark
- [ ] Timestamp ties resolved deterministically (composite tie-breaker or overlap + curated dedup)
- [ ] Concurrent runs cannot independently advance the same watermark (JDBC_005 optimistic versioning)
- [ ] Watermark commit occurs only after complete successful publication and reconciliation
- [ ] Failed runs are restartable and idempotent (RAW run_id guard, unchanged watermark)
- [ ] Full SQL and credential-bearing URLs are not logged (query hash by default; `diagnostics.log_sql` audited)
- [ ] Azure SQL TLS certificate verification enabled (`encrypt=true`, `trustServerCertificate=false`)
- [ ] Managed Identity / approved secret storage in use; no plaintext credentials in HOCON or logs
- [ ] Executor-to-Azure-SQL firewall requirements documented and validated (executor probe if needed)
- [ ] Custom SQL restricted (single SELECT, no semicolons/DDL/DML) and audited via query hash
- [ ] Source and target counts reconcile (`ingest_reconciliation`)
- [ ] SQL Server Testcontainers integration tests pass where Docker is available
- [ ] Operations runbook reviewed by the production support team

## Source Consistency Assumptions

The framework provides a *bounded, reproducible* extraction window — not a
transactionally consistent snapshot. During a long parallel read, different
partitions may observe the source at slightly different instants within the
window. If stronger guarantees are required, provide them at the database:

| Mechanism | What it gives you |
|-----------|-------------------|
| Bounded watermark window (built in) | No rows silently enter/leave the window mid-extraction |
| `READ_COMMITTED_SNAPSHOT` on the database | Readers never block/dirty-read writers |
| `SNAPSHOT` isolation + snapshot view | Point-in-time consistency across partitions |
| Source-generated batch id column | Extraction keyed to producer-defined batches |
| Source snapshot table | Full control of what a "batch" contains |

Do not claim transactional consistency for a feed unless one of the
database-side mechanisms is actually in place.
