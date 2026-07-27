# Deployment Guide

How to build, package, and deploy the Data Ingestion Framework. Companion
docs: [ARCHITECTURE.md](ARCHITECTURE.md), [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md).

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

## 4. spark-submit examples

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
  --driver-java-options "-Dconfig.file=$(basename "$CONF_FILE")" \
  --conf "spark.executor.extraJavaOptions=-Dconfig.file=$(basename "$CONF_FILE")" \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity health_sherpa_member \
  --mode FULL
```

Notes on the pattern:

- The HOCON config is shipped with `--files "$CONF_FILE"` and located on both
  driver and executors via `-Dconfig.file=<basename>` in both
  `--driver-java-options` and `spark.executor.extraJavaOptions`. Keep both in
  sync, or pass `--conf-path` explicitly instead.
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
  --driver-java-options "-Dconfig.file=application.conf" \
  --conf "spark.executor.extraJavaOptions=-Dconfig.file=application.conf" \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity my_jdbc_feed --mode INCR --run-id 2026-07-26T00
```

---

## 5. JDBC driver deployment (driver AND executors)

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

## 6. Azure SQL: firewall (driver vs. executor egress) and encryption

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

## 7. Secret provider deployment

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
provider needs one). See [ARCHITECTURE.md](ARCHITECTURE.md#extension-points)
for the registration contract.

---

## 8. Pre-production validation checklist

Before the first production run in a new environment, verify:

- Hive metastore connectivity (audit, watermark, registry, staging tables).
- HDFS paths and permissions for the feed's `source.folders`
  (landing/inprogress/processed/quarantine/archive) and RAW/curated warehouse.
- JDBC driver present on **driver and executors** (§5); run a
  `--validate-only` or a small `--dry-run` first.
- Executor egress to the source database / Azure SQL firewall (§6).
- Secret provider resolves on the hosts that need it (§7).
- Kafka client compatibility for Kafka feeds.

Metrics counters accumulate in-process via `JdbcMetrics`
(`jdbc_connection_attempt_total`, `jdbc_retry_total`,
`jdbc_watermark_commit_total`, `jdbc_watermark_conflict_total`,
`jdbc_schema_drift_total`, …). There is no built-in metrics backend — wire
`JdbcMetrics.snapshot` into your metrics agent at the deployment boundary.
