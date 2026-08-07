# SmartIQ_PDP — lower-environment end-to-end test plan

A slim stand-in for the 364-column production feed: **11 columns, its own
entity and tables, the same machinery**. A green run here means the
mechanisms the full feed depends on are working against the real SQL
Server, the real Hive metastore and the real cluster.

## Why these 11 columns

Not the first eleven — each one is carrying a specific mechanism:

| Column | Exercises |
|---|---|
| `file_name` | the business key of the merge |
| `last_modified_datetime` | freshness comparison **and** the extraction watermark (`datetime` in source) |
| `form_guid` | a column restored in mapping v2 (was dropped in v1) |
| `user_email_id` | PII tagging → masking in reject payloads |
| `effective_date` | the `date`-typed source column (typed, not string) |
| `group_number` + `ai_groupand_ba_numbers_section_number` | the composite-key alternative (scenario 8) |
| `ai_size_contract_count` | source name with `/` in it → alias mapping (`AISize/ContractCount`) |
| `funding_type` | plain varchar → `trim; empty → NULL` |
| `mds_retail_max_day_supply` + `..._incl_esn` | the workbook's name-collision pair and its rename |

Names, aliases, types, transforms and PII tags are copied verbatim from
`hive_raw_curated_mapping_v2_all_columns.xlsx` — this is a **subset of the
production contract, not a variant of it**.

## Isolation

The entity is `smartiq_pdp_e2e`, distinct from `smartiq_pdp`. Because the
framework keys watermarks, ledger history and entity locks by entity name,
this feed gets its own of each automatically. Tables are
`membership_common_raw.smartiq_pdp_e2e` and
`membership_common_curated.smartiq_pdp_e2e` — nothing is shared with
production except the databases themselves.

## Setup

1. **Source:** run section 0 of `source_test_data.sql` against the lower
   SQL Server to create `dbo.SmartIQ_PDP_E2E`.
2. **Hive:** run `raw_ddl_e2e.sql` and `curated_ddl_e2e.sql`, substituting
   `${LOCATION}`. Both are **ORC, EXTERNAL** — keep the `EXTERNAL` keyword:
   in Hive 3 a *managed* ORC table is created transactional (ACID) by
   default, and Spark 3.5 cannot write Hive ACID tables without the Hive
   Warehouse Connector. (Optional — the framework creates the tables itself if
   absent; pre-creating validates that your DDL and the framework agree.)
3. **Config:** copy `feed-smartiq-pdp-e2e.conf` + `smartiq-pdp-e2e-schema.conf`
   side by side (the feed `include`s the schema by relative name), then
   supply the connection details — see below.
4. **JDBC driver:** the Microsoft SQL Server driver must be on the
   classpath of the **driver and every executor** — the framework loads it
   reflectively in both places. Unless your cluster already ships one
   cluster-wide, pass it explicitly:
   `--jars /opt/jdbc/mssql-jdbc-12.4.2.jre11.jar` (match the version to
   your Java: `jre11` for Java 11). It is deliberately **not** bundled in
   the app jar — Microsoft's licence is separate from this framework's.
   A missing driver fails fast with `JDBC_001` before any read starts.
5. **Dry run first:**
   `--entity smartiq_pdp_e2e --validate-only` — config, contract and
   connectivity, no reads or writes.

The feed has `executor_probe = true`: every run proves **executors** can
reach SQL Server before extraction begins, which is the half a firewall
test from the edge node cannot cover.

## Where the MS SQL connection details go

All of them live in the `sqlserver { }` block at the top of
`feed-smartiq-pdp-e2e.conf`. Each key carries a lower-environment default
and an optional environment override, so the same file serves both
environments:

| Setting | Config key | Environment override |
|---|---|---|
| Host | `sqlserver.host` | `SMARTIQ_HOST` |
| Port | `sqlserver.port` | `SMARTIQ_PORT` |
| Database | `sqlserver.database` | `SMARTIQ_DB` |
| Schema + table | `sqlserver.table` | `SMARTIQ_TABLE` |
| Service account | `sqlserver.user` | `SMARTIQ_USER` |
| Password | — (never in the file) | `SMARTIQ_DB_PASSWORD` |

The `source` block composes them into the JDBC URL, so you never edit the
URL by hand:

```hocon
url = "jdbc:sqlserver://"${sqlserver.host}":"${sqlserver.port}";databaseName="${sqlserver.database}
```

Either edit the defaults in the file, or leave them and supply the values
at submit time.

### The cluster-mode environment trap (read this before running)

Both the `${?SMARTIQ_*}` HOCON overrides and the `env` password provider
read the **driver JVM's** environment. With `--deploy-mode cluster` the
driver is a YARN container on some cluster node, so `export FOO=...` in
your shell **never reaches it**. The two halves fail differently:

| What | If not forwarded | How you find out |
|---|---|---|
| `SMARTIQ_DB_PASSWORD` | run dies | `JDBC_002 Environment variable 'SMARTIQ_DB_PASSWORD' is not set` |
| `SMARTIQ_HOST` / `_DB` / `_TABLE` / `_USER` / `_PORT` | **silently falls back to the file's default** | nothing — check the driver log line `[Jdbc] url=... table=...` |

The second row is the dangerous one: there is no error, the run just uses
whatever the file says. Always confirm the `[Jdbc] url=` line names the
host and database you intended.

Two supported ways to get the values in:

**A. Cluster mode — forward them explicitly** (`spark.yarn.appMasterEnv.*`
sets the driver container's environment):

```bash
export SMARTIQ_HOST=lower-sql-01.corp.example.com
export SMARTIQ_DB=SmartIQ
export SMARTIQ_TABLE=dbo.SmartIQ_PDP_E2E
export SMARTIQ_DB_PASSWORD='...'

spark-submit ... \
  --conf spark.yarn.appMasterEnv.SMARTIQ_HOST="$SMARTIQ_HOST" \
  --conf spark.yarn.appMasterEnv.SMARTIQ_DB="$SMARTIQ_DB" \
  --conf spark.yarn.appMasterEnv.SMARTIQ_TABLE="$SMARTIQ_TABLE" \
  --conf spark.yarn.appMasterEnv.SMARTIQ_DB_PASSWORD="$SMARTIQ_DB_PASSWORD" \
  ...
```

`scripts/ingest_submit_common.sh` does this for you — list the names in
`INGEST_ENV_VARS` and it forwards each set one and warns about each unset
one:

```bash
INGEST_ENV_VARS="SMARTIQ_HOST,SMARTIQ_DB,SMARTIQ_TABLE,SMARTIQ_DB_PASSWORD" \
INGEST_EXTRA_FILES=smartiq-pdp-e2e-schema.conf \
INGEST_JARS=/opt/jdbc/mssql-jdbc-12.4.2.jre11.jar \
  scripts/run_ingest.sh feed-smartiq-pdp-e2e.conf smartiq_pdp_e2e INCR
```

Note the cost: a password passed this way sits in the `spark-submit`
command line, readable by `ps` on the submitting host and visible in the
YARN launch context. Fine for a lower-environment service account,
**not** for production — see the hardening note below.

**B. Client mode — plain `export` works** (`--deploy-mode client` makes
your shell the driver). Nothing sensitive touches a command line, and it
separates driver-side from executor-side failures. This is the quickest
way to get the E2E test moving:

```bash
INGEST_DEPLOY_MODE=client \
INGEST_JARS=/opt/jdbc/mssql-jdbc-12.4.2.jre11.jar \
INGEST_EXTRA_FILES=smartiq-pdp-e2e-schema.conf \
  scripts/run_ingest.sh /full/path/feed-smartiq-pdp-e2e.conf smartiq_pdp_e2e INCR
```

In client mode the wrapper skips the `appMasterEnv` forwarding entirely
(the driver already has your environment, and forwarding would put the
password on the command line for nothing) and passes the config by
**absolute** path, since a client-mode driver reads the original file
rather than a `--files` copy.

**Run cluster mode at least once before sign-off.** It is what Control-M
executes, and it exercises things client mode cannot: environment
forwarding, config shipping, and the driver's own network path to SQL
Server — which originates from a data node, not the edge node, so
firewall rules can differ.

**Also verified, not assumed:** substitutions resolve against environment
variables only — `-D` system properties are *not* consulted on the
`--conf-path` route, so `--conf spark.driver.extraJavaOptions=-Dsmartiq.host=...`
does nothing. The password is never read from the config file at all; it
is fetched at run time through the `env` secret provider, so it never
lands in source control or in a logged config dump.

For extra hardening the password can come from CyberArk, Azure Key Vault or
Conjur instead — swap the `password` provider block; the rest is unchanged.

## Run command

```bash
spark-submit --class com.hcsc.generic.ingest.app.IngestMain \
  --name ingest-smartiq_pdp_e2e --master yarn --deploy-mode cluster \
  --conf spark.yarn.appMasterEnv.SMARTIQ_HOST="$SMARTIQ_HOST" \
  --conf spark.yarn.appMasterEnv.SMARTIQ_DB="$SMARTIQ_DB" \
  --conf spark.yarn.appMasterEnv.SMARTIQ_TABLE="$SMARTIQ_TABLE" \
  --conf spark.yarn.appMasterEnv.SMARTIQ_DB_PASSWORD="$SMARTIQ_DB_PASSWORD" \
  --jars /opt/jdbc/mssql-jdbc-12.4.2.jre11.jar \
  --files feed-smartiq-pdp-e2e.conf,smartiq-pdp-e2e-schema.conf \
  ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --entity smartiq_pdp_e2e --mode INCR --run-id e2e-1 \
  --conf-path ./feed-smartiq-pdp-e2e.conf
```

The four `appMasterEnv` lines are **required in cluster mode** — without
them the password lookup fails with `JDBC_002` and the host/database/table
silently revert to the file's defaults. See the trap section above.

**Both files must be in `--files`** (the feed `include`s the schema) — this
is the one part no code fix can cover, because a file that was never
shipped cannot be found. The config is handed over with **`--conf-path`**;
`-Dconfig.file` also works now but reports missing files less clearly. See
the troubleshooting entry below.

## Running it with the wrapper script

The wrapper builds the whole `spark-submit` for you, so the `--jars`,
`--files` and environment-forwarding pieces cannot be forgotten.

**On the edge node, put these four things in one directory:**

| | |
|---|---|
| `ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar` | built from `main`, **after 2026-08-06** |
| `feed-smartiq-pdp-e2e.conf` | the feed |
| `smartiq-pdp-e2e-schema.conf` | the include — must sit beside the feed |
| `scripts/` | `ingest_submit_common.sh` + `run_ingest.sh`, keeping their relative layout (`run_ingest.sh` sources its sibling) |

**Set the connection details once per shell:**

```bash
export SMARTIQ_HOST=lower-sql-01.corp.example.com
export SMARTIQ_DB=SmartIQ
export SMARTIQ_TABLE=dbo.SmartIQ_PDP_E2E
export SMARTIQ_USER=svc_ingest
read -rs -p "SQL password: " SMARTIQ_DB_PASSWORD; export SMARTIQ_DB_PASSWORD; echo
```

`read -rs` keeps the password out of your shell history, which a plain
`export SMARTIQ_DB_PASSWORD=...` does not.

**Then run.** The feed does not declare `ingestion.execution`, so it is
COUPLED — raw and curated in one process, one job — which is
`run_ingest.sh`:

```bash
cd /path/to/that/directory

INGEST_DEPLOY_MODE=client \
INGEST_JARS=/opt/jdbc/mssql-jdbc-12.4.2.jre11.jar \
INGEST_EXTRA_FILES="$PWD/smartiq-pdp-e2e-schema.conf" \
  scripts/run_ingest.sh "$PWD/feed-smartiq-pdp-e2e.conf" smartiq_pdp_e2e INCR \
  --run-id e2e-1
```

Argument order is `<config> <entity> <FULL|INCR>`; anything after that
starting with `--` goes straight to the app, which is how the scenarios
below pass their `--run-id`. To use a jar that is not in the working
directory, either put its path in the 4th slot (before the flags) or set
`INGEST_JAR`.

**Start with a dry run** — it validates config, contract and connectivity
without reading or writing anything:

```bash
INGEST_DEPLOY_MODE=client INGEST_JARS=/opt/jdbc/mssql-jdbc-12.4.2.jre11.jar \
INGEST_EXTRA_FILES="$PWD/smartiq-pdp-e2e-schema.conf" \
  scripts/run_ingest.sh "$PWD/feed-smartiq-pdp-e2e.conf" smartiq_pdp_e2e INCR \
  --validate-only
```

**Wrapper environment variables:**

| Variable | Purpose |
|---|---|
| `INGEST_DEPLOY_MODE` | `cluster` (default, what Control-M runs) or `client` |
| `INGEST_JARS` | vendor JDBC driver(s), comma-separated — required for jdbc feeds |
| `INGEST_EXTRA_FILES` | files `include`d by the feed config, comma-separated |
| `INGEST_ENV_VARS` | names forwarded to a **cluster-mode** driver; ignored in client mode |
| `INGEST_JAR` | app jar path, instead of the positional slot |

For the later cluster-mode confirmation run, the same command plus the
forwarding list:

```bash
INGEST_DEPLOY_MODE=cluster \
INGEST_ENV_VARS="SMARTIQ_HOST,SMARTIQ_DB,SMARTIQ_TABLE,SMARTIQ_USER,SMARTIQ_DB_PASSWORD" \
INGEST_JARS=/opt/jdbc/mssql-jdbc-12.4.2.jre11.jar \
INGEST_EXTRA_FILES="$PWD/smartiq-pdp-e2e-schema.conf" \
  scripts/run_ingest.sh "$PWD/feed-smartiq-pdp-e2e.conf" smartiq_pdp_e2e INCR \
  --run-id e2e-cluster-1
```

**Reading the outcome.** Client mode prints the driver log to your
terminal; cluster mode needs `yarn logs -applicationId <appId>`. Either
way, check these three lines before trusting a green run:

```
[Config] --conf-path='...' includes resolve against '<dir>'   # the jar is current
[Jdbc]   url=jdbc:sqlserver://<host>...  table=<table>        # the RIGHT database
[Lock]   entity=smartiq_pdp_e2e lease released by run <id>    # clean exit
```

Exit code 0 means success, including a clean no-op. Non-zero propagates
the framework's `CFG_` / `JDBC_` / `PIPE_` / `CUR_` code — rerunning the
same job after a failure is always safe.

Explicit `--run-id` values matter: scenario 7 replays one by id. Use
`--deploy-mode client` for the first attempt if executor connectivity is
still unproven — it isolates driver-side from executor-side failures.

## Scenarios

Run one section of `source_test_data.sql`, then one pipeline run, then
check. **Counts assume the default `overlap = 300` (5 minutes)**, which
deliberately re-reads rows at the window boundary — those re-reads are the
idempotency test, not a fault.

| # | Source change | Extracted | RAW total | CURATED total | The point |
|---|---|---|---|---|---|
| 1 | initial 5 rows (`--run-id e2e-1`) | 5 | 5 | **4** | first publish; F004 appears twice → in-batch dedup keeps the 10:20 version; empty strings become NULL |
| 2 | F001 updated, F005 inserted (`e2e-2`) | 3 | 8 | 5 | insert **1** (F005), update **1** (F001), ignored **1** (F004 re-read at the overlap boundary — same key, same freshness → target wins) |
| 3 | F002 timestamp bumped only (`e2e-3`) | 2 | 10 | 5 | **version advance:** F002's content is unchanged so its hash matches — curated keeps the business values and does **not** restamp `last_modified_op`, but `last_modified_datetime` advances to 12:00. insert 0, update 0, ignored 2 |
| 4 | NULL-key row inserted (`e2e-4`) | 2 | 12 | 5 | the null-key row lands in RAW (faithful) but is **quarantined** before curated: one `ingest_rejects` row with `CUR_001`, `user_email_id` hashed in the payload |
| 5 | no change (`e2e-5`) | 1 | 13 | 5 | idempotency: the boundary row is re-read, nothing changes, run SUCCESS |

### Verification queries

```sql
-- counts per scenario
SELECT COUNT(*) FROM membership_common_raw.smartiq_pdp_e2e;
SELECT COUNT(*) FROM membership_common_curated.smartiq_pdp_e2e;

-- 1: in-batch dedup kept the newer F004, empty strings became NULL
SELECT file_name, ai_size_contract_count, funding_type,
       ai_groupand_ba_numbers_section_number
  FROM membership_common_curated.smartiq_pdp_e2e WHERE file_name IN ('F004.pdf','F003.pdf');
-- expect F004 -> 75 ; F003 section/mds columns NULL (not '')

-- 3: version advanced, business content and audit stamp untouched
SELECT file_name, last_modified_datetime, ai_size_contract_count, last_modified_op
  FROM membership_common_curated.smartiq_pdp_e2e WHERE file_name = 'F002.pdf';
-- expect 2026-03-03 12:00:00 , content unchanged , op still 'I'

-- 4: quarantine with PII masked
SELECT error_code, raw_record FROM ingest_audit.ingest_rejects
 WHERE entity = 'smartiq_pdp_e2e';
-- expect CUR_001 ; no plaintext e-mail address in raw_record

-- per-run outcome, counts and status
SELECT run_id, stage, status, raw_count, insert_count, update_count, event_ts
  FROM ingest_audit.ingest_run_audit
 WHERE entity = 'smartiq_pdp_e2e' ORDER BY event_ts;

-- watermark advanced only after curated succeeded
SELECT * FROM ingest_audit.ingest_watermarks WHERE entity = 'smartiq_pdp_e2e';
```

### 6. Watermark holds on failure

Add `publish { validation_query = "SELECT * FROM {table} WHERE file_name = 'F005.pdf'" }`
to the curated block and re-run after any source change. Expect: the run
fails, curated is **unchanged**, and `ingest_watermarks` still shows the
previous value. Remove the query and re-run — the delta is picked up. This
is the guarantee that a curated failure can never lose data.

### 7. Replay cannot regress curated

```
--stage curated --run-id e2e-1
```

Replays batch 1's RAW slice — which contains F001 at 10:00, older than the
11:00 version already in curated. Expect curated **unchanged**: the
freshness merge ignores the stale rows. This is also the recovery drill for
"raw succeeded, curated failed".

### 8. Composite keys (optional)

Switch `curated.merge.keys` to
`["group_number", "ai_groupand_ba_numbers_section_number"]`, point curated
at a fresh table, clear the watermark row for the entity, and load the two
rows in section 6 of the seed script. They share `GRP700` but differ by
section — expect **two** curated rows. A single-column or prefix-only match
would collapse them into one.

## Troubleshooting

**`include was not found: 'smartiq-pdp-e2e-schema.conf'`** (job dies with
YARN exit code 13, `ApplicationMaster: User class threw exception ...
ConfigException$IO`)

HOCON resolves `include` relative to the *parent directory* of the
including file. A **bare filename** — the natural form in a YARN container,
where `--files` drops everything in the working directory — has no parent,
so the include falls back to the classpath and is not found.

From 2026-08-06 the framework absolutises the config path on **both**
entry points (`--conf-path` and `-Dconfig.file`), so the bare-name case
resolves by itself, and a failed include now reports the directory it
searched and what is in it:

```
include was not found: 'smartiq-pdp-e2e-schema.conf' | searched directory
'/data/.../container_.../' which contains: [feed-smartiq-pdp-e2e.conf,
__spark_conf__, ...]. An included file must be shipped too, e.g.
--files /p/feed.conf,/p/feed-schema.conf
```

Read that bracket list first — it answers the question directly. Then, in
order:
1. **Is the schema file in the list?** If only the feed is there it was
   never shipped: add it to `--files` (comma-separated, no spaces). This is
   the most common cause and the one the fix cannot repair.
2. **Are you running a jar built before 2026-08-06?** An earlier revision
   absolutised only the `--conf-path` value, so `-Dconfig.file=<basename>`
   still failed, and the revision before that failed on both. `unzip -p
   <jar> META-INF/MANIFEST.MF` or compare the jar's timestamp against your
   last `mvn install`. Rebuild and re-upload before debugging anything else.
3. **Does the driver log line appear?** The run now logs
   `[Config] --conf-path='...' resolved='...' includes resolve against
   '<dir>'` before parsing. If that line is absent from the driver log, the
   jar is stale — see 2.

**`JDBC_001 Driver class 'com.microsoft.sqlserver.jdbc.SQLServerDriver' not
found on classpath`**

The Microsoft JDBC driver was not shipped. It is not bundled in the app jar
(separate licence), so pass it with `--jars`:

```bash
--jars /opt/jdbc/mssql-jdbc-12.4.2.jre11.jar
```

Use the `jre11` build to match Java 11; a `jre8` jar on Java 11 throws
`UnsupportedClassVersionError` instead. With the wrapper script, set
`INGEST_JARS` to the same path.

`--jars` distributes to the driver **and** the executors, which is what
this feed needs: the driver runs the health check and the watermark query,
and each executor opens its own connection for its partition of the read.
Placing the jar on only one side produces the same JDBC_001 later, from
an executor rather than the driver.

Reaching this error means config loading, credential resolution and the
entity lock all succeeded — it is the last piece of plumbing before the
first real read.

**`JDBC_002 Environment variable 'SMARTIQ_DB_PASSWORD' is not set`**

The driver container has no such variable. In `--deploy-mode cluster` your
shell's `export` does not reach the driver — forward it with
`--conf spark.yarn.appMasterEnv.SMARTIQ_DB_PASSWORD="$SMARTIQ_DB_PASSWORD"`
(or use `INGEST_ENV_VARS` with the wrapper script, or run client mode).
See "The cluster-mode environment trap" above.

Reaching this error is progress: config parsing and the schema `include`
both succeeded, or the run would have died earlier.

**Check the same submit for the SILENT half of this problem.** The
`SMARTIQ_HOST` / `_DB` / `_TABLE` / `_USER` / `_PORT` overrides have
defaults, so an unforwarded value produces no error — the run just uses
the file's default. Confirm the driver log line

```
[Jdbc] url=jdbc:sqlserver://<host>:1433;databaseName=<db> table=<table> ...
```

names the host and database you meant. A default of `SQLHOST-LOWER` in the
URL means nothing was forwarded.

A stack ending in `cleanupStagingDir` with *"Operation category READ is not
supported in state standby"* is a **secondary** error from the shutdown
hook against a standby HDFS NameNode — it hides the real failure above it.
Always read the first `ERROR ApplicationMaster: User class threw exception`
line, not the tail. Pull the whole log with
`yarn logs -applicationId <appId>`.

## Notes and honest limits

- **RAW accumulates the overlap re-reads** (13 rows for 8 logical records by
  scenario 5). That is the default `AT_LEAST_ONCE_APPEND` delivery mode
  working as designed — raw is history, curated is truth. Set
  `raw.delivery_mode = "DEDUPLICATED_APPEND"` with an identity column to
  suppress them if the duplication bothers reviewers.
- **A truly empty extraction** needs `overlap = "0"`; with an overlap the
  boundary row is re-read on every run by design.
- **Scenario 3 is the one to watch.** It is the newest correctness fix in
  the framework and the least intuitive: same content, newer version. If
  `last_modified_datetime` does *not* advance there, stop and report it.
- Timestamps in the seed data are fixed (March 2026) so results are
  reproducible; they are unrelated to the wall clock.
- This plan validates the *pipeline*, not the 364-column mapping. Column
  breadth is covered by `SmartIqMappingConfigTest` in the build; what only a
  real run can prove is connectivity, permissions, Hive DDL agreement,
  watermark persistence and executor behaviour.
