# Quick Reference — one page

Spark/Hive ingestion: source → RAW (faithful history) → CURATED
(freshness-merged current state), with audit ledger, reconciliation,
watermarks and entity locks. Detail lives in exactly four documents:

| Need | Document |
|---|---|
| operate / debug / Control-M | [operations/OPERATIONS_RUNBOOK.md](operations/OPERATIONS_RUNBOOK.md) |
| install / deploy / promote | [operations/DEPLOYMENT.md](operations/DEPLOYMENT.md) |
| every config key + error code | [architecture/CONFIGURATION_MODEL.md](architecture/CONFIGURATION_MODEL.md) |
| onboard a new feed | [development/DEVELOPER_GUIDE.md](development/DEVELOPER_GUIDE.md) |

Worked example (configs, DDLs, tests, promotion): [examples/smartiq_pdp/](examples/smartiq_pdp/)

## Build and deploy

```bash
mvn clean package        # -> ingestion-app/target/...-jar-with-dependencies.jar
```

Server layout (SmartIQ): scripts → `src/scripts/membership/smartiq_pdp/`,
jar + JDBC driver → `bin/membership/smartiq_pdp/`, feed + schema +
`smartiq.env` (600) + `smartiq.pwd` (400) → `params/membership/smartiq_pdp/`.
All three launcher scripts must sit together; feed and schema must sit together.

## Run

```bash
export SMARTIQ_ENV_FILE=/datalakebin/prod/gold/integration/params/membership/smartiq_pdp/smartiq.env
run_smartiq.sh e2e|prod INCR --validate-only      # no writes
run_smartiq.sh prod INCR --run-id <id>            # a load
run_smartiq.sh prod INCR --stage reconcile        # source-vs-curated proof
run_smartiq.sh prod INCR --stage retention        # purge per retention block
```

Generic feeds: `run_ingest.sh <conf> <entity> FULL|INCR [flags]`.
**FULL is initial-load or rewind-first only** — never scheduled (runbook
"Initial load" / §2.3).

## Minimum viable feed

```hocon
feeds.my_feed {
  entity = my_feed                        # NEVER rename: keys watermark/ledger/locks
  source  { type = jdbc, url = ..., table = ...,
            incremental { watermark_type = TIMESTAMP, watermark_columns = [...],
                          initial_value = ..., watermark_store { type = hive, database = ... } } }
  schema  { version = "1", columns = [ { name = ..., type = ..., business_key = true } ] }
  raw     { database = ..., table = ... }         # no path = managed table
  curated { database = ..., table = ...,
            merge { keys = [...], freshness { column = ... } } }
  audit   { database = ... }                      # the ledger — required
  rejects { database = ..., max_reject_percent = 1.0, max_reject_count = 100 }
  concurrency { lease_minutes = 30 }
}
```

Validation is automatic and loud: `CFG_*` at startup, `CUR_*`/`HDR_*` at
run time — every code is in CONFIGURATION_MODEL.md.

## Debug — in this order

1. **`[Build]`** first line of the driver log — wrong timestamp = stale jar,
   stop trusting everything else.
2. **`[Jdbc] url=`** — the host/db actually used (unforwarded env vars fall
   back SILENTLY to config defaults).
3. **Exit code** (client mode): `10` transient → retry OK · `20` data
   integrity → never retry · `30` config → fix first · `1` unclassified.
4. **The ledger**:

```sql
SELECT stage, status, raw_count, accepted_count, message
FROM   <audit_db>.ingest_run_audit WHERE run_id = '<id>' ORDER BY event_ts;

SELECT check_name, expected, actual FROM <audit_db>.ingest_reconciliation
WHERE  run_id = '<id>' AND passed = false;

SELECT watermark_value FROM <audit_db>.ingest_watermarks
WHERE  entity = '<entity>' ORDER BY watermark_version DESC LIMIT 1;
```

Top failures → runbook entry: `include was not found` (ship BOTH conf
files via `--files`) · `JDBC_002` env var not set (cluster mode doesn't
inherit your shell) · `PIPE_001` lock held (crashed holders self-clear in
~lease_minutes) · `watermark_continuity` after a reset (expected — §2.3) ·
`accepted_meets_minimum` (extract came back empty).

## Golden rules

- The **entity** is permanent; table names are config.
- The watermark is **append-only**: rewind = INSERT a higher version (§2.3),
  never UPDATE.
- Reruns are always safe: same run-id + `--resume`.
- A run that never happens writes **nothing** — the freshness monitor
  (`check_freshness.sh <entity> <hours>`) is the only detector.
- Ops change without a change order: override file
  (`INGEST_OVERRIDE_FILE`) — wins over the feed, logged loudly, delete
  after.
