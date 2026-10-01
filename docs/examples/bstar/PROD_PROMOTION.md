# bstar — Production Promotion Checklist

**State this assumes:** both e2e feeds have completed
`lower-env/LOWER_ENV_TEST_PLAN.md` green, and `bluestar_current.priv_addr`
/ `bluestar_current.sub_prem_det` already exist in production (created
ahead of the pipeline for consumers from `ddl/curated_ddl.sql`) and are
empty.

There is no RAW table to create: `raw.mode = SOURCE` reads `bstar_raw`
directly. `bluestar_raw` holds **only** the control tables, which the
framework creates on first run (`CREATE TABLE IF NOT EXISTS`).

## Phase 0 — Gates

1. **Read access** for the pipeline's principal on `bstar_raw.priv_addr`
   and `bstar_raw.sub_prem_det` (metastore + HDFS). The hive source has
   no credential of its own; it is the Spark session's identity. Prove it:
   `run_bstar.sh prod priv_addr INCR --validate-only` (HIVE_002 if the
   table is not visible).
2. **Write access** on `bluestar_raw` (control tables) and
   `bluestar_current` (curated). The framework never issues
   `CREATE DATABASE`; both must exist.
3. **Source owner agreement**, in writing: partitions are never rewritten
   in place; a late delivery arrives within 3 days (sizes `lookback`);
   `MSCK REPAIR` / `ADD PARTITION` is the owner's job and happens before
   the daily window the pipeline runs in; the retention of `bstar_raw`
   bounds how far back a replay can reach (HIVE_008 beyond it).
4. **Privacy sign-off** for the PII-tagged curated columns (address lines,
   zip, phone; dob). Tags drive reject-payload masking; the sign-off is
   organizational.
5. **Alerting**: production `BSTAR_ALERT_WEBHOOK` ready.
6. **Sizing for sub_prem_det first light**: ~30.9 GB of uncompressed text
   is parsed once. Agree the executor count / memory
   (`INGEST_EXECUTOR_MEMORY`) and the queue with the platform team before
   the FULL read, and run it in a maintenance window.

## Phase 1 — Environment preparation

1. Site layout per DEPLOYMENT.md: scripts together; jar in `bin/...`; the
   two feeds, the two contracts and `bstar.env` in `params/...`.
2. Build from `main`, ship via `build_bundle.sh`, verify `MANIFEST.sha256`.
3. `bstar.env` with **production** values: leave `BSTAR_SOURCE_DB`,
   `BSTAR_CONTROL_DB`, `BSTAR_CURATED_DB` **unset** so the feed defaults
   (`bstar_raw` / `bluestar_raw` / `bluestar_current`) apply — the override
   variables exist for lower environments. `BSTAR_DEPLOY_MODE=cluster` for
   Control-M, `INGEST_DRIVER_MEMORY=4g`, `BSTAR_ALERT_WEBHOOK`. `chmod 600`.

## Phase 2 — Hive

1. **Collision check** — `bluestar_raw` is new and should be empty:

   ```sql
   SHOW TABLES IN bluestar_raw;
   SHOW TABLES IN bluestar_current;
   DESCRIBE FORMATTED bluestar_current.priv_addr;     -- Location has no '$'
   ```

2. **Partition registration**: confirm the source's partitions are
   registered, not just present on HDFS — the pipeline sees the metastore,
   never the filesystem:

   ```sql
   SHOW PARTITIONS bstar_raw.priv_addr;          -- newest file_date should be today's/yesterday's
   ```

## Phase 3 — First light (manual, before any schedule)

Order matters; each gates the next. **INCR, not FULL**: with no watermark
row, INCR reads everything above `initial_value` — the whole table — and
commits the watermark. FULL is for a deliberate reload and is refused once
a watermark exists (HIVE_007).

```bash
run_bstar.sh prod priv_addr INCR --validate-only
run_bstar.sh prod priv_addr INCR --dry-run --run-id pa-prod-dry
run_bstar.sh prod priv_addr INCR --run-id pa-prod-initial-1
run_bstar.sh prod priv_addr INCR --stage reconcile          # source keys == curated keys
```

Verify after the initial run: `[Build]` line (right jar); the driver's
`[HiveSource] bstar_raw.priv_addr:` lines name the PRODUCTION source (the
silent-fallback trap);
ledger row `pa-prod-initial-1` with `window_start = 1900-01-01|00.00.00`,
`window_end` = the newest partition, plausible counts; checks all passed;
one watermark row (version 1); `SELECT COUNT(*) ... WHERE is_deleted IS NOT
TRUE` plausible against the source owner's live-row estimate. Then the same
four commands for `sub_prem_det` in its maintenance window.

The reconcile stage green is the strongest single signal: every distinct
source key made it to curated.

## Phase 4 — Schedule

1. Control-M folders per the runbook ("Control-M folder design", the
   `BSTAR_INGESTION` / `BSTAR_AUDIT` section): one INCR job per entity
   after the source's morning landing, reconcile off-peak, retention
   weekly, and the two freshness checks (`check_freshness.sh` on the
   ledger, `check_source_freshness.sh` on the source's partitions).
2. Fire the freshness alarm once and the exit-30 drill (HIVE_007 via FULL)
   once — the only proof alert routing works before a real failure.
3. Size the ledger freshness threshold above the longest legitimate gap
   between source landings (weekends included).

## Phase 5 — First week

- Watch the first cycles; `log_partition_counts` is off in prod — turn it on
  through the override file if a day's counts look wrong.
- After a week, decide `min_accepted_rows` from the accepted-count history
  (query in the feed's audit block). A CDC feed can legitimately deliver
  zero rows on a quiet day; set the floor only if the minimum is never 0.
- Announce `bluestar_current.priv_addr` / `sub_prem_det` as live to the
  consumers who built against the pre-created schema.

## Rollback posture

Nothing in Phases 1–2 touches data. Phase 3 writes only `bluestar_current`
and `bluestar_raw.ingest_*`; the source is read-only throughout (the
pipeline never writes, alters or drops it). To start over: truncate the two
curated tables and delete the entity's rows from `ingest_watermarks`,
`ingest_run_audit`, `ingest_reconciliation`, `ingest_run_locks`. The source
is untouched by definition.
