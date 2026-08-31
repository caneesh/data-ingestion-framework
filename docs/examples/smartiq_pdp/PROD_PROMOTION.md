# SmartIQ PDP — Production Promotion Checklist

**State this assumes:** the feed runs green in the test environment under
the final table names (`order_capture_smartiq_pdp` raw,
`order_capture_pdp_forms` curated). Production has **no** SmartIQ tables
yet — which simplifies promotion:

> **`rename_to_order_capture.sql` does NOT apply to production.** It
> migrates environments that ran under the old names. Prod is created
> under the new names from day one via `raw_ddl.sql` / `curated_ddl.sql`,
> and no bridge view is needed — consumers get one name, ever.

## Phase 0 — Gates (close these BEFORE scheduling the promotion)

These are the open items this feed has carried; in a real production
environment they stop being backlog and start being blockers.

1. **TLS.** The shipped feed carries the stopgap
   (`allow_insecure_tls = true`,
   `connection_properties { trustServerCertificate = "true" }`) —
   encryption on, certificate validation OFF. Production requires the CA
   import on the driver **and every executor**:

   ```bash
   keytool -importcert -alias corp-ca -file corp-ca.crt \
     -keystore "$JAVA_HOME/lib/security/cacerts" -storepass changeit
   ```

   Then **delete both lines from the prod copy of the feed** and prove the
   connection still succeeds (`--validate-only`). Every run logs
   `INSECURE TLS override approved` while the stopgap is active — that
   line appearing in prod means this gate was skipped.
2. **Privacy sign-off** for the PII/PHI-tagged curated columns (submitter
   and contact identities). The tags drive reject-payload masking; the
   sign-off is organizational and must be on record before prod data
   flows.
3. **Prod source access**: service account valid against the production
   SQL Server, firewall open from the driver host AND the data nodes
   (`executor_probe = true` verifies the executor half on every run).
4. **Alerting**: production `SMARTIQ_ALERT_WEBHOOK` value ready — without
   it, failure notifications go nowhere and only Control-M sees a red job.

## Phase 1 — Environment preparation

1. Lay out the site directories per DEPLOYMENT.md "Site layout" (scripts
   together in `src/scripts/...`; jar + JDBC driver in `bin/...`; feed,
   schema, `smartiq.env`, `smartiq.pwd` in `params/...`).
2. Build from `main`, ship via `build_bundle.sh` (or the zip flow), verify
   `MANIFEST.sha256` on the server.
3. `smartiq.env` with **production** values: `SMARTIQ_HOST/DB/TABLE/USER`,
   `SMARTIQ_CONF_DIR`, `SMARTIQ_JAR`, `SMARTIQ_JDBC_DRIVER`,
   `SMARTIQ_DEPLOY_MODE=client`, `INGEST_DRIVER_MEMORY=4g`,
   `SMARTIQ_ALERT_WEBHOOK`. `chmod 600`; `smartiq.pwd` `chmod 400`.

## Phase 2 — Hive

1. **Collision check first** — the databases are shared:

   ```sql
   SHOW TABLES IN membership_common_raw LIKE 'ingest_*';
   SHOW TABLES IN membership_common_raw LIKE 'order_capture*';
   ```

   The feed names its control tables explicitly; if another pipeline
   already owns any `ingest_*` name here, rename yours in the feed config
   BEFORE the first run — the framework appends to whatever exists.
2. **Create the data tables ONE way** — either run the two DDLs with
   `${LOCATION}` substituted (`sed`, then confirm `DESCRIBE FORMATTED`
   shows no `$`), or let the framework create them. Never both at one
   path: that is the schema-mismatch trap the test plan documents.

## Phase 3 — First light (manual, before any schedule)

In order, each gating the next:

```bash
run_smartiq.sh prod INCR --validate-only      # config, contract, connectivity
run_smartiq.sh prod INCR --dry-run            # reads + reconciles, writes nothing
run_smartiq.sh prod FULL --run-id pdp-prod-initial-1   # the initial load
run_smartiq.sh prod INCR --stage reconcile    # proves source==curated after it
```

Verify after the FULL: `[Build]` line (right jar), `[Jdbc] url=` (the
PRODUCTION host — the silent-fallback trap), ledger row with the pinned
run id and plausible counts, reconciliation checks all passed, watermark
row written. The reconcile stage green is the strongest single signal:
every source key made it to curated.

## Phase 4 — Schedule

1. Control-M folders per the runbook ("Control-M folder design") — if the
   folders built during test point at a test env file, the promotion is
   the `smartiq.env` / paths swap, not new folders.
2. Run the build guide's **verification steps 1–5**, including firing the
   freshness alarm once and the exit-30 drill. Step 5 is not optional in
   prod: it is the only proof alert routing works before a real failure.
3. Size the freshness threshold from the production source's rhythm (it
   must exceed the longest legitimate gap — weekends included), then
   enable calendars.

## Phase 5 — First week

- Watch the first scheduled cycles; the freshness monitor covers the
  silent-absence case.
- After a representative week, read the accepted-count history and decide
  `min_accepted_rows` (the query is commented in the feed config). If the
  quietest legitimate window is never zero, set the floor to 1.
- Announce the curated table (`membership_common_curated.order_capture_pdp_forms`)
  to consumers — one name, no view, no deprecation schedule needed.

## Rollback posture

Phases 1–2 are inert (files and empty tables). Phase 3's FULL load is the
first irreversible-ish step — and even it is recoverable: drop the data
tables and the watermark/ledger rows for the entity and the environment is
clean again. There is no old state in prod to protect, which is the whole
advantage of promoting under the final names.
