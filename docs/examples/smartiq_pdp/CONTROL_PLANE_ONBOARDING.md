# SmartIQ_PDP — Control Plane Onboarding, Step by Step

**Date:** 2026-08-19
**Applies:** [CONTROL_PLANE_IMPLEMENTATION_PLAN.md](../../reports/CONTROL_PLANE_IMPLEMENTATION_PLAN.md)
(phases and contracts) to the SmartIQ_PDP feeds specifically.
**Pilot rationale:** SmartIQ is the only feed with production history, and
it is **hand-authored** — so it exercises the import path, which a
wizard-generated feed would not.

Each step states what to do, how to prove it worked, and what failure looks
like. Nothing here requires the pipeline to change.

---

## Step 0 — Establish the baseline (do this first)

Before any control-plane work, record what "correct" means, so every later
step has something to compare against.

```sql
-- against membership_common_raw, via beeline or your SQL client
SELECT run_id, stage, status, raw_count, accepted_count,
       insert_count, update_count, config_fingerprint, event_ts
FROM   membership_common_raw.ingest_run_audit
WHERE  entity = 'smartiq_pdp'
ORDER  BY event_ts DESC LIMIT 20;
```

**Record:** the most recent successful `run_id`, its counts, and its
`config_fingerprint`. That fingerprint is the value the store must reproduce
in Step 2 — if it doesn't, the config you imported is not the config that
ran.

**Also record** the current watermark, so no step here is confused with a
data change:

```sql
SELECT entity, watermark_value, watermark_version, updated_ts
FROM   membership_common_raw.ingest_watermarks
WHERE  entity = 'smartiq_pdp'
ORDER  BY watermark_version DESC LIMIT 5;
```

---

## Step 1 — Validate the existing feeds (P1, no app required)

This is already done and permanently pinned:
`ShippedFeedValidationTest` validates both SmartIQ feeds on every build.

**Prove it locally:**

```bash
mvn -o -pl ingestion-config-gen test \
  -Dsuites='com.hcsc.generic.ingest.confgen.service.ShippedFeedValidationTest'
```

Expected: `shipped feed validates: smartiq_pdp` and `smartiq_pdp_e2e` pass.

**What this already caught.** Writing this step found that
`DryRunValidator` required `raw.path`, which `HiveSink` documents as
optional ("absent path = managed table in the warehouse") and which
**neither SmartIQ feed declares** — nor do either of the reference
templates. Both production feeds were rejected by the validator that the
control plane's entire promise rests on. Fixed, with the rule pinned
independently of the shipped files.

**Do not skip to Step 2 if this fails.** A feed that cannot pass validation
cannot be meaningfully stored — you would be versioning something the
application will refuse to submit.

### Step 1b — Extend the lint to your own feed directory

When feeds live outside this repo (`$SMARTIQ_CONF_DIR` on the server), point
the same check at them. Until the `feed-lint` CLI exists (plan P1), a
five-line ScalaTest against your directory is enough and runs in CI.

---

## Step 2 — Import SmartIQ into the store (P2)

**Key fact, verified:** the import path does **not** need wizard answers.
`validate`, `fingerprint` and `write` all take a parsed `Config`; only
`render` needs a wizard-shaped definition. A hand-authored feed is imported
as-is.

For each of the two feeds:

1. Parse the deployed `.conf` and extract `feeds.<entity>`.
2. `FeedService.validate` — must be `ok`.
3. `FeedService.fingerprint` — store it.
4. Insert `feed` + `feed_version` rows with:
   - `settings = NULL` — **imported versions are not re-renderable**, and
     that is correct: pretending otherwise would risk re-rendering a
     production feed into something subtly different.
   - `rendered_hocon` = the file's exact bytes
   - `rendered_sha256` = hash of those bytes — the change detector
   - `status = 'APPROVED'`, `change_ref` = the CO that deployed it
   - `validation_ok`, `sql_preview` from step 2

**Verify — this is the important check:**

```
feed_version.config_fingerprint  ==  the fingerprint recorded in Step 0
```

If they differ, the file you imported is **not** what produced that run.
Investigate before proceeding: someone edited the deployed config, or an
override was in effect.

**Expected fingerprints today** (from the shipped configs — recompute
against your *deployed* files, which is the point of the check):

| Feed | Fingerprint |
|---|---|
| `smartiq_pdp` | `v1:69f040613641920c392216ac448230ad` |
| `smartiq_pdp_e2e` | `v1:e3b29a101b3b6271b5764d1962e79e1b` |

> These are structural digests. Remember they **collide on value-only
> changes** — that is why `rendered_sha256` exists and why the fingerprint
> is a corroboration check, never the version identity.

---

## Step 3 — Submit an e2e run from the store (P3)

Pilot on `smartiq_pdp_e2e` — 11 columns, its own entity and tables, safe to
run beside production.

1. Generate `run_id` (e.g. `cp-e2e-<timestamp>`).
2. **Insert `feed_submission` first**, then execute. Crash-after-insert
   leaves the run attributable; crash-before leaves an unexplained ledger
   row, which is exactly what bypass detection is meant to flag.
3. Write `rendered_hocon` (and the schema file) to `$SMARTIQ_CONF_DIR`.
4. Invoke the existing launcher — do **not** rebuild the spark-submit
   command:

```bash
./scripts/run_smartiq.sh e2e INCR --run-id cp-e2e-<timestamp>
```

5. Record the outcome: exit code **and** ledger status. In cluster mode the
   exit code reflects the YARN application state, not the driver's
   classified code, so the ledger is authoritative.

**Verify the join works end to end:**

```sql
SELECT run_id, stage, status, raw_count, accepted_count
FROM   membership_common_raw.ingest_run_audit
WHERE  run_id = 'cp-e2e-<timestamp>';
```

and in the store, that `feed_submission.run_id` matches. **That single
matching id is the whole control-plane contract** — no framework change,
no new ledger column.

---

## Step 4 — Reconciliation reports (P4)

Run these against the real ledger before trusting any dashboard.

**Bypass detection** — runs nobody submitted through the app:

```sql
SELECT DISTINCT run_id, entity, MIN(event_ts) AS first_seen
FROM   membership_common_raw.ingest_run_audit
WHERE  entity IN ('smartiq_pdp','smartiq_pdp_e2e')
GROUP  BY run_id, entity;
-- left-anti join against feed_submission.run_id in the store
```

Every historical run will appear until you backfill submissions for them —
decide deliberately whether to backfill or to set a cutover timestamp.

**Override reconciliation** — an unmanaged override in effect:

```sql
SELECT run_id, entity, config_fingerprint, event_ts
FROM   membership_common_raw.ingest_run_audit
WHERE  config_fingerprint LIKE '%+ovr:%'
ORDER  BY event_ts DESC;
```

Any hit without `feed_submission.override_applied = true` is a policy
violation. Note SmartIQ currently sets `INGEST_OVERRIDE_FILE` **only if you
have configured it** — if this query returns nothing, no override has ever
applied.

**Unapproved versions:** `feed_submission` joined to
`feed_version.status <> 'APPROVED'`. Should be empty.

---

## Step 5 — Production cutover

Only after Steps 3–4 are green on e2e for several scheduled cycles.

1. Import `smartiq_pdp` (done in Step 2) and confirm the fingerprint match.
2. Run **one** production load through the app-driven path with a pinned
   `run_id`, out of the Control-M schedule, and compare counts to Step 0's
   baseline.
3. Switch Control-M to call the app's submission entry point rather than
   `run_smartiq.sh` directly.
4. Keep `run_smartiq.sh` working. It is the break-glass path when the app is
   unavailable, and Step 4's bypass report makes its use visible rather than
   invisible.

**Rollback at any point:** stop submitting through the app; Control-M calls
`run_smartiq.sh` as it does today. Nothing in the framework changed, so
there is nothing to revert.

---

## Testing summary

| Level | What it proves | Where |
|---|---|---|
| Unit | shipped SmartIQ feeds validate; managed-table feeds are legal | `ShippedFeedValidationTest` (in `main`) |
| Unit | headless render ≡ wizard render; `write` cannot ship what it did not validate | `FeedServiceTest` (in `main`) |
| Integration | store round-trip preserves the artifact byte-for-byte | P2, to build |
| Golden | an artifact from the store is executed by the real `IngestMain` | P3, to build |
| System | Step 3 on e2e: `run_id` present in both store and ledger | this document |
| System | Step 4 reports catch a seeded bypass and a seeded override | this document |
| Acceptance | Step 5: production counts match the Step 0 baseline | this document |

---

## What can go wrong

| Symptom | Cause | Action |
|---|---|---|
| Import fingerprint ≠ Step 0 fingerprint | deployed config differs from the repo copy, or an override applied | diff the deployed file against the repo; check the ledger for `+ovr:` |
| Validation fails on a deployed feed | drift between repo and server copies | reconcile before storing; do not "fix" by relaxing validation |
| e2e run starts but no ledger row | wrong `--run-id` plumbing, or the run died before the first stage | check the driver log's `[Build]` and `[Outcome]` lines |
| Every historical run flagged as bypass | expected — they predate the store | backfill submissions or set a cutover timestamp |
| App cannot validate: `NoClassDefFoundError` on `DataType` | `spark-sql` missing from the app classpath | add it; the authoring path parses DDL types (plan §2) |
