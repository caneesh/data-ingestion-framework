# SmartIQ PDP — QA Test Cases

Manual/environmental test cases for the SmartIQ order-capture feed.
**Scope**: what only a real environment can prove — connectivity,
permissions, scheduling, recovery, end-to-end counts. Pure logic
(merge arithmetic, validators, parsers) is covered by the automated
suite (~880 tests) and is *not* re-tested by hand here.

**Conventions**: run in the lower environment unless the case says PROD.
Seed data = `lower-env/ddl/source_test_data.sql`; expected counts assume
the default `overlap = 300` (see LOWER_ENV_TEST_PLAN.md). Record actual
results and the `run_id` of every run — the ledger is the evidence.

Priorities: **P1** = release-blocking, **P2** = must pass before prod
promotion, **P3** = should pass, defect tolerable with a ticket.

---

## A. Configuration and validation

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-A1 | P1 | Clean validate-only | `run_smartiq.sh e2e INCR --validate-only` | Exit 0; no writes anywhere; log shows contract resolution; `[Jdbc] url=` names the intended host/db |
| QA-A2 | P1 | Missing schema file is named | Move `smartiq-pdp-e2e-schema.conf` aside; run A1 | Fails BEFORE submit (preflight names the missing file) — never reaches YARN |
| QA-A3 | P1 | Misconfigured feed fails with a coded error | Temporarily set `curated.merge.deletes.mode = "BOGUS"` in a copy; validate | Named `CUR_005`-family error listing valid modes; restore the file |
| QA-A4 | P2 | Override file wins and is audited | Create override with `concurrency.lease_minutes = 99`; run with `INGEST_OVERRIDE_FILE` set | `[Override]` log lines name the path (never the value); ledger `config_fingerprint` ends `+ovr:<digest>` |
| QA-A5 | P2 | Missing override fails closed | Point `INGEST_OVERRIDE_FILE` at a nonexistent path | Run fails `CFG_019`; it does NOT silently run without the override |

## B. Extraction and watermark

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-B1 | P1 | Initial FULL load | Fresh tables + seed section 0; `run_smartiq.sh e2e FULL --run-id qa-b1` | RAW = source row count; curated = distinct business keys; watermark row created at max `LastModifiedDatetime` |
| QA-B2 | P1 | Incremental picks up only the delta | Seed section 2 (F001 update, F005 insert); INCR run | Extracted = delta + overlap boundary rows only (see scenario table); watermark advances |
| QA-B3 | P1 | Empty window is a clean no-op | INCR run with no source change | Exit 0; ledger SUCCESS; curated untouched; nothing quarantined |
| QA-B4 | P1 | Curated failure holds the watermark | Add a failing `publish.validation_query`; change a source row; run | Run FAILS; curated unchanged; `ingest_watermarks` still shows the previous value; remove query, rerun → delta arrives (nothing lost) |
| QA-B5 | P2 | Watermark reset is detected, not silent | Reset the watermark (runbook §2.2); run | `watermark_continuity` reconciliation failure whose message explains the reset case and the WARN-for-one-run path |

## C. RAW landing

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-C1 | P1 | Partitioning and load-type flag | After B1/B2: `SHOW PARTITIONS`; `SELECT file_type, COUNT(*) ... GROUP BY file_type` | Single-key `ingest_dt` partitions; `F` rows from FULL, `I` from INCR |
| QA-C2 | P1 | Lineage on every row | Sample rows: `run_id`, `record_hash`, extract-window columns | `run_id` matches the ledger; `record_hash` non-null; no NULL lineage |
| QA-C3 | P3 | Overlap re-reads are visible, not hidden | After several INCR runs, compare RAW total vs logical records | RAW total > logical count by exactly the boundary re-reads; `raw_overlap_reread`/ledger explains them |

## D. Curated merge (the business-visible behaviour)

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-D1 | P1 | In-batch dedup on initial load | Scenario 1 (F004 twice in seed) | Curated has ONE F004 row — the 10:20 version; empty strings landed as NULL |
| QA-D2 | P1 | Update vs insert accounting | Scenario 2 | Ledger: insert 1 (F005), update 1 (F001), ignored 1; curated F001 shows new values, original `create_timestamp`, `last_modified_op='U'` |
| QA-D3 | P1 | Version advance without churn | Scenario 3 (timestamp bumped, content unchanged) | Curated F002: `last_modified_datetime` ADVANCES, business columns and `last_modified_op` UNCHANGED; ledger insert 0 / update 0 / ignored 2 |
| QA-D4 | P1 | Null business key quarantined with PII masked | Scenario 4 (NULL `FileName` row) | Row lands in RAW; ONE `ingest_rejects` row, `error_code CUR_001`; `user_email_id` hashed in `raw_record`; run SUCCEEDS; curated count unchanged |
| QA-D5 | P1 | Stale replay cannot regress curated | `--stage curated --run-id qa-b1` (replays old F001) | Curated UNCHANGED — freshness merge ignores the older version |
| QA-D6 | P2 | Source delete is retained (IGNORE policy) | Delete one seeded row at source; INCR run | Curated still holds the row; nothing errors — this is the declared policy, verified by QA-F4 |

## E. Audit ledger

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-E1 | P1 | Every stage, every run, with provenance | After any run: query `ingest_run_audit` for its run_id | STARTED→SUCCESS per stage; counts populated; `framework_version` = the jar's `[Build]` stamp; `config_fingerprint` non-empty; window recorded |
| QA-E2 | P1 | All reconciliation checks recorded, passing | Query `ingest_reconciliation` for the run | Every check `passed=true`, including `raw_equals_accepted` and `curated_accounts_for_accepted_rows` with matching numbers |
| QA-E3 | P2 | Volume floor catches an empty extract | Via override file set `audit.reconciliation.min_accepted_rows = 1`; run with no source change | Run FAILS with `accepted_meets_minimum expected=>=1 actual=0`; check recorded; remove override |

## F. Independent reconcile stage

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-F1 | P1 | Clean pass on a consistent feed | `run_smartiq.sh e2e INCR --stage reconcile` | Exit 0; three checks in `ingest_reconciliation`; `source_keys_present_in_curated` passed |
| QA-F2 | P1 | Real loss is detected | INSERT a row at source; run reconcile BEFORE any load | `source_keys_present_in_curated` FAILS with actual=1; job still exit 0 (REPORT) but the failure notification arrives; then load + reconcile → clean |
| QA-F3 | P2 | Post-load edit is NOT reported as loss | UPDATE a loaded row's timestamp at source; reconcile without loading | Key checks PASS — lag is not loss (the false-alarm case) |
| QA-F4 | P2 | Upstream deletes are counted, not alarmed | After QA-D6, run reconcile | `curated_keys_absent_from_source` reports the count, `passed=true`; this number is the evidence the IGNORE policy needs review if it grows |

## G. Failure handling and recovery

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-G1 | P1 | Configuration failure classifies as 30 | Client mode, unset/blank the password source | `JDBC_002` in the log; `[Outcome] class=CONFIGURATION exitCode=30`; shell exit 30 |
| QA-G2 | P1 | Lock contention classifies as 10 | Start a run; start a second for the same entity while the first holds the lock | Second fails `PIPE_001`; exit 10; first run unaffected |
| QA-G3 | P1 | Crashed run self-clears | `kill -9` the driver mid-run; wait past the heartbeat window; rerun | Rerun acquires the lock (takeover/lease expiry) without manual cleanup; no data anomaly afterward (QA-E2 on the rerun) |
| QA-G4 | P1 | Resume skips completed stages | Rerun a FAILED run's exact command with `--resume --run-id <same>` | `already SUCCESS ... skipping (resume)` for completed stages; run completes; reconciliation clean |
| QA-G5 | P2 | Failure notification delivered | With `SMARTIQ_ALERT_WEBHOOK` set, force any failure | Webhook receives `[CLASS]`-prefixed message; message is redacted (no credential material) |

## H. Security

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-H1 | P1 | No secrets in any log | After a full day of runs: grep driver + YARN logs for the password value | Zero hits; SQL is logged as `queryHash=` only |
| QA-H2 | P1 | TLS posture is loud | Inspect a run's log | While the stopgap is active: exactly one `INSECURE TLS override approved` WARN. **In PROD after the CA import: this line MUST be absent** |
| QA-H3 | P2 | Secret file hygiene | `ls -l` `smartiq.pwd`, `smartiq.env` | `400` / `600`, owned by the service account; `smartiq.pwd` value absent from `ps` output during a run |
| QA-H4 | P2 | Reject payloads mask PII | Re-check QA-D4's reject row | Contact/email columns hashed, non-PII columns readable |

## I. Scheduling (Control-M)

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-I1 | P1 | Order-id traceability | Order `ORDER_CAPTURE_PDP_INCR_LOAD` manually | Ledger run_id = `pdp_<orderid>`; sysout shows `[Build]` and `[Jdbc]` lines |
| QA-I2 | P1 | Rerun path | Rerun the same order | Resume-skip lines; no `raw_equals_accepted` confusion |
| QA-I3 | P1 | Exit-30 routing drill | Move `smartiq.pwd` aside; order the load | Job NOTOK, **no rerun**, feed-owner notification fires; restore file |
| QA-I4 | P1 | Freshness alarm drill | Run monitor job with threshold `1` | Exit 1 → stale alert to the FEED destination; restore threshold |
| QA-I5 | P2 | Freshness broken-check drill | Point `INGEST_HIVE_JDBC` at a bad host; run monitor | Exit 2 → PLATFORM destination (not the feed alert) |
| QA-I6 | P2 | Transient retry works and stops | With the second-run lock trick (QA-G2) or a firewall block, force exit 10 | Control-M reruns per policy (≤3, spaced); a persistent failure ends NOTOK after the retries |

## J. Production promotion gates (PROD only)

| ID | P | Title | Steps | Expected |
|---|---|---|---|---|
| QA-J1 | P1 | First light sequence | PROD_PROMOTION.md Phase 3, in order | validate-only → dry-run → FULL → reconcile, all green; counts plausible vs source |
| QA-J2 | P1 | TLS gate closed | Inspect prod feed + first run log | No `allow_insecure_tls`, no `trustServerCertificate` in config; no INSECURE warn in log; connection succeeds |
| QA-J3 | P1 | 364-column scale | The FULL initial load with `INGEST_DRIVER_MEMORY=4g` | Completes without driver OOM; plan-build time acceptable |
| QA-J4 | P2 | Cluster mode once | One INCR via `SMARTIQ_DEPLOY_MODE=cluster` | Completes; `[Outcome]` line present in `yarn logs`; ledger authoritative |
| QA-J5 | P2 | Week-one calibration | After 5–7 scheduled runs | `min_accepted_rows` decided from history; freshness threshold confirmed against real rhythm |

---

## Traceability to automation

The automated suite already pins: merge/dedup/freshness arithmetic,
null-key policies, all `CFG_*`/`CUR_*` validation, watermark
continuity/versioning, reconcile check logic (including the
edit-is-not-loss and null-key false-positive cases), override semantics,
failure classification, lock takeover, and the shipped configs validating.
A QA pass therefore treats any *logic* discrepancy found manually as a
missing automated test — file it against the framework, not just the run.
