# Operations Runbook

Production support runbook for the Data Ingestion Framework. Every behavior
below is grounded in the current code — file references are given so an
on-call engineer can verify before acting.

Companion docs: [../architecture/ARCHITECTURE.md](../architecture/ARCHITECTURE.md) (how the pieces fit),
[DEPLOYMENT.md](DEPLOYMENT.md) (how it is deployed).

> **Golden rule:** the safest first move for almost any header/mapping issue is
> a read-only `--validate-only --explain-mapping` run. It writes nothing and
> moves no files. See [Validate-only workflow](#validate-only--explain-mapping-workflow).

---

## Self-healing: what recovers itself, and what must not

Much of the framework already recovers without intervention: the watermark
advances only on success (a failed run re-reads its window), the run-id and
window guards make a rerun idempotent, failed curated batches stay PENDING
for the next `--pending` drain, `record_hash` makes a re-publish a no-op,
and a crashed run's lock is now taken over rather than waited out. A
scheduler rerun of a failed job has always been safe.

### Exit codes: is retrying worth it?

| Exit | Class | Scheduler action |
|---|---|---|
| `0` | success (including an empty no-op) | — |
| `10` | `TRANSIENT` — connection reset, YARN preemption, lock contention, watermark conflict | retry with backoff |
| `20` | `DATA_INTEGRITY` — reconciliation, contract, curated integrity | **never retry**; alert |
| `30` | `CONFIGURATION` — `CFG_*`, missing file, bad credential | **never retry**; alert |
| `1` | unclassified | retry once, then alert |

Classification is deliberately conservative: anything not confidently
identified stays `1`, exactly as before. A missed `TRANSIENT` costs one
manual rerun; a wrong one would make a scheduler retry a data-integrity
failure forever and call it self-healing.

**The exit code only propagates in CLIENT mode.** In YARN cluster mode
`spark-submit` reports the final application state, not the driver's exit
code. Two mode-independent channels carry the same verdict:

```
[Outcome] class=TRANSIENT exitCode=10 retryable=true entity=... runId=... stage=...
```

and the notification payload, whose message is prefixed `[TRANSIENT]`,
`[DATA_INTEGRITY]` and so on. For a cluster-mode Control-M job, branch on
the log marker or the alert rather than the code.

### Stale lock takeover

A holder that stops renewing for `concurrency.stale_heartbeat_intervals`
(default 3) consecutive heartbeats is treated as crashed and its lease is
taken over, instead of blocking the entity for the remaining lease. A live
holder is still beating and cannot be stolen from, which is what makes this
safe. Set the value to `0` to restore lease-expiry-only behaviour.

### What deliberately does NOT self-heal

Reconciliation mismatches, contract violations, watermark continuity and
reject-threshold breaches all mean *the data is not what was expected*.
Retrying reproduces them; suppressing them removes the guarantees the
framework exists to provide. These page a human by design.


## Resetting a watermark (and the continuity failure that follows)

Clearing a watermark to re-pull history is a legitimate operation, but the
NEXT run then fails reconciliation by construction:

```
Reconciliation failed: watermark_continuity:
  expected=2026-05-01 20:34:20.0  actual=1900-01-01 00:00:00
```

The run started at `incremental.initial_value` while the ledger still
records where the previous window ended. The two stores disagree about
history — which is exactly what the check exists to detect, and exactly
what a manual reset creates. **The data is intact.** The message now says
so, naming `initial_value` as the reason.

Two ways through:

1. **Accept it once.** Set `audit.reconciliation.on_mismatch = "WARN"` in
   the feed for that single run, then put it back to `FAIL`. This is a feed
   config value, not a Spark `--conf`.
2. **Do not reset at all.** To see real rows flow, change a source row
   instead — the watermark then advances normally and continuity holds:

   ```sql
   UPDATE dbo.<table> SET <watermark_column> = SYSUTCDATETIME() WHERE <key> = '<value>';
   ```

Option 2 is preferable whenever the goal is to exercise the pipeline rather
than to genuinely re-load history.


## Driver OutOfMemoryError on a wide feed

Symptom: `java.lang.OutOfMemoryError: Java heap space` in DRIVER threads
(`driver-heartbeater`, `BlockManagerMaster`, netty RPC) with no reference to
the pipeline at all — it reads like a cluster fault rather than a sizing
one. It happens during ANALYSIS, before a single row is read.

The framework builds one projection per stage regardless of width (a
per-column fold was removed for exactly this reason), but a 364-column feed
still produces a large analyzer plan, and in **cluster mode the driver is a
YARN container at the queue default** — routinely smaller than an edge-node
shell. A feed that succeeds in client mode can therefore fail in cluster
mode with no other change.

```bash
# scripts/smartiq.env
INGEST_DRIVER_MEMORY=4g
```

Confirm it applied: the submit line contains `--driver-memory 4g`, and the
Spark UI Environment tab shows `spark.driver.memory`.

`run_smartiq.sh` warns when the 364-column feed runs without it.

### Settings added to the template after your file was created

`scripts/smartiq.env` is deliberately never overwritten — it holds site
values — so a key ADDED to `smartiq.env.example` later is invisible. That
is how `INGEST_DRIVER_MEMORY` went missing. `sync_artifacts.sh` now reports
the difference:

```
  settings in the template but NOT in smartiq.env:
    INGEST_DRIVER_MEMORY
  (this file is never overwritten — add anything you need by hand)
```


## Failure alerting

The framework detects failures precisely and, until a `notifications` block
exists, tells nobody — a failed Control-M job is visible only to whoever
queries the ledger. Configure one sink and every terminal failure (stage
failure, reconciliation mismatch, lock contention) reaches it:

```hocon
notifications {
  on      = ["FAILURE"]
  webhook { url = "https://hooks.example.com/ingest" }
  # command = "/opt/ingest/notify.sh"   # argv: outcome entity run_id stage message
}
```

Guarantees worth knowing:

- **Delivery never changes a run's outcome.** Every sink is best-effort; an
  unreachable webhook, an HTTP 500, a malformed URL or a hanging command is
  logged and swallowed. A pipeline must not fail because alerting failed,
  nor hide a data failure behind a delivery failure.
- **Messages are redacted** through the same sanitizer as the audit ledger:
  driver exceptions embed JDBC URLs, which can carry credentials, and a
  webhook reaches a wider audience than a log file.
- **SUCCESS is opt-in.** Alerting on every successful run of every feed is
  how alerting gets muted wholesale.
- The command sink receives the event as **arguments, never a shell
  string**, so metacharacters in source exception text cannot execute.


## PIPE_001: is the lock real, or did the holder crash?

A held lease and an abandoned one look identical if you only read
`lease_until` — both sit in the future. The distinguishing signal is the
**heartbeat**: a live run re-CLAIMs every `lease_minutes / 3`, so its last
row keeps moving; a crashed run's rows stop.

The error states the verdict directly:

```
PIPE_001 entity 'smartiq_pdp' is locked by run 'pdp-1'
(lease until 2026-08-11 15:18:53; last heartbeat 2m ago (renews every ~80m)
 — the holder appears ALIVE; wait for it)
```

Confirm it yourself — the trail is in the lock table:

```sql
SELECT holder_run_id, action, lease_until, event_ts
  FROM membership_common_raw.ingest_run_locks
 WHERE entity = '<entity>' ORDER BY event_ts DESC LIMIT 10;
```

Repeated CLAIM rows one interval apart mean a healthy run. If the newest is
older than two intervals, the holder died.

**Only then**, release it:

```sql
INSERT INTO membership_common_raw.ingest_run_locks
VALUES ('<entity>', '<holder_run_id>', 'RELEASE', CAST(NULL AS TIMESTAMP), current_timestamp());
```

### Size the lease for detection, not for run length

The default `lease_minutes = 240` predates the heartbeat and is the wrong
shape now: with an active heartbeat the lease need only outlive a transient
renewal hiccup, because a healthy run extends it indefinitely. What the
lease actually controls is **how long a crashed run blocks the entity**.

```hocon
concurrency { lease_minutes = 30 }   # heartbeat every 10m
```

A crashed run then self-clears in 30 minutes instead of 4 hours, and the
alive/dead verdict is decisive within ~20 minutes instead of ~160. Long
runs are unaffected — the heartbeat keeps renewing.

Do not shorten it so far that a normal metastore stall misses a renewal:
the heartbeat abandons ownership on the FIRST failure and the run aborts.
10–30 minutes is a reasonable band.

### Changing a setting mid-incident (`INGEST_OVERRIDE_FILE`)

A feed config sits behind change control; an incident does not wait for one.
Point `INGEST_OVERRIDE_FILE` at a small HOCON file and its values win over
the feed for every path it declares:

```hocon
# /opt/ingest/conf/override-smartiq-pdp.conf
feeds.smartiq_pdp.concurrency.lease_minutes = 120
```

```bash
# in the env file, or exported for a single run
INGEST_OVERRIDE_FILE=/opt/ingest/conf/override-smartiq-pdp.conf
```

Objects merge, so the line above leaves the rest of `concurrency` alone.
Lists are replaced wholesale. A path named but missing on disk **fails the
run (CFG_019)** — an override that silently did not apply would be the worst
outcome, since the run would look normal while behaving under the settings
you meant to change.

It is deliberately loud. `run_smartiq.sh` lists the overridden paths before
submitting, the driver logs each one under `[Override]` (safety-reducing
paths — `on_mismatch`, `max_reject_percent`, `allow_insecure_tls`,
`audit.enabled`, `concurrency.lock` — get an extra WARNING), and the ledger's
`config_fingerprint` gains an `+ovr:<digest>` suffix so an overridden run is
identifiable afterwards:

```sql
SELECT run_id, stage, status, config_fingerprint
FROM   membership_common_raw.ingest_run_audit
WHERE  entity = 'smartiq_pdp' AND config_fingerprint LIKE '%+ovr:%'
ORDER  BY event_ts DESC;
```

Values are never logged or stored — only path names and a digest — because
an override may carry a credential. Template:
`docs/examples/smartiq_pdp/override-smartiq-pdp.conf.example`.

**Delete it once the real change is deployed.** It is unversioned by design.

### Avoid creating stale locks

Losing the terminal kills the driver before it can release. Detach anything
long-running:

```bash
nohup scripts/run_smartiq.sh prod INCR --run-id pdp-initial-1 > pdp-initial-1.log 2>&1 &
```


## 1. Error catalog with operator actions

### 1.1 HDR_001 – HDR_018 (schema / header validation)

All HDR codes are defined in `ViolationKind`
(`ingestion-core/.../schema/SchemaValidator.scala`). Codes marked **FAIL
(fixed)** are non-negotiable data-safety failures and ignore drift policy;
the rest honor a configurable policy (`FAIL | WARN | IGNORE`) in the feed's
`schema.header_validation` block.

| Code | Meaning | Policy knob (default) | Operator action |
|------|---------|-----------------------|-----------------|
| HDR_001 | Required canonical header not found | `on_missing_required` (FAIL) | Vendor renamed/dropped a required column. Run `--validate-only --explain-mapping`; confirm the change with the source system; obtain approval; add the approved alias under `schema.columns[].aliases`; re-validate; move the quarantined file back to landing and re-run. |
| HDR_002 | Duplicate physical header (same string twice in the file) | `on_duplicate_columns` (FAIL) | Vendor extract bug. Get the source system to fix the extract; do not hand-edit files. |
| HDR_003 | Duplicate normalized header (distinct headers collapse to the same normalized name) | `on_duplicate_columns` (FAIL) | Two headers differ only by case/whitespace/punctuation. Fix the vendor extract or add an alias only if one is genuinely a different canonical column. |
| HDR_004 | Ambiguous alias mapping | FAIL (fixed) | A configuration bug — an alias matches more than one canonical column. Fix the contract; do not retry the file. |
| HDR_005 | Multiple source columns map to one canonical column | FAIL (fixed) | Two source headers both alias to the same canonical name. Remove the wrong alias from the contract. |
| HDR_006 | Unexpected (extra) header not in the contract | `on_extra_columns` (WARN) | Usually benign. If the vendor added a real column you need, add it to `schema.columns`. If it should hard-fail, set `on_extra_columns = "FAIL"`. |
| HDR_007 | Column-count mismatch | FAIL (fixed) | Under positional mapping, actual column count differs from the contract. Confirm delimiter and file layout; often a delimiter problem (see HDR_009). |
| HDR_008 | Invalid positional mapping | FAIL (fixed) | Positional fallback is enabled but not every contract column declares a `position`. Fix the contract. |
| HDR_009 | Suspected delimiter mismatch | FAIL (fixed) | Configured delimiter likely wrong for this file (one giant header, etc.). Check the feed's delimiter against the actual file. |
| HDR_010 | Blank / empty header field | FAIL (fixed) | Structural file corruption. Reject; ask the vendor to re-send. |
| HDR_011 | Malformed quoted header | FAIL (fixed) | Unbalanced quotes in the header line. Vendor extract bug; re-send. Note: multi-line quoted header fields are treated as malformed (known limitation). |
| HDR_012 | Unsupported encoding | FAIL (fixed) | File encoding not readable under the configured charset. Confirm the vendor's encoding; re-send in the expected charset. |
| HDR_013 | Header-only file (header row(s), zero data rows) | `header_only_policy` (WARN_AND_SKIP) | By default the file is skipped with a warning. Set `header_only_policy = "FAIL"` if an empty delivery must page. |
| HDR_014 | Schema-version mismatch (stored table version ≠ contract version) | `on_version_mismatch` (WARN) | A contract version bump landed. Confirm the version change is intended; the RAW table's `ingest.schema.version` property will be reconciled. Set to FAIL to gate on unexpected drift. |
| HDR_015 | Content validation failure (value fails regex / length / allowed_values / nonblank / typed rule beyond the failure-percentage threshold) | FAIL (fixed) | Inspect the failing column's values against the configured content rules. A column swap usually shows ~100% failure — that is the signal. Fix at source or correct the mapping. |
| HDR_016 | Repeated header row detected inside data | `repeated_header_policy` (FAIL) | A header line reappears mid-file. Options: `FAIL`, `REJECT_ROW`, `DROP_WITH_WARNING`. Choose per feed tolerance; default fails. |
| HDR_017 | Contract configuration collision (duplicate names/aliases/positions, bad type/category/policy value) | FAIL (fixed), at startup | A bug in the feed's own schema config — the error message names the colliding items. This fails fast at startup, not per file. Fix `application.conf`. |
| HDR_018 | Required output column missing before CURATED publish | FAIL (fixed) | A required output column is absent from the curated DataFrame before persist. A transform/config regression; do not publish. Review the curated column list and derivations. |

Where a code is emitted with a FAIL policy and `quarantine_on_failure = true`,
the file is moved to `quarantine` **after** the audit row is written (see
[Quarantine recovery](#3-quarantine-recovery)).

### 1.2 JDBC_001 – JDBC_005

Defined across `ingestion-jdbc/.../health/JdbcHealthCheck.scala`,
`auth/SecretProvider.scala`, `watermark/WatermarkStore.scala`, and
`watermark/Watermarks.scala`.

| Code | Meaning | Thrown as | Retry? | Operator action |
|------|---------|-----------|--------|-----------------|
| JDBC_001 | Connection failure: driver class not on classpath, or database unreachable, or a driver-side op (health check / metadata / upper-watermark capture / store op) exhausted retries on a transient error | `IllegalStateException` (health check) / `RuntimeException` (retry exhaustion) | Transient categories only, per `RetryPolicy` (bounded exponential backoff + jitter; auth/syntax/missing-object fail immediately) | If class-not-found: the JDBC driver is missing on the driver **or** executors — deploy it to both (see [DEPLOYMENT.md](DEPLOYMENT.md#jdbc-driver-deployment)). If connectivity: check firewall/DNS/credentials; remember the driver health check only proves the *driver's* path — executors open their own connections. Then re-run. |
| JDBC_002 | Authentication / secret resolution failure: bad SQL credentials, missing env/file/sysprop secret, or CyberArk CCP error | `RuntimeException` | No (permanent) | Verify the secret provider config and that the referenced secret exists (env var set, file present, CyberArk object/attribute correct). Fix and re-run. |
| JDBC_003 | Invalid configuration: e.g. watermark store database/table name is not a safe SQL identifier, or an unsupported `auth.type` for a non-sqlserver dialect, or mutually-exclusive partition options | `IllegalArgumentException` | No | A config bug. Correct `application.conf`; the message names the offending option. |
| JDBC_004 | Invalid watermark value: stored value fails the typed round-trip (wrong column count, unparseable numeric/date/timestamp/datetimeoffset/rowversion, null in a composite component) | `IllegalArgumentException` | No | The stored watermark is corrupt and is never allowed to become SQL. Repair it with an unconditional `record()` (see [Watermark operations](#2-watermark-operations)); the next run reads from the repaired value. |
| JDBC_005 | Watermark version conflict: a concurrent run advanced the same entity between this run's read and commit (optimistic version check failed) | `WatermarkConflictException` (message contains `JDBC_005`) | No — the losing run fails by design | Nothing was published by the losing run and its watermark did **not** advance. Simply re-run the feed; it re-extracts from the newly committed watermark. See [JDBC_005 recovery](#24-jdbc_005-conflict-recovery). |

Retry knobs (`retry` block): `max_attempts` (default 3), `backoff_ms`
(default base; exponential with jitter, capped per sleep). Only transient
categories classified by `SqlFailureClassifier` are retried.

---

### source_keys_present_in_curated — rows the source has and curated does not

Raised by `--stage reconcile`. This is the one finding that no in-run check
can produce: every run reconciled correctly, and curated is still missing
rows. It means data was lost **outside** a run.

Identify them first — the check reports a count, not the keys:

```sql
-- keys in the source key pull that have no curated row; run the same
-- projection the stage uses, then anti-join
SELECT s.<key> FROM <source_keys> s
LEFT ANTI JOIN <curated_db>.<table> c ON s.<key> = c.<key>;
```

Then work through the causes, most likely first:

1. **A run that never happened.** Check the ledger for a gap in
   `event_ts` — the framework cannot detect its own absence, so a missed
   schedule leaves no trace except missing data.
2. **A dropped or purged partition.** Check whether retention ran with a
   misconfigured period; RAW partition drops are irreversible.
3. **A watermark that skipped a window.** `watermark_continuity` would
   normally catch this, but an out-of-band watermark edit defeats it.
4. **Rows filtered out deliberately.** Confirm `source.where` and any
   contract-level filtering — the comparison applies the same filters, so
   this should not produce false positives, but a filter changed since the
   original load will.

**Recovery** is a replay bounded to the affected window, not a full
reload: `--stage curated --replay-from <date> --replay-to <date>` when the
RAW rows exist, or a watermark rewind plus re-extract when they do not
(see §2.3). Re-running `--stage reconcile` afterwards is the proof.

### curated_keys_absent_from_source — informational, but read it

Always passes; the number is the point. Under `deletes.mode = IGNORE`
these are rows deleted upstream and deliberately retained. A **non-zero
count on a feed that assumes no source deletes occur** means the
assumption is wrong — revisit the delete policy with the source team
before a consumer asks why a deleted record is still in the warehouse.

### accepted_meets_minimum — the extract came back empty

The feed declared `audit.reconciliation.min_accepted_rows` and this run
accepted fewer. The run failed *before* anyone downstream saw a stale
table, which is the point.

This is **not** a data-integrity failure — nothing was corrupted, nothing
was lost. It means the extract returned less than this feed considers
possible. Check, in order:

1. **Is the source reachable and populated?** Run the previewed extraction
   SQL by hand against the source.
2. **Has the watermark overrun the data?** Compare the stored watermark to
   `MAX(<watermark column>)` at source — a watermark ahead of the data
   selects nothing, forever.
3. **Did the source change underneath the feed?** A renamed table, a
   changed filter column, or a migration that reset timestamps.
4. **Was it genuinely quiet?** If zero is legitimate for this window
   (a weekend, a holiday), the floor is set too high — lower it or remove
   it rather than muting reconciliation wholesale.

Re-running does not help until the cause is fixed: the same window will
return the same nothing. The watermark did **not** advance, so no data is
skipped once the cause is resolved.

## 2. Watermark operations

Incremental feeds track their position in an **append-only** history table.
The production store is `HiveWatermarkStore`
(`ingestion-jdbc/.../watermark/WatermarkStore.scala`); tests use
`InMemoryWatermarkStore`.

- **Table:** `ingest_watermarks` by default (override with
  `incremental.watermark_store.table`), in `incremental.watermark_store.database`.
- **Columns:** `entity`, `watermark_value`, `run_id`, `watermark_version`
  (BIGINT, monotonically increasing), `updated_ts`.
- **Semantics:** advance-only (never regresses) and append-only (full history
  retained). The current position is the row with the highest
  `watermark_version` for the entity.
- **Advance timing:** the watermark is committed **only after RAW write and
  CURATED publish succeed**, via the `WatermarkAdvancing.advanceWatermark`
  hook invoked from `IngestPipeline` after publish.

### 2.1 Inspect watermark history

```sql
-- Current position for an entity:
SELECT entity, watermark_value, watermark_version, run_id, updated_ts
FROM   <watermark_db>.ingest_watermarks
WHERE  entity = 'my_feed'
ORDER  BY watermark_version DESC
LIMIT  10;
```

The top row (highest `watermark_version`) is what the next run reads from.
Older rows are the audit trail — do not delete them.

### 2.2 Manual repair (unconditional record)

Use when the stored value is corrupt (JDBC_004) or must be re-anchored (e.g.
after a data fix at source). The `record(entity, value, runId)` method appends
a new highest-version row **without** the optimistic version check, so it
always wins — reserve it for deliberate operator action.

Practical repair via a one-off driver-side call (or a controlled admin job)
that invokes `store.record("my_feed", WatermarkValue(Seq("<safe-value>")),
"manual-repair-<ticket>")`. The value must be type-valid for the configured
`watermark_type` (TIMESTAMP / NUMERIC / DATE / DATETIMEOFFSET / ROWVERSION /
COMPOSITE), or the next read fails again with JDBC_004.

Because the store is advance-only on the *automatic* path but `record()` is
unconditional, `record()` is the only supported way to move a watermark
**backwards** for a deliberate replay.

### 2.3 Replay from an older watermark

To reprocess a window that was already committed:

1. Identify the older `watermark_value` you want to replay from (query §2.1).
2. Use the manual repair path (§2.2) to append that older value as the new
   current position.
3. Re-run the feed in `INCR` mode. The bounded read re-extracts from the
   re-anchored lower edge up to the freshly captured upper. RAW is
   at-least-once and the curated merge deduplicates, so replay is safe.

Prefer this over editing history rows directly — the append keeps the audit
trail intact and respects the version counter.

### 2.4 JDBC_005 conflict recovery

Two runs advanced the same entity concurrently. The commit performs a
compare-and-set against the version observed at read time; the losing run
throws `WatermarkConflictException` (message contains `JDBC_005`) and fails
**before** its watermark advances. Guarantees:

- The losing run published nothing that the winning run didn't already cover,
  and its watermark did not move.
- Recovery is simply **re-run the losing feed**. It reads from the version the
  winner committed and extracts the next window.

If conflicts are frequent, you are running the same entity concurrently —
serialize the schedules, or plug in a transactional control-table
`WatermarkStore` (the Hive store's CAS is best-effort).

---

## 3. Quarantine recovery

### 3.1 Header-failure flow (audit-before-move)

When a file fails header/content validation under a FAIL policy with
`quarantine_on_failure = true`
(`IngestPipeline.handleContractFailure`, `files/FileIntakeService.scala`):

1. **Audit is written first.** `recordHeaderValidation` persists a full
   diagnostic row to `ingest_header_audit` (status `FAILED`, the `error_code`,
   `error_message`, expected canonical columns, actual/normalized headers,
   resolved mappings, missing/unexpected/duplicate breakdown, and the intended
   quarantine path).
2. **Then the file moves** to the `quarantine` folder.

This ordering is a hard invariant: if the audit write fails, the move is
skipped and the file stays in `inprogress` (restart-safe). If the move fails,
the audit row already records the quarantine intent. So the audit table is
always the authoritative record of *why* a file was rejected.

Folder lifecycle: `landing → inprogress → processed` (with optional `archive`
copy); `quarantine` is a terminal side branch — quarantined files are not
retried automatically.

### 3.2 Diagnose a quarantined file

```sql
SELECT source_files, error_code, error_message,
       expected_canonical_columns, actual_source_headers, quarantine_path, event_ts
FROM   <audit_db>.ingest_header_audit
WHERE  validation_status = 'FAILED'
ORDER  BY event_ts DESC;
```

Then run the read-only validation to see the proposed mapping (§4).

### 3.3 Move files back to landing and re-run

Once the root cause is fixed (usually an approved alias added to the
contract):

1. Confirm the fix with a `--validate-only --explain-mapping` run.
2. Move the quarantined file from the `quarantine` folder back to the feed's
   `landing` folder (HDFS `mv`/`fs -mv`, respecting the configured folder
   paths in `source.folders`).
3. Re-run the feed normally. Nothing was written to RAW or CURATED for a
   quarantined file, so no cleanup is required beyond the move.

### 3.4 --validate-only / --explain-mapping workflow

See [§4](#validate-only--explain-mapping-workflow).

---

## 4. Validate-only / --explain-mapping workflow

```bash
spark-submit ... --entity my_feed --mode FULL --validate-only --explain-mapping
```

Flags (`ingestion-core/.../model/Cli.scala`):

- `--validate-only`: discovers files, extracts and validates physical headers,
  resolves canonical mappings — with **no RAW write, no CURATED write, no file
  moves, and no audit writes** (the audit service is structurally disabled for
  the run, so even future code changes cannot write). Safe for production
  support.
- `--explain-mapping`: only meaningful with `--validate-only`; for each file
  that resolves, prints a human-readable `source header → canonical column`
  explanation.

Typical support loop:

1. `--validate-only --explain-mapping` to see what the framework would do.
2. Fix the contract (add approved alias, adjust policy).
3. Re-validate until the mapping resolves cleanly.
4. Move quarantined files back to landing (§3.3) and run for real.

---

## 5. Restart / resume

### 5.1 `--resume --run-id <id>` semantics

(`IngestPipeline`, `runStage`/`runRaw`/`resumeCuratedSlice`.)

- `--run-id <id>` pins the run identifier; it is also stamped on every RAW row
  as `run_id`. `--resume` **requires** `--run-id` (enforced by `CliParser`).
- On `--resume`, a stage already recorded `SUCCESS` in stage-level audit is
  skipped. **VALIDATE is never skipped** — file intake is idempotent, so it is
  always safe to re-run.
- If RAW already succeeded, curated **replays from the RAW `run_id` slice**
  (`resumeCuratedSlice` reads the RAW table filtered by `run_id`) instead of
  re-reading the source or re-appending to RAW.

### 5.2 Idempotency guarantees

- **RAW run_id guard:** before writing RAW, the pipeline checks whether the RAW
  table already holds rows for this `run_id`; if so it **skips the write**
  ("idempotent replay"). Re-running the same `run_id` cannot double-append.
  (Legacy tables without a `run_id` column cannot attribute rows and get no
  guard — add `run_id` to enable it.)
- **Watermark advance-after-publish:** the watermark commits only after RAW and
  CURATED succeed, so a failed publish means the next run re-extracts the same
  bounded window — at-least-once into RAW, deduplicated by the curated merge.
- **File intake:** files left in `inprogress` by a crashed run are re-staged
  automatically on the next run; the content checksum registry
  (`ingest_file_registry`) is written only after a fully successful run.

### 5.3 BATCH_ATOMIC stuck-batch recovery

With `header_validation.batch_policy = "BATCH_ATOMIC"`, one invalid file fails
the whole batch (default is `FILE_ATOMIC`, which quarantines invalid files
individually and continues). After a BATCH_ATOMIC failure:

- The bad file is quarantined; files staged earlier in the same run remain in
  `inprogress` (restart-safe by design — nothing was written to RAW/CURATED).
- **Recover:** fix or remove the cause (usually add an approved alias, or leave
  the bad file in quarantine), then re-run the feed — `inprogress` leftovers
  are re-staged automatically.
- **Abandon the batch:** move the `inprogress` files back to `landing`
  manually; nothing needs to be undone downstream.

---

## 6. Stale-state cleanup

### 6.1 Staging tables (24h age gate)

Transactional publish materializes curated data into a per-run staging table
named `__stg_<table>_<run_id>` in the target database
(`ingestion-core/.../publish/PublishService.scala`).

- On any publish failure the staging table is dropped and the target is
  untouched.
- On the **next** publish, `cleanupStaleStaging` drops leftover staging tables
  from crashed runs — **but only once they are older than 24 hours**
  (`StaleStagingAgeMillis = 24h`), using a creation-time TBLPROPERTY stamp so a
  concurrent run's *live* staging table is never dropped.
- **Manual cleanup** (only if you must reclaim space before the 24h gate and
  you are certain no run is live): drop tables matching
  `<database>.__stg_<table>_*` whose stamped creation time is old. Verify no
  active job owns them first.

### 6.2 Inprogress leftovers

Files left in `inprogress` by a crashed run are **not** stale state to clean —
they are re-staged automatically on the next run (they were audited `VALIDATED`
when first staged and are not re-audited). To deliberately abandon them, move
them back to `landing` (to reprocess) or to `quarantine`/`processed` per your
decision. Do not delete a file that was staged but whose RAW write status is
unknown without first checking the run audit and the `run_id` slice of RAW.

### 6.3 Audit / registry tables

`ingest_header_audit`, `ingest_file_audit`, `ingest_run_audit`,
`ingest_reconciliation`, `ingest_watermarks`, and `ingest_file_registry` are
append-only history. Do **not** truncate them as routine cleanup — they are the
forensic record for restart safety and reconciliation. Apply a retention policy
only with explicit approval and after confirming no in-flight run depends on
recent rows (watermarks and file registry especially).

`--stage retention` now holds its entity lease with the same heartbeat the
pipeline uses, and **aborts with PIPE_001 if a lease renewal has failed**
rather than completing the purge. That is deliberate: every purge is a
read-then-overwrite, so a run that lost the entity would discard whatever
the new holder appended in the meantime — silently, since the discarded
rows are the audit trail. An aborted attempt purges nothing; re-run it once
the entity is idle. A retention job that starts failing this way is
reporting a real lock problem, not a regression.

Note that `--stage retention` covers the audit tables, rejects, raw
partitions and watermarks but **not** `ingest_file_registry` — deliberately,
since trimming it re-enables re-ingestion of any file whose checksum is
dropped. The registry therefore grows for the life of the feed, and file
intake reads every checksum for the entity into driver memory on each run.
That is fine at daily cadence for years; if a feed ever makes it large,
raise driver memory rather than truncating the table.

## 7. Entity run locks (PIPE_001)

Every run (coupled, curated replay, retention) takes an entity-level lease
before writing and holds it through the watermark commit. Two providers,
selected by `concurrency.provider`:

| Provider | Guarantee | Use for |
|----------|-----------|---------|
| `HIVE` (default, `LockService`) | **Best-effort only.** Hive appends are not transactional; the claim → settle → re-read protocol shrinks the race window to the settle interval but does **not** eliminate it — two claims within one settle interval (plus driver clock skew) can both win. | Dev/test, and production where a single scheduler already serializes runs of one entity. |
| `JDBC` (`JdbcRunLock`) | **Atomic.** Single-statement UPDATE compare-and-set on a relational control table (`ingestion_lock`, one row per entity); of N concurrent acquirers exactly one wins. | Production with real concurrent submission. |

```hocon
concurrency {
  lock     = REQUIRED            # or OFF (loud warning, no protection)
  provider = JDBC                # default HIVE
  jdbc {
    url           = "jdbc:sqlserver://..."   # required
    user          = "..."                    # optional
    password      = "..."                    # optional; never logged
    driver        = "..."                    # optional driver class
    table         = "ingestion_lock"         # default
    lease_minutes = 240                      # default 4h
  }
}
```

Operational facts:

- **Heartbeat:** after acquiring, the pipeline renews the lease from a daemon
  thread every `lease/3` (default: every 80 min for the 4 h lease), so a
  healthy run can miss two beats before its lease lapses. There are no
  stage-boundary renewals anymore — the heartbeat replaced them.
- **Ownership loss aborts before damage:** if any renewal fails, the run does
  not stop mid-stage; it aborts with
  `PIPE_001 lease ownership lost mid-run; aborting before publish/watermark`
  at the next danger point — before the curated publish and before the
  watermark advance. Nothing was published by the aborted run at that point;
  re-run it once the competing holder finishes.
- **Crashed holder:** the lease expires after `lease_minutes`; a new run then
  takes over automatically. To free it earlier: HIVE provider — use
  `forceRelease` / append a RELEASE row; JDBC provider — `UPDATE
  ingestion_lock SET holder_run_id = NULL WHERE entity_name = '<entity>'`.
- **JDBC clock caveat:** lease-expiry comparisons run on the database's
  `CURRENT_TIMESTAMP` (one clock for all competitors), but the stored expiry
  is computed from the acquiring JVM's clock — JVM↔DB skew stretches or
  shrinks the effective lease by the skew. Keep clocks NTP-synced.
- **SQL Server:** pre-create `ingestion_lock` with `DATETIME2` columns; the
  auto-create DDL is ANSI (H2/dev) and SQL Server's `TIMESTAMP` type is a
  rowversion, not a datetime.

## Decoupled Raw / Curated operation

See ../architecture/DECOUPLING_DESIGN.md for the full design. Quick reference:

- Raw job: `--stage raw` + feed `watermark { advance_after = RAW }`
  (required in decoupled operation — without it the window re-reads forever).
- Curated job: `--stage curated --pending` (or `--replay-failed`,
  `--replay-last N`, `--replay-from/--replay-to`, `--replay-source-system X`).
- Batch status: query the `ingest_batch_control` view
  (ddl/ingest_batch_control_view.sql) — `curated_done` is the checkpoint
  truth; `retry_count` counts FAILED events.
- Failures raise PIPE_005 and leave failed batches pending; PIPE_006 =
  driver preconditions (ledger + enabled curated); PIPE_007 =
  --replay-source-system without a source_system RAW column.

### Control-M scheduling: one process or two

Declare the choice per feed — CFG_016 validates consistency and the entry
point refuses mismatched invocations (so the scheduler cannot silently
double-process a feed):

- **COUPLED** (default): `ingestion.execution = COUPLED` (or absent).
  One Control-M job → `scripts/run_ingest.sh <conf> <entity> [FULL|INCR]`.
  Raw then curated in one process; watermark advances after curated.
- **DECOUPLED**: `ingestion.execution = DECOUPLED` +
  `watermark { advance_after = RAW }` (required for incremental sources).
  Two independent Control-M jobs:
  1. `scripts/run_raw.sh <conf> <entity> [FULL|INCR]` — extract, land RAW,
     advance the watermark. Schedule as often as the source needs.
  2. `scripts/run_curated_pending.sh <conf> <entity>` — drain pending
     batches to curated. Schedule independently (no dependency required;
     a dependency on job 1 is also fine).
  A DECOUPLED feed REJECTS `--stage all` (CFG_016) — batches would be
  double-processed by the scheduled curated job.

Exit codes for Control-M: 0 = success, including a clean "nothing pending"
no-op; non-zero = failure. Failed curated batches stay PENDING, so
re-running the same job after a failure resumes exactly where it stopped —
both jobs are safe to rerun unconditionally.

### Control-M folder design: INGESTION vs AUDIT

Two SMART folders, split by what the jobs are FOR rather than what they
touch. The framework's entity lock serializes any accidental overlap, so
the folders need no cross-dependencies — only calendars that keep them out
of each other's way on purpose.

**Run scheduled jobs in CLIENT mode** (`SMARTIQ_DEPLOY_MODE=client` in the
env file, `INGEST_DRIVER_MEMORY=4g` for the wide feed — the driver now
builds its plan on the agent host). The classified exit codes below only
propagate in client mode; in cluster mode spark-submit reports YARN
application state and every failure reads as 1.

#### Folder 1 — `SMARTIQ_PDP_INGESTION` (the SLA path)

| Job | Command | Schedule |
|---|---|---|
| `SMARTIQ_PDP_INGEST` | `run_smartiq.sh prod INCR --resume --run-id pdp_%%ORDERID` | the source's rhythm |

Two deliberate choices in that command line:

- **`--run-id pdp_%%ORDERID`** ties every ledger, RAW and reject row to
  the exact scheduler execution.
- **`--resume` always on.** First attempt: no-op (nothing to skip).
  Control-M rerun (same order id, same run id): skips completed stages and
  continues from the failure. Without it, a rerun with the same run id
  hits the RAW idempotency guard and produces the confusing
  `raw_equals_accepted` mismatch documented above. One command line,
  correct in both cases.

Folder-level ON rules — this is where the exit codes earn their keep:

| Exit | Action |
|---|---|
| 0 | OK |
| 10 (transient) | rerun, up to 3 times, 10–15 min interval |
| 20 (data integrity) | **no rerun** — retrying reproduces it exactly; page on-call |
| 30 (configuration) | **no rerun** — something must be edited; notify the feed owner |
| 1 (unclassified) | no rerun, notify — unknown is not transient |

Never add a rerun on 20: that is the retry-a-data-failure-forever
anti-pattern the classification exists to prevent. YARN blind retries are
already off (`spark.yarn.maxAppAttempts=1` in the wrapper); Control-M is
the only retry authority. A job Control-M kills leaves a lock whose
heartbeat died with it — the lease self-clears within ~`lease_minutes`,
and an immediate rerun that hits PIPE_001 exits 10, which the retry rule
absorbs.

#### Folder 2 — `SMARTIQ_PDP_AUDIT` (detection and governance, own calendar)

| Job | Command | Schedule | Needs SQL credential? |
|---|---|---|---|
| `SMARTIQ_PDP_RECONCILE` | `run_smartiq.sh prod INCR --stage reconcile` | nightly, off-peak | **yes** (queries the source) |
| `SMARTIQ_PDP_RETENTION` | `run_smartiq.sh prod INCR --stage retention` | weekly, quiet window | no — Hive only |
| `SMARTIQ_PDP_FRESHNESS` | `check_freshness.sh smartiq_pdp 26` | cyclic, every 4h | no — Hive only |

The credential asymmetry is worth preserving in the folder design: only
the ingestion folder and the reconcile job carry `SMARTIQ_DB_PASSWORD`.
Retention and freshness run with no database credential at all — smaller
blast radius, simpler variable management.

Failure routing differs from Folder 1 by intent: these jobs DETECT, they
do not deliver. Reconcile under the default `on_mismatch = "REPORT"` exits
0 even with findings (they alert through the notification webhook and land
in `ingest_reconciliation`); a red nightly comparison job gets silenced
rather than read. Retention failing with PIPE_001 is the ownership guard
working, not a regression — it purged nothing; treat as exit 10 and rerun
in a quiet window.

The freshness job closes the one gap no framework job can: **a run that
never happens writes nothing** — no ledger row, no alert. It must live
outside the pipeline:

```bash
INGEST_HIVE_JDBC="jdbc:hive2://<host>:10000/default" \
  scripts/check_freshness.sh smartiq_pdp 26
```

Exit 0 fresh, **1 stale** (alert — including the never-succeeded case),
**2 the check itself broke** (Hive unreachable). The 1-vs-2 split is
deliberate: "the feed is stale" and "I could not tell" must not share an
alert, or a Hive outage pages the feed team. Tune the hours to the feed's
SLA; it must exceed the longest legitimate gap between successful loads.

#### Keeping the folders apart

No hard dependency is REQUIRED — the entity lock is authoritative. But a
reconcile retrying through a load window is noise, so either separate the
calendars (ingest per source rhythm; reconcile 02:00; retention Sunday
03:00; freshness cyclic) or, if schedules may drift together, add a
Control-M quantitative resource (e.g. `SMARTIQ_PDP_ENTITY`, quantity 1)
held by every job in both folders. That is a courtesy mutex at the
scheduler layer; the framework lock remains the real one.

If the feed later becomes DECOUPLED, only Folder 1 changes shape: it gains
the `run_raw.sh` / `run_curated_pending.sh` pair described above, and the
audit folder is untouched.
