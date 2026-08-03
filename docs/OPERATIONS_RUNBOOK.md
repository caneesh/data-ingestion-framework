# Operations Runbook

Production support runbook for the Data Ingestion Framework. Every behavior
below is grounded in the current code — file references are given so an
on-call engineer can verify before acting.

Companion docs: [ARCHITECTURE.md](ARCHITECTURE.md) (how the pieces fit),
[DEPLOYMENT.md](DEPLOYMENT.md) (how it is deployed).

> **Golden rule:** the safest first move for almost any header/mapping issue is
> a read-only `--validate-only --explain-mapping` run. It writes nothing and
> moves no files. See [Validate-only workflow](#validate-only--explain-mapping-workflow).

---

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

## Decoupled Raw / Curated operation

See docs/DECOUPLING_DESIGN.md for the full design. Quick reference:

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
