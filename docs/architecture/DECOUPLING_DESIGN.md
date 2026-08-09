# Decoupled Raw / Curated Operation

Raw ingestion and Curated processing run as independent jobs, coordinated
only through the RAW tables and the run ledger. Composable pieces — no
mode flag; the coupled single-pipeline default is unchanged.

## The two jobs

**Raw job** (schedule freely): `--entity E --mode INCR --stage raw` with
`watermark { advance_after = RAW }`. Extracts the bounded window, lands
RAW with full lineage (run_id = the batch id, source_system, extract
window), records raw SUCCESS in the ledger, advances the extraction
watermark. Never touches curated. Safe because RAW is the durable landing
zone — curated replays from it, so a curated failure can never lose data.

**Curated job** (schedule independently): `--entity E --stage curated
--pending`. Selects RAW batches from the ledger, merges each through the
governed replay (lock, ledger, raw-SUCCESS guard, rejects, staged
publish), never reads the source, never touches the watermark.

## Dual checkpoints

1. **Extraction position**: JDBC watermark store / Kafka offset ledger /
   file registry — advanced by the raw job at raw success.
2. **Curated progress**: the ledger itself. `curatedReplay --run-id RID`
   reuses RID as its run context, so its curated SUCCESS row lands under
   the RAW run's id. *A batch is curated-done iff the ledger holds
   EXISTS(stage='curated', status='SUCCESS') for its run_id.* EXISTS, not
   latest-event: SUCCESS is a monotone fact — later SKIPPED rows (resume)
   or FAILED replay attempts never un-checkpoint a batch. Dry-run raw
   SUCCESS rows (message='dry-run') write no data and are never selected.

## Selectors (`--stage curated` only; mutually exclusive)

| Flag | Selects |
|---|---|
| `--pending` | raw-SUCCESS batches with no curated SUCCESS (the normal driver) |
| `--replay-failed` | pending batches with ≥1 curated FAILED attempt |
| `--replay-last N` | last N raw-SUCCESS batches, regardless of curated state (forced rebuild; content-idempotent under the freshness merge) |
| `--replay-from D` / `--replay-to D` | raw-SUCCESS date range (event_ts, yyyy-MM-dd) |
| `--replay-source-system X` | filter (composable, or standalone = pending for X); needs the RAW source_system column (PIPE_007) |

Batches process in ascending raw-SUCCESS event_ts (extraction commit
order — the entity lock serializes raw runs). Each batch replays under the
run_mode its raw run recorded, per batch, never unioned: the per-run
curated SUCCESS row IS the checkpoint. `curated.pending { max_batches = 0,
on_failure = STOP | CONTINUE }`; failures raise PIPE_005 and the failed
batches stay pending. Preconditions (PIPE_006): ledger enabled, curated
configured + enabled.

## Batch control surface

`BatchControl.batchControl(entity)` (and `ddl/ingest_batch_control_view.sql`)
project the ledger into one row per batch: batch_id (= run_id), run_mode,
extract window, `raw_status`/`curated_status` (latest-terminal, display),
`curated_done` (EXISTS — checkpoint truth; the two differ after a failed
retry of a done batch), counts, `retry_count` (FAILED events), first/last
event_ts. Read-only: the ledger stays the single source of truth — there
is deliberately NO physical batch table (dual writes would drift), and no
run_id→batch_id rename (view alias only).

## Recovery matrix

| State | Action |
|---|---|
| Raw success, curated failed/never ran | `--pending` (or `--replay-failed`) — nothing manual |
| Raw failed | re-run the raw job; watermark never advanced |
| Crash mid-driver | rerun `--pending`: checkpointed batches skip, rest resume |
| Suspected bad curated content | `--replay-last N` / date range — idempotent rebuild |

File-feed nuance: files register PROCESSED after RAW (correct — curated
replays from the RAW table); the batch-control view is the
curated-visibility signal. `--resume-ingest-dt` partition replays run
under fresh run_ids and do not clear pending. Decoupled feeds SHOULD set
`merge.freshness` so replay order and duplicates can never affect content.

## Prompt-program map (uploaded 00–10)

0/1 separation → this design + existing stages; 2 batch_id → run_id
lineage (pre-existing); 3 control table → BatchControl view; 4 dual
checkpoints → advance_after=RAW (pre-existing) + EXISTS ledger checkpoint;
5 curated-from-raw → curatedReplay + pending driver; 6 retry →
--replay-failed / --resume; 7 replay → selector set; 8 audit →
per-batch ledger counts + reconciliation identity (pre-existing);
9 recovery → matrix above; 10 review → post-implementation review pass.

## Execution declaration (Control-M)

`ingestion { execution = COUPLED | DECOUPLED }` (default COUPLED) is the
operator-facing switch. It adds no third behavior — it validates the
combination (CFG_016: DECOUPLED needs `advance_after = RAW` for
incremental sources and the run ledger) and makes a DECOUPLED feed refuse
`--stage all`, so a scheduler cannot double-process batches its curated
job drains. Wrapper scripts: `scripts/run_ingest.sh` (coupled),
`scripts/run_raw.sh` + `scripts/run_curated_pending.sh` (decoupled pair);
exit 0 = success including empty-pending no-ops, non-zero = failure with
failed batches left pending — unconditional reruns are safe.
