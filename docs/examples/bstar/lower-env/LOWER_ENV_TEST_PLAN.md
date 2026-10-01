# bstar — lower-environment end-to-end test plan

A stand-in for the two production feeds: **the full contracts (24 and 30
columns), a synthetic Hive source, one database, the same machinery**. A
green walk through this plan means the hive source, the `raw.mode = SOURCE`
raw layer, the lookback, the CDC soft-delete merge and the replay paths all
work against the real metastore and the real cluster — with no real bstar
row involved.

Everything is in `bluestar_e2e` (`BSTAR_E2E_DB` to change it): the source
tables `priv_addr_src` / `sub_prem_det_src`, the curated tables `priv_addr` /
`sub_prem_det`, and the control tables `ingest_*`. Production separates
these across `bstar_raw` / `bluestar_raw` / `bluestar_current`; the
separation is config, and `BstarConfigTest` pins the production files.

## Pre-flight checklist

**1. Refresh the artifacts on the edge node.** `sync_artifacts.sh` knows
only the smartiq file set; copy the bstar pair by hand (the feed and its
contract must sit side by side):

```bash
git pull
cp docs/examples/bstar/lower-env/params/feed-bstar-priv-addr-e2e.conf \
   docs/examples/bstar/lower-env/params/bstar-priv-addr-e2e-schema.conf "$BSTAR_CONF_DIR"/
```

`run_bstar.sh` warns (never blocks) when the copy in `BSTAR_CONF_DIR`
differs from the repo. The FIRST line of every driver log is
`[Build] ingestion framework <version> (built <timestamp>)`. If that
timestamp is not from your build, nothing else you observe can be trusted.

**2. Clear any half-created curated tables.** A previous run that mixed the
hand-written DDL and a framework-created table at one path leaves files
whose schema does not match the table (`converted table has N columns, but
source Hive table has M columns`). `DROP TABLE` on an EXTERNAL table does
**not** remove the files:

```sql
DESCRIBE FORMATTED bluestar_e2e.priv_addr;      -- note Location
```
```bash
hdfs dfs -rm -r -skipTrash <curated Location>
```
```sql
DROP TABLE IF EXISTS bluestar_e2e.priv_addr;
```

Never touch the SOURCE tables this way — recreate them only through section
0 of `ddl/source_test_data.sql`.

**3. Clear the entity's control rows** when restarting the walk from
scenario 1 (a stale watermark makes scenario 1 an empty window):

```sql
DELETE FROM bluestar_e2e.ingest_watermarks WHERE entity = 'bstar_priv_addr_e2e';
-- or, on a store without row deletes: runbook §2.3 (rewind) with
-- initial_value as the target
```

**4. Site settings.** `scripts/bstar.env` from `bstar.env.example`:
`BSTAR_CONF_DIR` pointing at a directory holding **both** the feed and its
`*-e2e-schema.conf` (the feed includes it by relative name), `BSTAR_JAR`,
`BSTAR_E2E_DB` if not `bluestar_e2e`. No password, no JDBC driver — the
hive source reads through the session's own metastore access.

**5. Validate before reading anything:**

```bash
scripts/run_bstar.sh e2e priv_addr INCR --validate-only
scripts/run_bstar.sh e2e priv_addr INCR --dry-run --run-id pa-dry
```

`--validate-only` parses config and contract and confirms the source table
exists (HIVE_002 if not) and the watermark columns are STRING partition
columns (HIVE_003). `--dry-run` reads the window and runs the checks but
writes nothing — the ledger row for `pa-dry` has `raw_count = 0`.

## The scenario walk

One section of `ddl/source_test_data.sql` per run, in order. After each run
check the three places the framework writes: the ledger
(`ingest_run_audit`), the checks (`ingest_reconciliation`) and the curated
table. Run ids are pinned with `--run-id` so a replay in scenario 8 can name
them.

```bash
scripts/run_bstar.sh e2e priv_addr INCR --run-id pa-1     # after section 1
```

| # | Section | Run | What it proves | Expected |
|---|---------|-----|----------------|----------|
| 1 | 1 | `pa-1` | First light over an F and an I partition; CHAR key padding trimmed; latest-per-key across partitions | 4 curated rows; `GRP000001/1/1/1` = `101 TEST ST`, every `last_modified_op = 'I'`; ledger `window_start = 1900-01-01\|00.00.00`, `window_end = 2026-03-02\|04.00.00`, `raw_count = accepted_count = 5`; watermark `2026-03-02\|04.00.00` v1; checks `source_equals_accepted_plus_rejected`, `curated_accounts_for_accepted_rows` passed; **no** `raw_equals_accepted` row (nothing written) |
| 2 | 2 | `pa-2` | Steady state: only the new partition read; UP → `'U'`; continuity | `GRP000002` `str_ln_2 = 'APT 3'`, op `'U'`; 4 rows; `window_start = 2026-03-02\|04.00.00`; watermark `2026-03-03\|04.00.00` v2; `watermark_continuity` passed |
| 3 | 3 | `pa-3` | **Late partition inside the lookback**: re-read, stale version loses on freshness, late new key lands | `GRP000001/1/1/1` **still** `101 TEST ST`; `GRP000004` inserted; 5 rows; no duplicate keys; ledger `window_start` is the STORED watermark (`2026-03-03\|04.00.00`), not the rewound bound; watermark **unchanged** at v2 (the late partition sorts below it) |
| 4 | 4 | `pa-4` | DL tombstone (SOFT) | `GRP000001/1/1/2` `is_deleted = true`, op `'D'`; still 5 rows; `is_deleted IS NOT TRUE` → 4 |
| 5 | 5 | `pa-5` | Null key quarantine (CUR_001) with PII masked; CHAR-padded key matches | run SUCCESS; 1 row in `ingest_rejects` with `str_ln_1` / `zip_cd` / `phn_nbr` masked; `GRP000003` updated to `301 TEST BLVD`; 5 rows |
| 6 | 6 | `pa-6` | Empty window is a clean no-op (the lookback partitions are re-read; nothing new) | ledger SUCCESS; curated unchanged; watermark `2026-03-05\|04.00.00` v4 unchanged, no new version row |
| 7 | 7 | `pa-7` then `--stage reconcile` | **Very late partition outside the lookback** is NOT read; reconcile is the detector | `pa-7` re-reads only the lookback partitions (`2026-03-03` … `2026-03-05`); the `2026-03-01\|23.00.00` partition is below the rewound bound, so `GRP000005` is absent from curated; reconcile exits **0** (REPORT) with a `source_keys_present_in_curated` row `passed = false` in `ingest_reconciliation`; recovery = runbook §2.3 rewind to `2026-03-01\|00.00.00` and re-run |
| 8a | — | `--stage curated --run-id pa-2` | Replay re-reads the recorded window from the source | curated unchanged (replayed rows are older than what curated holds); watermark untouched; `curated_accounts_for_replayed_rows` passed |
| 8b | — | drop partition, then `--stage curated --run-id pa-2` | **HIVE_008**: the recorded window is gone from the source | exit **20** (DATA_INTEGRITY); ledger curated row FAILED; curated unchanged. Re-insert section 2 to restore the source |
| S1 | S, S1 | `sp-1` (sub_prem_det) | 7-column key: rows differing only in `sub_prm_det_sq_nbr` stay distinct; DECIMAL types; second entity on shared control tables | 2 curated rows, `prem_amt` 123.45 / 67.89; its own watermark row and lock row beside priv_addr's |
| S2 | S2 | `sp-2` | UP + DL on the composite key | det 1 `prem_amt = 130.00` op `'U'`; det 2 `is_deleted = true` op `'D'` |

Useful queries:

```sql
SELECT run_id, stage, status, window_start, window_end, raw_count, accepted_count, event_ts
FROM   bluestar_e2e.ingest_run_audit WHERE entity = 'bstar_priv_addr_e2e' ORDER BY event_ts;

SELECT run_id, check_name, passed, detail
FROM   bluestar_e2e.ingest_reconciliation WHERE entity = 'bstar_priv_addr_e2e' ORDER BY run_id, check_name;

SELECT watermark_version, watermark_value, run_id, updated_ts
FROM   bluestar_e2e.ingest_watermarks WHERE entity = 'bstar_priv_addr_e2e' ORDER BY watermark_version;

SELECT corp_ent_cd, acct_grp_nbr, sub_seq_nbr, mem_nbr, addr_seq_nbr, str_ln_1, cdc_src_actn_cd,
       file_date, file_time, last_modified_op, is_deleted
FROM   bluestar_e2e.priv_addr ORDER BY 1,2,3,4,5;
```

## Failure drills (after the walk)

| Drill | How | Expected |
|-------|-----|----------|
| FULL refused once a watermark exists | `run_bstar.sh e2e priv_addr FULL` | exit **30**, `HIVE_007` naming the entity and the stored version; nothing read |
| Wrong source table | `BSTAR_E2E_DB=nosuchdb run_bstar.sh e2e priv_addr INCR --validate-only` | exit 30, `HIVE_002` |
| `--resume-ingest-dt` under SOURCE | `... INCR --stage curated --resume-ingest-dt 2026-03-01` | exit 30, `CFG_028` (there is no RAW partition to replay; use `--run-id`) |
| Lock contention | start a run, immediately start a second | second exits with `PIPE_001`; runbook "PIPE_001" |
| Alert routing | set `BSTAR_ALERT_WEBHOOK` to a request-bin, run the HIVE_007 drill | one redacted FAILURE message arrives |
| Retention | `... INCR --stage retention --dry-run` then without | dry-run lists what would go; real run leaves `watermarks_keep_last` versions; the source is untouched (`SHOW PARTITIONS` identical before/after) |

## Exit criteria

- Every row of the scenario table matches, including the two **unchanged**
  outcomes (3 and 8a) — a merge that is too eager passes every other row.
- Both drills that must FAIL (HIVE_007, HIVE_008) fail with the right exit
  code; Control-M tells retry from page by that number alone.
- `SHOW PARTITIONS bluestar_e2e.priv_addr_src` is identical before and
  after the whole walk except for the partitions the plan itself added or
  dropped: the pipeline never writes, alters or drops the source.
