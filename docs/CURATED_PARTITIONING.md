# Curated Partitioning & Write Strategies

Status: implemented (non-transactional Hive **datasource** tables, ORC/Parquet).
The curated writer is `CuratedService` — one implementation shared by every
input mode (file, JDBC, Kafka batch): sources standardize to a DataFrame
before the curated layer and the writer never sees connections, topics,
offsets or files.

## Configuration

```hocon
curated {
  database = analytics
  table    = member
  format   = parquet                    # orc | parquet (datasource tables)

  partitioning {                        # absent/empty keys = unpartitioned
    keys = ["src_sys_nm", "corp_ent_cd"]
    null_values = "REJECT"              # REJECT (default) | DEFAULT | HIVE
    default_value = "UNKNOWN"           # required when DEFAULT
    max_affected_partitions = 1000      # literal-pruning cap; beyond -> semi-join
  }

  publish {
    mode = "PARTITION_OVERWRITE"        # MERGE | PARTITION_OVERWRITE | FULL_OVERWRITE
    # absent = legacy behavior: INCR vs existing table merges, else full replace
  }

  merge {
    keys = ["member_id"]                # business keys — INDEPENDENT of layout
    freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
  }
}
```

### Examples

Unpartitioned merge (unchanged legacy behavior):
```hocon
curated { database = analytics, table = member, format = parquet
  merge { keys = ["member_id"], freshness { column = "src_modified_ts" } } }
```

Single partition, partition-scoped rewrite:
```hocon
curated { database = analytics, table = member, format = parquet
  partitioning { keys = ["src_sys_nm"] }
  publish { mode = "PARTITION_OVERWRITE" }
  merge { keys = ["member_id"], freshness { column = "src_modified_ts" } } }
```

Multiple partitions:
```hocon
curated { database = analytics, table = member, format = parquet
  partitioning { keys = ["src_sys_nm", "corp_ent_cd"] }
  publish { mode = "PARTITION_OVERWRITE" }
  merge { keys = ["member_id"], freshness { column = "src_modified_ts" } } }
```

## Write strategies

| Mode | What happens | Cost | Use when |
|---|---|---|---|
| `MERGE` | Freshness-compared merge materialized as a **full staged rewrite** (the format has no row-level MERGE — no fake merge is claimed) | O(table) per run | Unpartitioned tables; small/medium tables; when every run may touch most partitions |
| `PARTITION_OVERWRITE` | The same merge, scoped to the **affected partitions only**: incoming partitions ∪ old partitions of moved keys are read, contested and rewritten via staged dynamic partition overwrite | O(affected slice) | Partitioned tables with localized daily deltas — the normal choice for large partitioned curated tables |
| `FULL_OVERWRITE` | Explicit full snapshot rebuild | O(table) | Complete-snapshot feeds; `deletes.mode = FULL_SNAPSHOT_ABSENCE`; disaster rebuilds. Never selected implicitly for a partitioned incremental run |

Absent `publish.mode` keeps the legacy mapping (INCR + existing table →
MERGE; otherwise FULL_OVERWRITE), so **existing feeds are unchanged**.

All three modes share one code path for validation, key hygiene
(null-key quarantine/passthrough), in-batch dedup (deterministic
`row_number` over freshness + tie-breakers; exact ties keep the target),
stale-update protection, SOFT-delete tombstones, the record_hash no-change
pre-filter, alignment guards and the count identity
(`insert+update+delete+ignored+nullKey+deduped+passthrough == accepted`).

## Semantics and trade-offs

**Business keys vs partition columns.** Independent by design. When the
partition columns are NOT a subset of the business keys, a key can move
between partitions; `PARTITION_OVERWRITE` then runs a column-pruned key
lookup against the target to add the old partitions of contested keys to
the affected set (one extra key-column scan — the documented trade-off).
When partition columns ⊆ business keys, keys cannot move and the lookup is
skipped.

**Moved keys / emptied partitions.** Dynamic partition overwrite only
replaces partitions PRESENT in the written data, so a partition emptied by
a move is explicitly `ALTER TABLE ... DROP PARTITION`ed after a successful
publish. Emptied tuples containing NULL values (HIVE null policy) cannot be
addressed by DROP PARTITION and are logged instead.

**Full replace on a partitioned table** uses STATIC overwrite mode —
dynamic mode would silently keep partitions the staged data does not
mention. Conversely the partition-scoped path sets
`spark.sql.sources.partitionOverwriteMode=dynamic` (session-scoped,
restored afterward). A partitioned **Hive-format** (`STORED AS`) target
cannot express a full replace in one INSERT OVERWRITE at all and is
rejected with guidance (CUR_006): recreate as a datasource table or use
PARTITION_OVERWRITE.

**Affected-partition pruning.** Distinct tuples are collected only up to
`max_affected_partitions` (default 1000) to build a literal pruning
predicate; beyond the cap the target is filtered with a broadcast
semi-join instead — never an unbounded driver collect, never a per-partition
job loop.

**Null partition values.** `REJECT` (default) fails the run before any
write; `DEFAULT` substitutes `default_value` (cast to the column type);
`HIVE` passes through to `__HIVE_DEFAULT_PARTITION__`.

**Empty batch.** Zero incoming rows after hygiene returns before any
partition discovery: nothing is read, written or dropped; the run succeeds.

**Failure safety.** All modes stage the full replacement data
(`saveAsTable` + validation + uniqueness/shrink guards) before the target
is touched; the shrink guard for partition-scoped publishes compares
against the affected-slice count, not the whole table. Residual risk on
this format: a multi-partition INSERT OVERWRITE is not atomic across
partitions — a crash mid-statement can leave some affected partitions
rewritten and others not. Mitigation: rerunning the same batch is
idempotent (same slice, same winners, same output), and the emptied-
partition drop runs only after a successful publish. True multi-partition
atomicity requires a transactional format (plan item #19).

## Input-mode independence

`CuratedService.process(df, runMode, ctx, contract, rejects)` is the single
entry point for every source. File and JDBC pipelines already call it; a
Kafka Structured Streaming `foreachBatch` can call the same method with the
micro-batch DataFrame and `batchId` as the run id — empty micro-batches
no-op, and replays of a batch id are idempotent at the curated layer.
Framework/source metadata columns (`RawMetadata.ColumnNames`) never leak
into curated business content: they are excluded from record hashing and
dropped by target-schema alignment unless the target declares them.

Not yet implemented (honest gaps): a Kafka offset-range ledger
(offset tracking marked complete only after curated success), a shipped
streaming driver around `foreachBatch`, and cross-SOURCE precedence
ordering (today ordering is by the configured freshness columns regardless
of source; two sources writing one table must share that contract — the
entity lock serializes concurrent runs of one entity, and partition-level
locking across DIFFERENT entities does not exist). Exactly-once into
non-transactional Hive is not achievable; the guarantee is idempotent
at-least-once.

## Migration & rollback

Existing tables (unpartitioned): no action; configs without `partitioning`
behave exactly as before. To adopt partitioning for an existing table the
layout must be rebuilt once: add the `partitioning` block + set
`publish.mode = FULL_OVERWRITE` for one complete-snapshot run (the publish
recreates the table partitioned — the guard requires recreating
Hive-format targets as datasource tables first), then switch to
`PARTITION_OVERWRITE`. Rollback: remove the `partitioning` block and run
one `FULL_OVERWRITE` rebuild; or revert the config and restore from the
previous snapshot tag — the raw layer is untouched by any of this and can
always replay curated via `--stage curated`.

## INSERT OVERWRITE scaling honesty (350-column current-snapshot tables)

The MERGE publish rewrites the full table per run; PARTITION_OVERWRITE
rewrites affected partitions. Expected behavior at scale (unbenchmarked
estimates — a reproducible wide-data benchmark is recorded as follow-up
work, not shipped):

| Rows × 350 cols | MERGE (full rewrite) | PARTITION_OVERWRITE |
|---|---|---|
| 1M | comfortable (seconds–minutes) | comfortable |
| 10M | workable; run time grows linearly with history | comfortable when deltas are partition-localized |
| 100M | full rewrite per run becomes the dominant cost and an operational risk window | viable IF partitioning matches delta locality; otherwise degrades toward full rewrite |

At the 100M+ scale the honest answer remains a transactional format with
row-level MERGE (plan item #19) — O(delta) instead of O(table), plus
multi-partition atomicity. Interim levers: PARTITION_OVERWRITE with a
delta-aligned layout, `max_affected_partitions`, and the key-only
projections already used for the contest joins.
