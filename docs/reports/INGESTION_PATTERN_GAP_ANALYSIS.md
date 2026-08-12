# Ingestion Pattern Gap Analysis

**Prompt 0 deliverable — assessment only, no code changed.**

Answers one question: which of the requested extraction and curated
strategies already exist, which are partial, and which are genuinely
missing. Findings are read from the code, not from documentation.

---

## Current architecture, in the requested terms

```
Source → Extraction Strategy → Raw Write Strategy → Curated Strategy
```

| Axis | Where it lives | Vocabulary today |
|---|---|---|
| Source | `SourceRegistry` (`jdbc`, `file`, `kafka`) | pluggable, registry-based |
| Extraction | `JdbcMode` + `WatermarkType` + `PartitionStrategy` | `FULL_TABLE`, `SELECT_QUERY`, `CUSTOM_SQL`, `SQL_TEMPLATE`, `INCREMENTAL` |
| Raw write | `raw.strategy` + `raw.delivery_mode` | `APPEND_BATCH`, `SNAPSHOT`, `CDC_EVENTS`; `AT_LEAST_ONCE_APPEND`, `DEDUPLICATED_APPEND` |
| Curated | `CuratedPublishMode` + `DeleteSpec` | `MERGE`, `PARTITION_OVERWRITE`, `FULL_OVERWRITE`; `IGNORE`, `SOFT`, `FULL_SNAPSHOT_ABSENCE` |

There is already a decomposed pattern vocabulary in
`config/IngestionPattern.scala` — the abstraction Prompt 1 asks for exists
in substance, under different names. **Prompt 1 should extend that, not
introduce a parallel hierarchy**, or the framework ends up with two ways to
express the same shape.

---

## Extraction strategies

| Requested | Status | Where |
|---|---|---|
| `FULL` | **Implemented** | `JdbcMode.FullTable`; `IngestionPattern.FullSnapshot` |
| `WATERMARK` | **Implemented** | `JdbcMode.Incremental` + `WatermarkType.Timestamp/Date/DatetimeOffset/RowVersion`; upper bound pinned via `WatermarkUpperBound` |
| `ID_WATERMARK` | **Implemented** | `WatermarkType.Numeric`; composite key ordering via `WatermarkType.Composite` |
| `SLIDING_WINDOW` | **Partial** | `incremental.overlap` re-reads a look-back window every run (`TimestampOverlap`). What is missing is a *fixed-width* window independent of the stored watermark ("always re-read the last 7 days") |
| `PARTITION` | **Partial — and ambiguous** | `PartitionStrategy` (`STATIC_RANGE`, `MIN_MAX_QUERY`, `PREDICATES`) is **read parallelism**, not partition-scoped extraction. `PARTITION_RANGE` appears in `CONFIGURATION_MODEL.md` but has no implementation — a documentation defect worth fixing regardless |
| `CDC` | **Declared, deliberately unimplemented** | `IngestionPattern.ChangeTracking` / `CdcBatch` are in `unsupported`, so declaring one fails fast with CFG_010 rather than silently misbehaving. `raw.strategy = CDC_EVENTS` handles CDC-shaped *input*; nothing reads a CDC source |
| `BACKFILL` | **Implemented** | `IngestionPattern.Backfill` / `RawReplay` as intent overrides; both default `watermark_commit = false` so a backfill cannot burn the live window |

## Curated strategies

| Requested | Status | Where |
|---|---|---|
| `APPEND` | **Implemented** | `curated.strategy = APPEND` |
| `LATEST_SNAPSHOT` / `SCD1` | **Implemented** | `TYPE1_MERGE` + `CuratedPublishMode.Merge`, with freshness-compared merge, `record_hash` no-change skip and deterministic tie-breaking |
| `PARTITION_REPLACE` | **Implemented** | `CuratedPublishMode.PartitionOverwrite`, including moved-key handling |
| `DELETE_AWARE_MERGE` | **Implemented** | `DeleteSpec.Soft` (indicator column) and `SnapshotAbsence` (absence over a complete snapshot, gated on `confirm_complete_extract`) |
| `SCD2` | **NOT IMPLEMENTED** | no effective-dating, no version chain, no current-flag maintenance anywhere in `CuratedService` |

---

## Gaps, ranked

**1. SCD2 — the only wholly missing capability.**
Everything else is present or partial. SCD2 is a different shape from every
existing curated mode: today's merge keeps ONE row per business key, while
SCD2 keeps a version chain with effective-from/to and a current flag, and
must close the prior version atomically with opening the new one. It cannot
be expressed by configuring `TYPE1_MERGE`.

**2. `PARTITION_RANGE` is documented but does not exist.**
`CONFIGURATION_MODEL.md` lists it as an extraction strategy. Either
implement partition-scoped extraction or remove the line — a configuration
key that appears in the reference and fails at runtime is worse than an
absent feature.

**3. `SLIDING_WINDOW` has no name of its own.**
`overlap` covers late-arriving data relative to the watermark. A declared
fixed-width window is a distinct requirement (reprocess the last N days
regardless of watermark position) and would currently be hand-rolled per
feed.

**4. CDC extraction is honestly absent, not broken.**
The fail-fast on `CHANGE_TRACKING` / `CDC_BATCH` is the right behaviour and
should be preserved. Implementing CDC means a new source reader, not a new
curated mode — `CDC_EVENTS` raw input and `DELETE_AWARE_MERGE` already
handle the downstream half.

---

## Recommendation for Prompts 1–16

**Extend `IngestionPattern`; do not create a parallel strategy hierarchy.**
It already decomposes the axes the pack asks for, already validates
incompatible combinations (CFG_010–CFG_016), and already fails closed on
unimplemented capabilities. A second abstraction alongside it would double
the validation surface and split the vocabulary.

Sequencing by value, given the above:

1. **SCD2** (Prompt 9) — the only true capability gap, and the one with
   real design content
2. **`SLIDING_WINDOW`** (Prompt 7) — small, and mostly naming plus
   validation over machinery that exists
3. **Configuration consolidation** (Prompt 15) — worthwhile, but it must
   preserve every current key; the deployed SmartIQ feeds use them
4. **`PARTITION_RANGE`** (Prompt 11) — implement or retract the doc line
5. **CDC** (Prompt 13) — largest, least urgent; the fail-fast is correct
   in the meantime

Prompts 2–6, 8, 10, 12 and 14 describe behaviour that is **already
implemented and tested**. For those, the useful work is verification and
documentation of the support matrix (Prompt 16), not new code.

---

## Backward-compatibility constraint

Two SmartIQ feeds are deployed and one has passed an end-to-end run. Every
change from this pack must leave their configuration parsing and behaving
identically. The existing 796 tests are the contract; per Prompt 16, they
are not to be weakened to make new functionality pass.
