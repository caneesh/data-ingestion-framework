# Input Modes and the Shared Curated Writer

One curated writer — `CuratedService` (batch) with `CuratedMicroBatch` as
its `foreachBatch` facade — serves every input mode. Sources standardize to
a DataFrame before the curated layer; the writer receives only the frame,
the curated config, the run/batch identifier and the contract. It never
touches connections, topics, offsets or files. There are NO per-source
merge algorithms.

| Concern | Where it lives (all modes) |
|---|---|
| Config / business-key / partition validation | CuratedService (CUR_00x) |
| Incoming dedup + latest-record selection | freshness column + directional tie-breakers, `row_number`, target wins exact ties |
| Affected partitions / merge / partition overwrite / full overwrite | CuratedService publish modes (CURATED_PARTITIONING.md) |
| Schema alignment, audit metrics, failure recovery | align/guards, run ledger, staged publish |

## Per-source position tracking

| Mode | Position | Advanced when |
|---|---|---|
| File | content checksums in the file registry; files move to processed | after raw + curated + audit succeed |
| JDBC | versioned watermark store, bounded `(lower, upper]` window with captured upper and optional overlap | same gate (`WatermarkAdvancing`) |
| Kafka batch | **offset-range ledger** (`ingest_kafka_offsets`): starting offsets = MAX committed until-offset per partition, end offsets PINNED via the consumer API before the read | same gate — a failed run never commits offsets; the replay re-reads the identical window |
| Kafka streaming | Spark checkpoint (`checkpoint_location`); batch id = run identity | checkpoint advances after `foreachBatch` returns |

**Equal timestamps (documented semantics).** The JDBC window is
half-open — `wm > lower AND wm <= captured_upper` — so a row exactly at the
upper bound lands in THIS window and never re-appears in the next (the
HALF_OPEN convention; overlap re-reads are deduplicated by the curated
merge). Within the merge, equal freshness values fall to the configured
tie-breakers (e.g. `src_seq`, `kafka_offset`); an exact tie on everything
keeps the TARGET row — an older or equal record can never replace a newer
one, from any source.

## Kafka specifics

- Metadata columns `kafka_topic/kafka_partition/kafka_offset/
  kafka_timestamp/kafka_tombstone` are stamped by the source (tombstone flag
  captured BEFORE JSON decoding so null-value records survive parsing).
  `source.retain_kafka_metadata = false` drops them at the source.
- **They never become curated business columns by accident**: the curated
  stage drops all technical source metadata (`SourceMetadata`) unless
  `curated.retain_source_metadata = true` — and fails with CUR_007 if merge
  keys, ordering, partitioning or the delete indicator reference a column
  that would be dropped.
- Out-of-order events: order by `kafka_timestamp` (or a payload sequence)
  with `kafka_offset` as tie-breaker (requires `retain_source_metadata`).
- Tombstones / CDC deletes: `deletes { mode = SOFT, indicator_column =
  "kafka_tombstone" }`, or a payload op column for CDC streams
  (insert/update flow through the merge; delete via the indicator).
- Streaming: `KafkaStreamingRunner.start(spark, feedConf, entity, logger)`
  wires `readStream → decode → foreachBatch → CuratedMicroBatch`. Empty
  micro-batches no-op; FULL_OVERWRITE is refused for micro-batches;
  replaying a batch id is idempotent.

## Exactly-once statement (non-transactional Hive targets)

Not achievable, and not claimed. The guarantee is **at-least-once delivery
with a convergent target**: positions (files/watermarks/offsets/checkpoint)
advance only after the curated write succeeds, and a replayed
window/file/offset-range/micro-batch re-derives the same winners — replayed
rows count as `ignored`, the snapshot is unchanged. A crash between the
curated write and position advancement causes reprocessing, never loss and
never divergence. True exactly-once requires a transactional table format
(plan item #19).

## Source behavior that cannot use the common writer

Reported honestly:

1. **Nothing in the merge semantics.** Every source's insert/update/delete/
   out-of-order/duplicate behavior is expressed through the common model
   (freshness + tie-breakers + delete indicators).
2. **Position tracking is inherently per-source** (checksums vs watermark
   vs offsets vs checkpoint) and lives in the sources/pipeline, not the
   writer — by design.
3. **Cross-source precedence beyond freshness is not implemented**: two
   sources writing one curated table must share the freshness/tie-breaker
   contract (a `source_precedence` tie-breaker column is expressible today
   by deriving a rank column per feed, but no first-class config exists).
   The entity lock serializes runs of ONE entity; two DIFFERENT entities
   writing the same curated table are not partition-locked against each
   other — configure them as one entity (or accept last-publisher-wins per
   run) until table-level locking is added.
4. **Kafka Avro payloads** are not yet decoded (needs spark-avro; explicit
   `UnsupportedOperationException`).
5. **File row-removal by position does not survive an input split.**
   `skip_first_n` and `trailer.by_last_row` order rows with
   `monotonically_increasing_id()`, which tracks physical line order only
   while a file is read as a single input split (~one HDFS block). Above
   that the ordering is arbitrary and the WRONG rows are dropped — silently,
   since the row count is still correct. Both also funnel every row of a
   file through one partition. `trailer.marker` is a content filter with no
   ordering dependency and is the safe form at any size; the order-dependent
   options warn on every run.
