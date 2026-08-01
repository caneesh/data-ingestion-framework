package com.hcsc.generic.ingest.transform

/** Source-specific TECHNICAL metadata columns (Kafka coordinates, file
  * record numbers, sequence numbers). They flow into RAW for audit and
  * idempotency, but must never become curated business columns by
  * accident: the curated stage drops them unless the feed opts in with
  * `curated.retain_source_metadata = true` — and a feed whose merge keys,
  * ordering, partitioning or delete indicator references one MUST opt in,
  * loudly, instead of the column silently vanishing mid-merge. */
object SourceMetadata {

  val KafkaColumns: Set[String] =
    Set("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "kafka_tombstone")

  val FileColumns: Set[String] =
    Set("source_file_modified_ts", "source_record_number")

  val CommonColumns: Set[String] =
    Set("source_sequence_number")

  /** All technical source-metadata names, lowercase. */
  val TechnicalColumns: Set[String] = KafkaColumns ++ FileColumns ++ CommonColumns
}
