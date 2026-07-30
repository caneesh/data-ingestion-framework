package com.hcsc.generic.ingest.jdbc.extraction

import java.sql.Timestamp

/** Parallel-read configuration. Bounds may be absent — the splitter probes
  * MIN/MAX from the source when a column and partition count are given
  * without explicit bounds. */
final case class PartitionSpec(
  column: Option[String],
  lowerBound: Option[Long],
  upperBound: Option[Long],
  numPartitions: Option[Int],
  predicates: Seq[String]
) {
  def isEmpty: Boolean =
    column.isEmpty && numPartitions.isEmpty && predicates.isEmpty
}

object PartitionSpec {
  val none: PartitionSpec = PartitionSpec(None, None, None, None, Seq.empty)
}

/** The typed product of configuration parsing — strategies never see raw
  * HOCON. One spec describes one entity's extraction. */
final case class ExtractionSpec(
  strategyKind: String,
  table: String,
  projection: Seq[String],
  filters: Seq[String],
  boundaryColumns: Seq[BoundaryColumn],
  initialBoundary: Option[BoundaryValue],
  overlap: Option[BigDecimal],
  partition: PartitionSpec,
  fetchSize: Int
)

/**
  * The complete input to planning. Strategies are pure functions of this
  * context (plus their injected probe): equal contexts with equal probe
  * answers must produce equal plans. `capturedAt` is injected — strategies
  * never read the clock themselves.
  */
final case class ExtractionContext(
  entity: String,
  runId: String,
  spec: ExtractionSpec,
  previousCheckpoint: Option[Checkpoint],
  capturedAt: Timestamp
) {
  def relation: Relation = Relation.Table(spec.table)
}
