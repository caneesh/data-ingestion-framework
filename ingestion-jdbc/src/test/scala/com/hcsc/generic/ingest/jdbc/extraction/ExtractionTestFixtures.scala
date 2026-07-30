package com.hcsc.generic.ingest.jdbc.extraction

import java.sql.Timestamp

/** Constructor-injected stub probe: answers are data, calls are recorded —
  * the DI seam every strategy test uses instead of a database. */
final class StubProbe(
  maxAnswer: Option[BoundaryValue] = None,
  minMaxAnswer: Option[(Long, Long)] = None
) extends SourceBoundaryProbe {
  var maxCalls: List[(Seq[BoundaryColumn], Seq[String])] = Nil
  var minMaxCalls: List[String] = Nil

  override def max(columns: Seq[BoundaryColumn], relation: Relation, filters: Seq[String]): Option[BoundaryValue] = {
    maxCalls = (columns, filters) :: maxCalls
    maxAnswer
  }

  override def minMax(column: String, relation: Relation, filters: Seq[String]): Option[(Long, Long)] = {
    minMaxCalls = column :: minMaxCalls
    minMaxAnswer
  }
}

object ExtractionTestFixtures {
  val capturedAt: Timestamp = Timestamp.valueOf("2026-07-28 12:00:00")

  val tsColumn = BoundaryColumn("modified_ts", BoundaryType.Timestamp)
  val keyColumn = BoundaryColumn("claim_id", BoundaryType.Numeric)

  def spec(
    kind: String,
    boundaryColumns: Seq[BoundaryColumn] = Seq.empty,
    initial: Option[String] = None,
    overlap: Option[BigDecimal] = None,
    partition: PartitionSpec = PartitionSpec.none,
    filters: Seq[String] = Seq.empty
  ): ExtractionSpec = ExtractionSpec(
    strategyKind = kind,
    table = "dbo.claims",
    projection = Seq.empty,
    filters = filters,
    boundaryColumns = boundaryColumns,
    initialBoundary = initial.map(BoundaryValue.deserialize),
    overlap = overlap,
    partition = partition,
    fetchSize = 1000
  )

  def context(
    spec: ExtractionSpec,
    previous: Option[Checkpoint] = None
  ): ExtractionContext =
    ExtractionContext("claims_feed", "run-1", spec, previous, capturedAt)

  def checkpoint(value: String, version: Long = 1L, kind: String = "TIMESTAMP"): Checkpoint =
    Checkpoint("claims_feed", kind, BoundaryValue.deserialize(value), version, "run-0", capturedAt)
}
