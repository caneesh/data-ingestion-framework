package com.hcsc.generic.ingest.raw

/**
  * Strategy kinds and a pure factory — no mutable registry. Strategies are
  * stateless; each call returns a fresh, independently wired instance.
  */
object RawWriteStrategies {
  val AppendBatch = "APPEND_BATCH"
  val Snapshot = "SNAPSHOT"
  val CdcEvents = "CDC_EVENTS"

  val all: Seq[String] = Seq(AppendBatch, Snapshot, CdcEvents)

  def forKind(kind: String): RawWriteStrategy = kind.toUpperCase match {
    case AppendBatch => new AppendBatchStrategy
    case Snapshot    => new SnapshotStrategy
    case CdcEvents   => new CdcEventsStrategy(new AppendBatchStrategy)
    case other =>
      throw new IllegalArgumentException(
        s"RAW_003 Unknown raw write strategy '$other'. Available: ${all.mkString(", ")}")
  }
}
