package com.hcsc.generic.ingest.jdbc.extraction

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import scala.collection.JavaConverters._

/**
  * Parses the `extraction` block of a source config into a typed
  * ExtractionSpec. Structural validation happens here (types, pairs,
  * positivity); strategy-specific rules live in each strategy's validate.
  *
  * extraction {
  *   strategy = "TIMESTAMP"          # FULL_SNAPSHOT | TIMESTAMP |
  *                                   # TIMESTAMP_KEY | INCREASING_KEY |
  *                                   # PARTITION_RANGE
  *   table = "dbo.claims"
  *   columns = ["claim_id", "amount", "modified_ts"]   # optional projection
  *   filters = ["state = 'IL'"]                        # trusted predicates
  *   boundary {
  *     columns = [ { name = "modified_ts", type = "TIMESTAMP" } ]
  *     initial = "1900-01-01 00:00:00"    # composite: values joined with '|'
  *     overlap = 300                      # seconds (TIMESTAMP) / amount (NUMERIC)
  *   }
  *   partition {
  *     column = "claim_id"
  *     num_partitions = 8
  *     lower_bound = 1                     # optional pair; absent = MIN/MAX probe
  *     upper_bound = 1000000
  *     predicates = []                     # alternative to range partitioning
  *   }
  *   fetchsize = 5000
  * }
  */
object ExtractionSpecParser {

  /** None when the source config carries no `extraction` block. */
  def parse(source: Config): Option[ExtractionSpec] =
    ConfigUtils.optConfig(source, "extraction").map(parseBlock)

  private def parseBlock(e: Config): ExtractionSpec = {
    def required(key: String): String = ConfigUtils.optString(e, key)
      .map(_.trim).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"EXT_001 extraction.$key is required"))

    val boundaryConf = ConfigUtils.optConfig(e, "boundary")
    val boundaryColumns = boundaryConf.toSeq.flatMap { b =>
      if (!b.hasPath("columns")) Seq.empty
      else b.getConfigList("columns").asScala.map { c =>
        val columnType = ConfigUtils.optString(c, "type").getOrElse(BoundaryType.Timestamp).toUpperCase
        if (!BoundaryType.all.contains(columnType))
          throw new IllegalArgumentException(
            s"EXT_001 extraction.boundary.columns type '$columnType' must be one of ${BoundaryType.all.mkString(", ")}")
        BoundaryColumn(c.getString("name"), columnType)
      }
    }

    val initial = boundaryConf.flatMap(b => ConfigUtils.optString(b, "initial")).map(BoundaryValue.deserialize)
    initial.foreach { v =>
      if (boundaryColumns.nonEmpty && v.values.size != boundaryColumns.size)
        throw new IllegalArgumentException(
          s"EXT_001 extraction.boundary.initial has ${v.values.size} part(s) but ${boundaryColumns.size} column(s) are declared")
    }

    val overlap = boundaryConf.flatMap(b => ConfigUtils.optString(b, "overlap")).map(BigDecimal(_))
    overlap.foreach(o => if (o < 0)
      throw new IllegalArgumentException(s"EXT_001 extraction.boundary.overlap must not be negative, found $o"))

    val p = ConfigUtils.optConfig(e, "partition")
    val partition = PartitionSpec(
      column = p.flatMap(c => ConfigUtils.optString(c, "column")),
      lowerBound = p.flatMap(c => ConfigUtils.optLong(c, "lower_bound")),
      upperBound = p.flatMap(c => ConfigUtils.optLong(c, "upper_bound")),
      numPartitions = p.flatMap(c => ConfigUtils.optInt(c, "num_partitions")),
      predicates = p.map(c => ConfigUtils.stringList(c, "predicates")).getOrElse(Seq.empty)
    )
    partition.numPartitions.foreach(n => if (n <= 0)
      throw new IllegalArgumentException(s"EXT_001 extraction.partition.num_partitions must be positive, found $n"))
    if (partition.lowerBound.isDefined != partition.upperBound.isDefined)
      throw new IllegalArgumentException(
        "EXT_001 extraction.partition lower_bound and upper_bound must be configured together")

    ExtractionSpec(
      strategyKind = required("strategy").toUpperCase,
      table = required("table"),
      projection = ConfigUtils.stringList(e, "columns"),
      filters = ConfigUtils.stringList(e, "filters"),
      boundaryColumns = boundaryColumns,
      initialBoundary = initial,
      overlap = overlap,
      partition = partition,
      fetchSize = ConfigUtils.optInt(e, "fetchsize").getOrElse(1000)
    )
  }
}
