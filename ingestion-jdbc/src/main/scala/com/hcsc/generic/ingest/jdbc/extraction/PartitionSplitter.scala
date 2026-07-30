package com.hcsc.generic.ingest.jdbc.extraction

import ExtractionPartition.{PredicateSlice, RangeSlice}

/**
  * Turns a PartitionSpec into concrete slices. Injected into strategies
  * (composition — there is deliberately no strategy base class).
  *
  * Guarantees: slices are pairwise disjoint and complete over the partition
  * column's value space — contiguous ranges, inclusive final upper bound,
  * and NULLs assigned to the first slice.
  */
final class PartitionSplitter(probe: SourceBoundaryProbe) {

  def split(spec: PartitionSpec, relation: Relation, filters: Seq[String]): Seq[ExtractionPartition] = {
    if (spec.predicates.nonEmpty)
      return spec.predicates.zipWithIndex.map { case (p, i) => PredicateSlice(p, i) }
    if (spec.column.isEmpty || spec.numPartitions.isEmpty)
      return Seq.empty // single full read

    val column = spec.column.get
    val n = spec.numPartitions.get
    require(n > 0, s"EXT_001 num_partitions must be greater than zero, found $n")

    val bounds = (spec.lowerBound, spec.upperBound) match {
      case (Some(lo), Some(hi)) => Some((lo, hi))
      case (None, None)         => probe.minMax(column, relation, filters)
      case _ =>
        throw new IllegalArgumentException(
          "EXT_001 partition lower_bound and upper_bound must be configured together (or neither, for MIN/MAX probing)")
    }

    bounds match {
      case None => Seq.empty // empty source: one full (empty) read
      case Some((lo, hi)) =>
        require(lo <= hi, s"EXT_001 partition lower_bound $lo must not exceed upper_bound $hi")
        // Bounds SIZE the strides; they never filter. The first slice is
        // lower-unbounded and the last upper-unbounded (Spark JDBC
        // semantics), so rows outside [lo, hi] — stale bounds, values
        // arriving after a MIN/MAX probe, DECIMAL truncation — are still
        // read. BigInt keeps the stride math safe at Long extremes.
        val range = BigInt(hi) - BigInt(lo) + 1
        val count = range.min(BigInt(n)).toInt.max(1)
        if (count == 1) return Seq.empty // one slice = plain full read
        val stride = (range + count - 1) / count
        val cut: Int => Long = i => (BigInt(lo) + stride * i).toLong
        (0 until count).map { i =>
          RangeSlice(
            column = column,
            lowerInclusive = if (i == 0) None else Some(cut(i)),
            upperExclusive = if (i == count - 1) None else Some(cut(i + 1)),
            includeNulls = i == 0,
            index = i)
        }
    }
  }
}
