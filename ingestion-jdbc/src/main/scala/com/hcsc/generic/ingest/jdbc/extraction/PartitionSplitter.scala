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
        val count = math.min(n.toLong, hi - lo + 1).toInt.max(1)
        val stride = (hi - lo + 1 + count - 1) / count // ceil so ranges cover [lo, hi]
        (0 until count).map { i =>
          val sliceLo = lo + i * stride
          val sliceHi = math.min(sliceLo + stride, hi + 1)
          RangeSlice(
            column = column,
            lowerInclusive = sliceLo,
            upperBound = if (i == count - 1) hi else sliceHi,
            upperInclusive = i == count - 1,
            includeNulls = i == 0,
            index = i)
        }
    }
  }
}
