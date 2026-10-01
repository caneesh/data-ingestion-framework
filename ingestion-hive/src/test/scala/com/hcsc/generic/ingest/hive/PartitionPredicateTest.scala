package com.hcsc.generic.ingest.hive

import org.scalatest.funsuite.AnyFunSuite

/** Pure parts of the partition predicate — no SparkSession. */
class PartitionPredicateTest extends AnyFunSuite {

  private val cols = Seq("file_date", "file_time")

  test("greater-than text for one, two and three columns") {
    assert(PartitionPredicate.greaterThanSql(Seq("d"), Seq("2025-01-07")) == "(d > '2025-01-07')")
    assert(PartitionPredicate.greaterThanSql(cols, Seq("2025-01-07", "06.24.07")) ==
      "(file_date > '2025-01-07') OR (file_date = '2025-01-07' AND file_time > '06.24.07')")
    assert(PartitionPredicate.greaterThanSql(Seq("a", "b", "c"), Seq("1", "2", "3")) ==
      "(a > '1') OR (a = '1' AND b > '2') OR (a = '1' AND b = '2' AND c > '3')")
  }

  test("sql composes lower, upper and where; quotes are doubled") {
    val p = PartitionPredicate(cols, Some(Seq("2025-01-07", "06.24.07")), Some(Seq("2025-01-10", "04.52.00")),
      Some("inc_ful_flag = 'I'"))
    assert(p.sql == "((file_date > '2025-01-07') OR (file_date = '2025-01-07' AND file_time > '06.24.07')) AND " +
      "(NOT ((file_date > '2025-01-10') OR (file_date = '2025-01-10' AND file_time > '04.52.00'))) AND " +
      "((inc_ful_flag = 'I'))")
    assert(PartitionPredicate.quote("it's") == "'it''s'")
    assert(PartitionPredicate(cols, None, None, None).sql == "true")
  }

  test("lexicographic compare with the equal-first-column tie-break") {
    import PartitionPredicate.compare
    assert(compare(Seq("2025-01-07", "06.24.07"), Seq("2025-01-07", "06.24.07")) == 0)
    assert(compare(Seq("2025-01-07", "10.05.54"), Seq("2025-01-07", "06.24.07")) > 0)
    assert(compare(Seq("2025-01-08", "00.00.00"), Seq("2025-01-07", "23.59.59")) > 0)
    assert(compare(Seq("2025-01-07", "06.24.07"), Seq("2025-01-07", "06.24.08")) < 0)
    val sorted = Seq(Seq("2025-02-01", "04.36.32"), Seq("2025-01-07", "06.24.07"), Seq("2025-01-07", "10.05.54"))
      .sorted(PartitionPredicate.tupleOrdering)
    assert(sorted.map(_.mkString("|")) == Seq("2025-01-07|06.24.07", "2025-01-07|10.05.54", "2025-02-01|04.36.32"))
  }

  test("rewind by days crosses a month boundary and blanks the trailing components") {
    assert(PartitionPredicate.rewindDays(Seq("2025-03-02", "06.24.07"), 3, None) == Seq("2025-02-27", ""))
    assert(PartitionPredicate.rewindDays(Seq("2025-01-01", "x", "y"), 1, None) == Seq("2024-12-31", "", ""))
  }

  test("rewind by days honours a custom date format") {
    assert(PartitionPredicate.rewindDays(Seq("20250302"), 2, Some("yyyyMMdd")) == Seq("20250228"))
  }

  test("equal-length guard rejects an unpadded value") {
    val err = PartitionPredicate.formatGuard(cols, Seq(Seq("2025-01-07", "2025-01-08"), Seq("06.24.07", "6.24.07")), Nil)
    assert(err.exists(_.startsWith("HIVE_006")))
    assert(err.get.contains("file_time"))
    assert(PartitionPredicate.formatGuard(cols, Seq(Seq("2025-01-07"), Seq("06.24.07", "10.05.54")), Nil).isEmpty)
  }

  test("strict formats reject 99.99.99 and accept 06.24.07") {
    val formats = Seq("yyyy-MM-dd", "HH.mm.ss")
    assert(PartitionPredicate.formatGuard(cols, Seq(Seq("2025-01-07"), Seq("06.24.07")), formats).isEmpty)
    val err = PartitionPredicate.formatGuard(cols, Seq(Seq("2025-01-07"), Seq("06.24.07", "99.99.99")), formats)
    assert(err.exists(m => m.startsWith("HIVE_006") && m.contains("99.99.99")))
    assert(PartitionPredicate.formatGuard(cols, Seq(Seq("2025-13-40"), Seq("06.24.07")), formats)
      .exists(_.contains("2025-13-40")))
  }

  test("attribute names are extracted from a where text") {
    assert(PartitionPredicate.attributeNames("inc_ful_flag IN ('I','F') AND file_date >= '2025-01-01'") ==
      Seq("inc_ful_flag", "file_date"))
    assert(PartitionPredicate.attributeNames("val = 'a'") == Seq("val"))
  }
}
