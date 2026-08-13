package com.hcsc.generic.ingest.schema

import com.hcsc.generic.ingest.transform.{CuratedTransform, SharedSparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/**
  * Plan SHAPE for wide feeds.
  *
  * The 364-column production feed exhausted the driver heap during analysis,
  * before a single row was read. Cause: renaming columns in a fold adds one
  * Spark Project per column, and every Project carries the full attribute
  * list — so an N-column feed built an N-deep plan holding N*N attribute
  * references (~130,000 at 364).
  *
  * Correct output is necessary but not sufficient here: the fold produced
  * correct output too. These tests assert the PLAN DEPTH stays constant as
  * the feed widens, because that is the property that actually failed.
  */
class WideFeedPlanDepthTest extends AnyFunSuite with SharedSparkSession {

  private def wideFrame(n: Int) = {
    val fields = (1 to n).map(i => StructField(s"Src_Col_$i", StringType))
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row((1 to n).map(i => s"v$i"): _*))),
      StructType(fields))
  }

  /** Nodes in the ANALYZED plan — what the driver actually builds and holds. */
  private def planNodes(df: org.apache.spark.sql.DataFrame): Int =
    df.queryExecution.analyzed.collect { case p => p }.size

  test("renaming every column keeps plan depth constant as the feed widens") {
    def depthFor(n: Int) = {
      val df = wideFrame(n)
      val renames = (1 to n).map(i => s"Src_Col_$i" -> s"tgt_col_$i")
      planNodes(ColumnMapping.applyRenames(df, renames))
    }
    val narrow = depthFor(10)
    val wide = depthFor(300)
    assert(wide == narrow,
      s"plan depth must not grow with column count (10 cols -> $narrow nodes, " +
        s"300 cols -> $wide). A per-column fold is what exhausted the driver heap.")
  }

  test("renames still produce the right names, values and order") {
    val df = wideFrame(5)
    val renamed = ColumnMapping.applyRenames(df,
      Seq("Src_Col_2" -> "b", "Src_Col_4" -> "d"))
    assert(renamed.columns.toSeq == Seq("Src_Col_1", "b", "Src_Col_3", "d", "Src_Col_5"),
      "renamed in place, order preserved, untouched columns unchanged")
    val row = renamed.collect().head
    assert(row.getString(1) == "v2" && row.getString(3) == "v4", "values follow their column")
  }

  test("renames match case-insensitively, as withColumnRenamed did") {
    val renamed = ColumnMapping.applyRenames(wideFrame(3), Seq("src_col_2" -> "b"))
    assert(renamed.columns.toSeq == Seq("Src_Col_1", "b", "Src_Col_3"))
  }

  test("a rename naming a column that is not present is ignored, not an error") {
    val renamed = ColumnMapping.applyRenames(wideFrame(2), Seq("absent" -> "x"))
    assert(renamed.columns.toSeq == Seq("Src_Col_1", "Src_Col_2"))
  }

  test("an empty rename list returns the frame untouched") {
    val df = wideFrame(3)
    assert(ColumnMapping.applyRenames(df, Seq.empty).columns.toSeq == df.columns.toSeq)
  }

  test("aligning to a wide target keeps plan depth constant too") {
    // align() previously added each MISSING column with its own withColumn.
    def depthFor(n: Int) = {
      val df = wideFrame(2)   // only two of the target's columns are present
      val target = StructType(
        (1 to 2).map(i => StructField(s"src_col_$i", StringType)) ++
        (1 to n).map(i => StructField(s"missing_$i", StringType)))
      planNodes(new CuratedTransform(spark).align(df, target, None))
    }
    val few = depthFor(5)
    val many = depthFor(200)
    assert(many == few,
      s"align must not add a Project per missing column (5 missing -> $few nodes, " +
        s"200 missing -> $many)")
  }

  test("align still null-fills missing columns and keeps present values") {
    val df = wideFrame(2)
    val target = StructType(Seq(
      StructField("src_col_1", StringType),
      StructField("src_col_2", StringType),
      StructField("absent_one", StringType)))
    val aligned = new CuratedTransform(spark).align(df, target, None)
    assert(aligned.columns.toSeq == Seq("src_col_1", "src_col_2", "absent_one"))
    val row = aligned.collect().head
    assert(row.getString(0) == "v1" && row.getString(1) == "v2")
    assert(row.isNullAt(2), "a target column absent from the source is null-filled")
  }
}
