package com.hcsc.generic.ingest.jdbc

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager

/** PREDICATES and MIN_MAX_QUERY partitioning strategies end to end, plus
  * the structured query model against a live H2 database. */
class PartitionStrategiesH2Test extends AnyFunSuite with SharedSparkSession with BeforeAndAfterAll {

  private val url = "jdbc:h2:mem:partition_tests;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"

  override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("DROP TABLE IF EXISTS orders")
        stmt.execute("CREATE TABLE orders (order_id INT, region VARCHAR(10), amount INT)")
        (1 to 20).foreach { i =>
          val region = if (i % 2 == 0) "EAST" else "WEST"
          stmt.execute(s"INSERT INTO orders VALUES ($i, '$region', ${i * 10})")
        }
      } finally stmt.close()
    } finally conn.close()
  }

  private def conf(extra: String): Config = ConfigFactory.parseString(
    s"""
       |type = "jdbc"
       |url = "$url"
       |dialect = "generic"
       |driver = "org.h2.Driver"
       |auth { user = "sa", password = { provider = "inline", value = "" } }
       |health_check { enabled = false }
       |$extra
    """.stripMargin)

  test("PREDICATES strategy: one partition per predicate, complete data") {
    val df = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "orders"
        |partition_strategy = "PREDICATES"
        |partition_predicates = ["region = 'EAST'", "region = 'WEST'"]
      """.stripMargin))
    assert(df.rdd.getNumPartitions == 2)
    assert(df.count() == 20)
    assert(df.selectExpr("order_id").collect().map(_.getInt(0)).toSet == (1 to 20).toSet)
  }

  test("MIN_MAX_QUERY strategy discovers bounds and partitions the read") {
    val df = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "orders"
        |partition_strategy = "MIN_MAX_QUERY"
        |partitionColumn = "order_id"
        |numPartitions = 4
      """.stripMargin))
    assert(df.rdd.getNumPartitions == 4)
    assert(df.count() == 20)
  }

  test("MIN_MAX_QUERY honors the where clause and degrades gracefully") {
    // Bounds discovered over the filtered set only
    val filtered = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "orders"
        |where = "region = 'EAST'"
        |partition_strategy = "MIN_MAX_QUERY"
        |partitionColumn = "order_id"
        |numPartitions = 2
      """.stripMargin))
    assert(filtered.count() == 10)

    // Degenerate range (single row) -> unpartitioned read, still correct
    val single = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "orders"
        |where = "order_id = 5"
        |partition_strategy = "MIN_MAX_QUERY"
        |partitionColumn = "order_id"
        |numPartitions = 4
      """.stripMargin))
    assert(single.count() == 1)
    assert(single.rdd.getNumPartitions == 1)
  }

  test("structured projections and filters render through the dialect end to end") {
    val df = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "orders"
        |columns = [
        |  { source = "order_id", target = "id" },
        |  { expression = "amount * 2", target = "doubled" }
        |]
        |filters = [
        |  { column = "region", operator = "=", value = "EAST" },
        |  { column = "amount", operator = ">=", value = "100", type = "NUMBER" }
        |]
      """.stripMargin))
    assert(df.columns.map(_.toLowerCase).toSet == Set("id", "doubled"))
    val rows = df.collect()
    assert(rows.nonEmpty)
    assert(rows.forall(_.getInt(1) >= 200)) // amount >= 100 doubled
  }

  test("SQL_TEMPLATE mode renders typed parameters into the pushed-down query") {
    val df = JdbcSource.read(spark, conf(
      """
        |mode = "SQL_TEMPLATE"
        |sql = "SELECT order_id, amount FROM orders WHERE region = :region AND amount >= :floor"
        |parameters = [
        |  { name = "region", type = "STRING", value = "WEST" },
        |  { name = "floor", type = "NUMBER", value = "150" }
        |]
      """.stripMargin))
    val ids = df.selectExpr("order_id").collect().map(_.getInt(0)).toSet
    assert(ids.forall(_ % 2 == 1)) // WEST = odd ids
    assert(df.collect().forall(_.getInt(1) >= 150))
  }

  test("FULL_TABLE rejects ignored projections/predicates at parse time") {
    val ex = intercept[IllegalArgumentException] {
      JdbcSource.read(spark, conf(
        """
          |mode = "FULL_TABLE"
          |table = "orders"
          |where = "region = 'EAST'"
        """.stripMargin))
    }
    assert(ex.getMessage.contains("FULL_TABLE mode does not apply"))
  }

  test("predicate/range partitioning configs are mutually exclusive") {
    val ex = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |table = "orders"
          |partition_strategy = "PREDICATES"
          |partition_predicates = ["a = 1"]
          |numPartitions = 4
        """.stripMargin))
    }
    assert(ex.getMessage.contains("exclusive"))
  }

  test("skew metrics logging runs without disturbing results") {
    val df = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "orders"
        |partition_strategy = "PREDICATES"
        |partition_predicates = ["order_id <= 15", "order_id > 15"]
        |skew_metrics = true
      """.stripMargin))
    assert(df.count() == 20)
  }
}
