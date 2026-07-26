package com.hcsc.generic.ingest.reject

import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.SchemaContract
import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

class RejectServiceTest extends AnyFunSuite with SharedSparkSession {

  private val logger = Logger.getLogger(getClass.getName)
  // dryRun avoids persisting rejects to a table; counts are still computed.
  private val ctx = RunContext("run-1", "member", "FULL", "F", dryRun = true)

  import spark.implicits._

  private def df = Seq(
    ("S001", "H1", "f1", "file1.csv", 0L),
    ("", "H2", "f1", "file1.csv", 1L),
    ("S003", null.asInstanceOf[String], "f1", "file1.csv", 2L)
  ).toDF("subscriber_id", "hios_id", "file_id", "source_file", "row_idx")

  private def service(hocon: String, contractHocon: Option[String] = None) = {
    val contract = contractHocon.flatMap(c => SchemaContract.parse(ConfigFactory.parseString(c)))
    new RejectService(spark, Some(ConfigFactory.parseString(hocon)), contract, logger)
  }

  test("disabled or rule-less service passes everything through") {
    val none = new RejectService(spark, None, None, logger)
    assert(!none.enabled)
    assert(none.split(df, ctx).rejectedCount == 0L)

    val noRules = service("""database = ingest_audit""")
    assert(noRules.split(df, ctx).rejectedCount == 0L)
  }

  test("configured rules split accepted and rejected") {
    val svc = service(
      """
        |database = ingest_audit
        |rules = [
        |  { name = "blank_subscriber", condition = "subscriber_id IS NULL OR trim(subscriber_id) = ''",
        |    error_code = "R001", category = "MISSING_KEY", message = "subscriber_id required" }
        |]
      """.stripMargin)

    val result = svc.split(df, ctx)
    assert(result.acceptedCount == 2L)
    assert(result.rejectedCount == 1L)
    assert(result.accepted.select("subscriber_id").as[String].collect().toSet == Set("S001", "S003"))
  }

  test("contract nullability generates reject rules") {
    val svc = service(
      """
        |database = ingest_audit
        |use_contract_nullability = true
      """.stripMargin,
      Some(
        """schema {
          |  version = "1.0"
          |  columns = [
          |    { name = "subscriber_id", nullable = false },
          |    { name = "hios_id", nullable = false }
          |  ]
          |}""".stripMargin)
    )

    val result = svc.split(df, ctx)
    // blank subscriber row and null hios row both rejected
    assert(result.rejectedCount == 2L)
    assert(result.acceptedCount == 1L)
  }

  test("rule conditions evaluating to NULL do not drop rows (three-valued logic)") {
    // "hios_id > 'H1'" is NULL for the null-hios row; without coalescing the
    // combined predicate that row would match neither accepted nor rejected.
    val svc = service(
      """
        |database = ingest_audit
        |rules = [
        |  { name = "hios_after_h1", condition = "hios_id > 'H1'" }
        |]
      """.stripMargin)

    val result = svc.split(df, ctx)
    assert(result.acceptedCount + result.rejectedCount == 3L, "no row may vanish")
    assert(result.rejectedCount == 1L)
    assert(result.accepted.select("subscriber_id").as[String].collect().toSet == Set("S001", "S003"))
  }

  test("max_reject_count threshold fails the run") {
    val svc = service(
      """
        |database = ingest_audit
        |max_reject_count = 0
        |rules = [
        |  { name = "blank_subscriber", condition = "trim(coalesce(subscriber_id, '')) = ''" }
        |]
      """.stripMargin)

    intercept[RejectThresholdExceededException] { svc.split(df, ctx) }
  }

  test("max_reject_percent threshold fails the run") {
    val svc = service(
      """
        |database = ingest_audit
        |max_reject_percent = 10.0
        |rules = [
        |  { name = "blank_subscriber", condition = "trim(coalesce(subscriber_id, '')) = ''" }
        |]
      """.stripMargin)

    // 1 of 3 rejected = 33% > 10%
    intercept[RejectThresholdExceededException] { svc.split(df, ctx) }
  }
}
