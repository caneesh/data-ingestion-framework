package com.hcsc.generic.ingest.retention

import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

/** Under `raw.mode = SOURCE` there is no RAW table: the raw purge must
  * resolve no table name (there is no `raw.database`/`raw.table` to resolve)
  * and produce no result row, while `COPY` keeps naming the RAW table. */
class RetentionSourceModeTest extends AnyFunSuite with SharedSparkSession {

  private val logger = Logger.getLogger(getClass.getName)

  test("SOURCE: retention.raw resolves no RAW table and purges nothing") {
    val conf = ConfigFactory.parseString(
      """raw { mode = "SOURCE", record_hash = true }
        |retention { raw = "30d" }""".stripMargin)
    assert(new RetentionService(spark, conf, logger).run(dryRun = true).isEmpty)
  }

  test("COPY (default) still names the RAW table for the raw purge") {
    val conf = ConfigFactory.parseString(
      """raw { database = "rsm_raw", table = "rsm_t" }
        |retention { raw = "30d" }""".stripMargin)
    assert(new RetentionService(spark, conf, logger).run(dryRun = true) ==
      Seq(("rsm_raw.rsm_t", "skipped (absent)", 0L)))
  }

  test("an explicit raw.mode = COPY behaves exactly like the default") {
    val conf = ConfigFactory.parseString(
      """raw { mode = "copy", database = "rsm_raw", table = "rsm_t" }
        |retention { raw = "30d" }""".stripMargin)
    assert(new RetentionService(spark, conf, logger).run(dryRun = true) ==
      Seq(("rsm_raw.rsm_t", "skipped (absent)", 0L)))
  }
}
