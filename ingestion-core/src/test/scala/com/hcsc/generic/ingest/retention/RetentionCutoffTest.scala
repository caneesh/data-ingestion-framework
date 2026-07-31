package com.hcsc.generic.ingest.retention

import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, ZoneOffset}

/** The retention cutoff must follow the SPARK SESSION time zone (the zone
  * the pipeline stamps timestamps in), never the JVM default — a non-UTC
  * edge node would otherwise shift every cutoff by its offset. */
class RetentionCutoffTest extends AnyFunSuite with SharedSparkSession {

  private val logger = Logger.getLogger(getClass.getName)
  private val conf = ConfigFactory.parseString("""retention { raw = "365d" }""")

  test("cutoff dates are computed in the session time zone, not the JVM zone") {
    // Pick a zone whose calendar date differs from the JVM's right now; the
    // 26-hour spread of UTC offsets guarantees at least one always exists.
    val jvmToday = LocalDate.now()
    val divergent = (-12 to 14).map(ZoneOffset.ofHours)
      .find(z => LocalDate.now(z) != jvmToday)
      .getOrElse(fail("no UTC offset with a different calendar date — impossible"))

    val prev = spark.conf.get("spark.sql.session.timeZone")
    try {
      spark.conf.set("spark.sql.session.timeZone", divergent.getId)
      val svc = new RetentionService(spark, conf, logger)
      assert(svc.cutoffDate(0) == LocalDate.now(divergent),
        "cutoff must be today in the SESSION zone")
      assert(svc.cutoffDate(0) != jvmToday,
        "cutoff must not silently follow the JVM default zone")
      assert(svc.cutoffDate(10) == LocalDate.now(divergent).minusDays(10))
    } finally spark.conf.set("spark.sql.session.timeZone", prev)
  }
}
