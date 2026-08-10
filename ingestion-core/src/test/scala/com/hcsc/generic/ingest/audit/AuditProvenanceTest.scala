package com.hcsc.generic.ingest.audit

import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus, Stages}
import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/**
  * Provenance columns and the ledger query that restores the accounting
  * identity to decoupled curated replays.
  *
  * The fingerprint deliberately hashes the config's KEY STRUCTURE and not
  * its values: feed configs carry hostnames, database names and account
  * identifiers, and this column lands in a table with a wide readership.
  * "The shape of the configuration changed" is what provenance needs; the
  * values themselves must not be republished, even as a digest that could
  * be confirmed by guessing.
  */
class AuditProvenanceTest extends AnyFunSuite with SharedSparkSession {

  private val db = "prov_audit"
  private val feedConf = ConfigFactory.parseString(
    s"""audit { database = "$db", run_table = "run_audit" }
       |source { url = "jdbc:sqlserver://host-a;databaseName=DB1", type = "jdbc" }
       |curated { database = "c", table = "t" }""".stripMargin)
  private val audit = AuditService(spark, feedConf)

  private def ctx(runId: String) = RunContext(runId, "prov_feed", "INCR", "I")

  locally {
    purgeWarehouseDb(db)
    spark.sql(s"DROP TABLE IF EXISTS $db.run_audit")

    audit.recordStage(ctx("P1"), Stages.Raw, StageStatus.Success,
      counts = StageCounts(acceptedCount = 100L, rawCount = 97L))
    audit.recordStage(ctx("DRY"), Stages.Raw, StageStatus.Success,
      counts = StageCounts(rawCount = 5L), message = "dry-run")
    audit.recordStage(ctx("NOCOUNT"), Stages.Raw, StageStatus.Success)
  }

  test("every ledger row records which build, which config and whose identity") {
    val r = spark.table(s"$db.run_audit")
      .filter(org.apache.spark.sql.functions.col("run_id") === "P1")
      .collect().head

    assert(Option(r.getAs[String]("framework_version")).exists(_.nonEmpty),
      "framework_version must always be populated ('unknown' outside a jar)")
    assert(Option(r.getAs[String]("principal")).exists(_.nonEmpty),
      "principal must always be populated")

    val fp = r.getAs[String]("config_fingerprint")
    assert(fp != null && fp.startsWith("v1:"),
      s"config_fingerprint must be versioned so the recipe can change later, got '$fp'")
  }

  test("the fingerprint tracks config STRUCTURE and never leaks values") {
    val base = ConfigFactory.parseString(
      """a.b = "secret-host-1"
        |a.c = 5""".stripMargin)
    val sameShapeDifferentValues = ConfigFactory.parseString(
      """a.b = "totally-different-host"
        |a.c = 99""".stripMargin)
    val differentShape = ConfigFactory.parseString(
      """a.b = "secret-host-1"
        |a.c = 5
        |a.d = true""".stripMargin)

    assert(AuditService.fingerprint(base) == AuditService.fingerprint(sameShapeDifferentValues),
      "values must not affect the fingerprint — they are not hashed at all")
    assert(AuditService.fingerprint(base) != AuditService.fingerprint(differentShape),
      "an added key IS a configuration change and must be visible")

    // The point of hashing structure only: no value, and no digest of a
    // value, can be recovered from or confirmed against the column.
    val fp = AuditService.fingerprint(base)
    assert(!fp.contains("secret-host-1"))
    val valueDigest = java.security.MessageDigest.getInstance("SHA-256")
      .digest("secret-host-1".getBytes("UTF-8")).map("%02x".format(_)).mkString
    assert(!fp.contains(valueDigest.take(16)),
      "a value's digest must not appear either — that would be guessable")
  }

  test("rawRowCount gives the decoupled replay an INDEPENDENT expectation") {
    assert(audit.rawRowCount("P1", "prov_feed").contains(97L),
      "the raw run's own recorded write count is what the replay must account for")
  }

  test("rawRowCount refuses to guess where it cannot know") {
    assert(audit.rawRowCount("DRY", "prov_feed").isEmpty,
      "a dry run wrote nothing; its count must never become an expectation")
    assert(audit.rawRowCount("NOCOUNT", "prov_feed").isEmpty,
      "an unmeasured count (-1) must yield None, not a check that always fails")
    assert(audit.rawRowCount("MISSING", "prov_feed").isEmpty,
      "an unknown run yields None so the identity is skipped, not failed")
  }
}
