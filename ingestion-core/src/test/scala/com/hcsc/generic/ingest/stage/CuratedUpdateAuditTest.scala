package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

/** Incremental merges must keep update-vs-insert audit semantics: an update
  * preserves the original create_timestamp and is stamped 'U'; an insert
  * keeps 'I' with its fresh creation time. */
class CuratedUpdateAuditTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private val service = new CuratedService(spark, ConfigFactory.parseString(
    """database = "audit_db"
      |table = "t"
      |merge { keys = ["id"] }""".stripMargin))

  test("updates inherit the original create_timestamp and are marked 'U'") {
    val originalCreate = Timestamp.valueOf("2025-01-01 08:00:00")
    val newCreate = Timestamp.valueOf("2026-07-27 12:00:00")

    val target = Seq(
      (1, "old-name", originalCreate, "I"),
      (2, "kept-name", originalCreate, "I")
    ).toDF("id", "name", "create_timestamp", "last_modified_op")

    val incoming = Seq(
      (1, "new-name", newCreate, "I"),   // replaces existing key -> update
      (3, "brand-new", newCreate, "I")   // new key -> insert
    ).toDF("id", "name", "create_timestamp", "last_modified_op")

    val audited = service.applyUpdateAudit(incoming, target, Seq("id"))
      .collect().map(r => r.getAs[Int]("id") ->
        ((r.getAs[Timestamp]("create_timestamp"), r.getAs[String]("last_modified_op")))).toMap

    assert(audited(1) == ((originalCreate, "U")),
      "update keeps the ORIGINAL creation timestamp and flips to 'U'")
    assert(audited(3) == ((newCreate, "I")),
      "insert keeps its fresh creation timestamp and stays 'I'")
  }

  test("feeds without the audit columns pass through untouched") {
    val target = Seq((1, "a")).toDF("id", "name")
    val incoming = Seq((1, "b"), (2, "c")).toDF("id", "name")
    val out = service.applyUpdateAudit(incoming, target, Seq("id"))
    assert(out.columns.toSeq == Seq("id", "name"))
    assert(out.count() == 2)
  }
}
