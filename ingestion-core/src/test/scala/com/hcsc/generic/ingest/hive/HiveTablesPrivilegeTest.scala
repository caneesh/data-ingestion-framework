package com.hcsc.generic.ingest.hive

import com.hcsc.generic.ingest.transform.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
  * Control tables must be creatable inside a database the job did not
  * create and cannot create.
  *
  * Many sites hand a pipeline write access to one shared database and
  * nothing more. `CREATE DATABASE IF NOT EXISTS` looks harmless — Hive
  * treats it as a no-op when the database exists — but an authorizer
  * (Ranger, SQL-standard auth) evaluates the CREATE privilege *before*
  * reaching the IF-NOT-EXISTS, so the statement fails on a database that is
  * already there. The framework must therefore not issue it at all in that
  * case.
  *
  * The local metastore cannot reproduce a privilege denial, so the property
  * is asserted structurally: the statement is not issued.
  */
class HiveTablesPrivilegeTest extends AnyFunSuite with SharedSparkSession {

  private val db = "preexisting_shared_db"

  test("no CREATE DATABASE is issued when the database already exists") {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
    HiveTables.resetEnsureCache()

    val statements = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(name: String, qe: org.apache.spark.sql.execution.QueryExecution,
                             durationNs: Long): Unit =
        statements.add(qe.logical.getClass.getSimpleName)
      override def onFailure(name: String, qe: org.apache.spark.sql.execution.QueryExecution,
                             e: Exception): Unit = ()
    }
    spark.listenerManager.register(listener)
    try {
      HiveTables.ensure(spark, db, s"$db.ctl_one", "a STRING, b BIGINT")
      // The listener bus is asynchronous and its flush hook is private to
      // Spark, so wait for the CREATE TABLE event to land. Events are
      // ordered, so once it appears any CREATE DATABASE would already have.
      val deadline = System.currentTimeMillis() + 15000
      import scala.collection.JavaConverters._
      // Spark names it CreateDataSourceTableCommand for a USING ORC table,
      // so match the family rather than one exact class.
      def seenCreateTable = statements.asScala.exists { n =>
        val l = n.toLowerCase
        l.contains("createtable") || l.contains("createdatasourcetable")
      }
      while (!seenCreateTable && System.currentTimeMillis() < deadline) Thread.sleep(50)
      assert(seenCreateTable, s"expected a CREATE TABLE event, saw: ${statements.asScala.toList}")
    } finally spark.listenerManager.unregister(listener)
    import scala.collection.JavaConverters._
    val seen = statements.asScala.toList

    assert(spark.catalog.tableExists(s"$db.ctl_one"),
      "the control table must still be created inside the existing database")
    assert(!seen.exists(n => n.toLowerCase.contains("createdatabase") ||
                             n.toLowerCase.contains("createnamespace")),
      s"CREATE DATABASE must not be issued for an existing database — a site that " +
        s"cannot create databases would fail on a statement with nothing to do. Saw: $seen")
  }

  test("a genuinely absent database is still created") {
    val fresh = "absent_db_created_on_demand"
    spark.sql(s"DROP DATABASE IF EXISTS $fresh CASCADE")
    HiveTables.resetEnsureCache()

    HiveTables.ensure(spark, fresh, s"$fresh.ctl_two", "a STRING")
    assert(spark.catalog.databaseExists(fresh),
      "where the framework CAN create the database, it still must — the " +
        "zero-setup path for sites without this restriction is unchanged")
    assert(spark.catalog.tableExists(s"$fresh.ctl_two"))
  }
}
