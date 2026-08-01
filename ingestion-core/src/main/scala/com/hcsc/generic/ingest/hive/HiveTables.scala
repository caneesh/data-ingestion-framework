package com.hcsc.generic.ingest.hive

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * The framework's control tables (audit, rejects, file registry, watermarks)
  * all follow one write discipline: CREATE DATABASE/TABLE IF NOT EXISTS then
  * append. The IF-NOT-EXISTS pair makes concurrent first runs race safely at
  * the metastore instead of one saveAsTable(Overwrite) wiping the other; this
  * helper is that discipline's single home.
  *
  * Callers must pass pre-validated identifiers (ConfigUtils.requireSqlIdentifier
  * or equivalent) — this helper interpolates them verbatim, exactly as the
  * call sites it replaced did.
  */
object HiveTables {

  /** Tables already ensured by THIS JVM: every audit/reject/watermark append
    * routes through ensure(), and the two IF-NOT-EXISTS statements are pure
    * metastore overhead after the first call. Existence never regresses
    * mid-run (nothing drops control tables), so a per-JVM cache is safe;
    * a concurrent external drop would surface at the append exactly as it
    * did before this cache. */
  private val ensured = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()

  /** Test hook: forget the ensure cache (e.g. after a suite drops tables). */
  private[hive] def resetEnsureCache(): Unit = ensured.clear()

  /** Ensures `database` and `fullTable` exist (ORC, `columnsDdl` schema). */
  def ensure(spark: SparkSession, database: String, fullTable: String, columnsDdl: String): Unit = {
    // Scoped by application id: a later SparkContext in the same JVM (test
    // forks, restarted sessions) has its own metastore and must re-ensure.
    val key = s"${spark.sparkContext.applicationId}:$fullTable"
    if (ensured.contains(key)) return
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $fullTable ($columnsDdl) USING ORC")
    ensured.add(key)
  }

  /** Ensures `database` and `fullTable` exist (ORC, `columnsDdl` schema) and
    * appends `rows` positionally via insertInto. */
  def appendEnsuringTable(
    spark: SparkSession,
    database: String,
    fullTable: String,
    columnsDdl: String,
    rows: DataFrame
  ): Unit = {
    ensure(spark, database, fullTable, columnsDdl)
    rows.write.mode(SaveMode.Append).insertInto(fullTable)
  }
}
