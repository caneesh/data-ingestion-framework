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

  /** Ensures `database` and `fullTable` exist (ORC, `columnsDdl` schema). */
  def ensure(spark: SparkSession, database: String, fullTable: String, columnsDdl: String): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $fullTable ($columnsDdl) USING ORC")
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
