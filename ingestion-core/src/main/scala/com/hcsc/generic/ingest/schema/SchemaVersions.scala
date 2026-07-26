package com.hcsc.generic.ingest.schema

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/** Reads and records the schema contract version on the RAW table
  * (table property `ingest.schema.version`), shared by IngestPipeline and
  * the legacy RawStageRunner path. */
object SchemaVersions {
  val Property = "ingest.schema.version"

  private def validate(name: String, label: String): String = {
    require(name.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"$label '$name' is not a safe SQL identifier")
    name
  }

  def stored(spark: SparkSession, database: String, table: String): Option[String] = {
    val fullTable = s"${validate(database, "database")}.${validate(table, "table")}"
    if (!spark.catalog.tableExists(fullTable)) None
    else
      spark.sql(s"SHOW TBLPROPERTIES $fullTable")
        .collect()
        .collectFirst { case row if row.getString(0) == Property => row.getString(1) }
  }

  /** Best-effort: the version property is metadata, so a failure here must
    * not fail a run whose data write already committed. */
  def record(spark: SparkSession, database: String, table: String, version: String, logger: Logger): Unit = {
    val fullTable = s"${validate(database, "database")}.${validate(table, "table")}"
    val escaped = version.replace("'", "''")
    try {
      spark.sql(s"ALTER TABLE $fullTable SET TBLPROPERTIES ('$Property'='$escaped')")
    } catch {
      case e: Exception =>
        logger.warn(s"[SchemaVersions] Could not record schema version '$version' on $fullTable: ${e.getMessage}. " +
          s"Update manually with ALTER TABLE $fullTable SET TBLPROPERTIES ('$Property'='$escaped')")
    }
  }
}
