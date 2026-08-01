package com.hcsc.generic.ingest.schema

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/** Reads and records the schema contract version on the RAW table
  * (table property `ingest.schema.version`), shared by IngestPipeline and
  * the legacy RawStageRunner path. */
object SchemaVersions {
  val Property = "ingest.schema.version"

  /** Snapshot of the last-written contract's REQUIRED columns (name:type,
    * ';'-joined) — the baseline BACKWARD-compatibility is enforced against. */
  val RequiredProperty = "ingest.schema.required"

  private def validate(name: String, label: String): String =
    com.hcsc.generic.ingest.config.ConfigUtils.requireSqlIdentifier(name, label)

  private def property(spark: SparkSession, fullTable: String, key: String): Option[String] =
    spark.sql(s"SHOW TBLPROPERTIES $fullTable")
      .collect()
      .collectFirst { case row if row.getString(0) == key => row.getString(1) }

  def stored(spark: SparkSession, database: String, table: String): Option[String] = {
    val fullTable = s"${validate(database, "database")}.${validate(table, "table")}"
    if (!spark.catalog.tableExists(fullTable)) None
    else property(spark, fullTable, Property)
  }

  /** The ';' and ':' separators are part of the wire format, so column
    * names/types containing them must be escaped — an unescaped ';' in a
    * name would shear the snapshot and manufacture false BACKWARD breaks.
    * Percent-encoding keeps legacy snapshots (no '%') parsing unchanged. */
  private def escapeField(v: String): String =
    v.replace("%", "%25").replace(";", "%3B").replace(":", "%3A")

  private def unescapeField(v: String): String =
    v.replace("%3A", ":").replace("%3B", ";").replace("%25", "%")

  private[schema] def parseRequiredSnapshot(s: String): Map[String, String] =
    s.split(";").filter(_.nonEmpty).flatMap { entry =>
      entry.split(":", 2) match {
        case Array(n, t) => Some(unescapeField(n) -> unescapeField(t))
        case _ => None
      }
    }.toMap

  /** The previously recorded required-column snapshot: name -> declared type. */
  def storedRequired(spark: SparkSession, database: String, table: String): Option[Map[String, String]] = {
    val fullTable = s"${validate(database, "database")}.${validate(table, "table")}"
    if (!spark.catalog.tableExists(fullTable)) None
    else property(spark, fullTable, RequiredProperty).map(parseRequiredSnapshot)
  }

  def requiredSnapshot(contract: SchemaContract): String =
    contract.requiredColumns.map(c => s"${escapeField(c.name)}:${escapeField(c.dataType)}").mkString(";")

  /** Best-effort: the version property is metadata, so a failure here must
    * not fail a run whose data write already committed. */
  def record(spark: SparkSession, database: String, table: String, version: String, logger: Logger): Unit =
    record(spark, database, table, version, None, logger)

  def record(
    spark: SparkSession,
    database: String,
    table: String,
    version: String,
    contract: Option[SchemaContract],
    logger: Logger
  ): Unit = {
    val fullTable = s"${validate(database, "database")}.${validate(table, "table")}"
    def escape(v: String) = v.replace("'", "''")
    val desired: Seq[(String, String)] = Seq(Property -> version) ++
      contract.map(c => RequiredProperty -> requiredSnapshot(c))
    // Steady state is "already recorded": one metastore READ to confirm
    // beats an unconditional ALTER (a metastore write + table lock) on
    // every raw publish.
    val unchanged =
      try {
        val existing = spark.sql(s"SHOW TBLPROPERTIES $fullTable").collect()
          .map(r => r.getString(0) -> r.getString(1)).toMap
        desired.forall { case (k, v) => existing.get(k).contains(v) }
      } catch { case _: Exception => false }
    if (unchanged) return
    val props = desired.map { case (k, v) => s"'$k'='${escape(v)}'" }.mkString(", ")
    try {
      spark.sql(s"ALTER TABLE $fullTable SET TBLPROPERTIES ($props)")
    } catch {
      case e: Exception =>
        logger.warn(s"[SchemaVersions] Could not record schema version '$version' on $fullTable: ${e.getMessage}. " +
          s"Update manually with ALTER TABLE $fullTable SET TBLPROPERTIES ($props)")
    }
  }
}
