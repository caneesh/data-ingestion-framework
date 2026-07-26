package com.hcsc.generic.ingest.publish

import com.hcsc.generic.ingest.runtime.RunContext
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

final case class PublishRequest(
  database: String,
  table: String,
  format: String,
  path: Option[String],
  expectedCount: Option[Long] = None,
  allowEmpty: Boolean = false,
  validationQuery: Option[String] = None
)

final case class PublishResult(publishedCount: Long, stagingTable: String)

final class PublishValidationException(message: String) extends RuntimeException(message)

/**
  * Transactional (staging-based) publication:
  *
  * 1. Stale staging tables from crashed prior runs are dropped.
  * 2. The DataFrame is materialized into a staging table.
  * 3. The staging table is validated BEFORE the target is touched:
  *    non-empty (unless allow_empty), expected count, and an optional
  *    validation query ({table} placeholder; any returned row = failure).
  * 4. Only after validation passes is the target overwritten in a single
  *    INSERT OVERWRITE statement.
  * 5. On any failure the staging table is dropped and the target remains
  *    exactly as before (rollback = do nothing destructive early).
  */
final class PublishService(spark: SparkSession, logger: Logger) {

  private def stagingPrefix(table: String) = s"__stg_${table}_"

  def publish(df: DataFrame, req: PublishRequest, ctx: RunContext): PublishResult = {
    require(req.database.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"database '${req.database}' is not a safe SQL identifier")
    require(req.table.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"table '${req.table}' is not a safe SQL identifier")

    val fullTable = s"${req.database}.${req.table}"
    val stagingTable = s"${req.database}.${stagingPrefix(req.table)}${ctx.runId.replace("-", "_")}"

    cleanupStaleStaging(req.database, req.table, stagingTable)

    if (ctx.dryRun) {
      val count = df.count()
      logger.info(s"[Publish] DRY-RUN: would publish $count rows to $fullTable")
      return PublishResult(count, stagingTable)
    }

    logger.info(s"[Publish] Materializing staging table $stagingTable")
    df.write.format(req.format).mode(SaveMode.Overwrite).saveAsTable(stagingTable)
    // Stamp the creation time so cleanupStaleStaging can distinguish a
    // concurrent run's live staging table from a crashed run's leftover.
    spark.sql(s"ALTER TABLE $stagingTable SET TBLPROPERTIES " +
      s"('${PublishService.StagingCreatedProperty}'='${System.currentTimeMillis()}')")

    try {
      val stagedCount = spark.table(stagingTable).count()
      validate(stagingTable, stagedCount, req)

      if (spark.catalog.tableExists(fullTable)) {
        spark.sql(s"INSERT OVERWRITE TABLE $fullTable SELECT * FROM $stagingTable")
      } else {
        val writer = spark.table(stagingTable).write.format(req.format).mode(SaveMode.Overwrite)
        req.path.fold(writer)(p => writer.option("path", p)).saveAsTable(fullTable)
      }
      logger.info(s"[Publish] Published $stagedCount rows to $fullTable")
      PublishResult(stagedCount, stagingTable)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $stagingTable")
    }
  }

  private def validate(stagingTable: String, stagedCount: Long, req: PublishRequest): Unit = {
    if (stagedCount == 0 && !req.allowEmpty)
      throw new PublishValidationException(
        s"Staging table $stagingTable is empty and allow_empty=false; publish aborted, target untouched"
      )

    req.expectedCount.foreach { expected =>
      if (stagedCount != expected)
        throw new PublishValidationException(
          s"Staging count $stagedCount does not match expected $expected; publish aborted, target untouched"
        )
    }

    req.validationQuery.foreach { template =>
      val query = template.replace("{table}", stagingTable)
      val failures = spark.sql(query).count()
      if (failures > 0)
        throw new PublishValidationException(
          s"Publish validation query returned $failures failing row(s); publish aborted, target untouched. Query: $query"
        )
    }
  }

  /** Drops leftover staging tables from crashed runs of the same target.
    * Tables younger than [[PublishService.StaleStagingAgeMillis]] are presumed
    * to belong to a concurrent live run and are left alone — dropping another
    * run's staging table mid-publish would fail that run. */
  private def cleanupStaleStaging(database: String, table: String, currentStaging: String): Unit = {
    if (!spark.catalog.databaseExists(database)) return
    val prefix = stagingPrefix(table)
    val candidates = spark.catalog.listTables(database).collect()
      .map(_.name)
      .filter(name =>
        name.startsWith(prefix) &&
          name.matches("[a-zA-Z_][a-zA-Z0-9_]*") && // never interpolate an unvalidated name
          s"$database.$name" != currentStaging)
    candidates.foreach { name =>
      if (isStale(database, name)) {
        logger.warn(s"[Publish] Dropping stale staging table $database.$name")
        spark.sql(s"DROP TABLE IF EXISTS `$database`.`$name`")
      } else {
        logger.info(s"[Publish] Staging table $database.$name looks live (younger than " +
          s"${PublishService.StaleStagingAgeMillis / 3600000}h); leaving it for its own run")
      }
    }
  }

  /** A staging table is stale when its creation stamp is older than the
    * threshold. A missing/unparseable stamp means a leftover from a version
    * without stamping — treated as stale, matching the previous behavior. */
  private def isStale(database: String, name: String): Boolean = {
    val created = spark.sql(s"SHOW TBLPROPERTIES `$database`.`$name`").collect()
      .collectFirst {
        case row if row.getString(0) == PublishService.StagingCreatedProperty => row.getString(1)
      }
      .flatMap(v => scala.util.Try(v.toLong).toOption)
    created.forall(ts => System.currentTimeMillis() - ts > PublishService.StaleStagingAgeMillis)
  }
}

object PublishService {
  /** Table property stamping a staging table's creation time (epoch millis). */
  val StagingCreatedProperty = "ingest.staging.created"

  /** Staging tables younger than this are presumed live and never cleaned up. */
  val StaleStagingAgeMillis: Long = 24L * 60 * 60 * 1000
}
