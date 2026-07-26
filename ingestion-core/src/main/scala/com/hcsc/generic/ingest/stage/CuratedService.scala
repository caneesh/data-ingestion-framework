package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.publish.{PublishRequest, PublishService}
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.transform.CuratedTransform
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}

import java.util.UUID

final case class CuratedResult(
  publishedCount: Long,
  insertCount: Long,
  updateCount: Long,
  deleteCount: Long
)

final class CuratedService(spark: SparkSession, conf: Config) {
  private val logger = Logger.getLogger(getClass.getName)
  private val transform = new CuratedTransform(spark)
  private val publisher = new PublishService(spark, logger)

  val enabled: Boolean =
    ConfigUtils.optBoolean(conf, "enabled").getOrElse(true)

  /** Legacy entry point; runs with a synthetic run context. */
  def process(rawDf: DataFrame, runMode: String): Option[CuratedResult] =
    process(rawDf, runMode, RunContext(UUID.randomUUID().toString, "unknown", runMode, "F"))

  def process(rawDf: DataFrame, runMode: String, ctx: RunContext): Option[CuratedResult] = {
    if (!enabled) return None

    val database = ConfigUtils.sqlIdentifier(conf, "database")
    val table = ConfigUtils.sqlIdentifier(conf, "table")
    val fullTable = s"$database.$table"
    val path = ConfigUtils.optString(conf, "path")
    val format = ConfigUtils.optString(conf, "format").getOrElse("orc")

    val prepared0 = transform.castConfigured(rawDf, conf)
    val prepared1 = transform.applyTransforms(prepared0, conf)
    val prepared2 = transform.ensureAudit(prepared1)
    val prepared = transform.normalizeKeys(prepared2, conf)

    val publishConf = ConfigUtils.optConfig(conf, "publish")
    val request = PublishRequest(
      database = database,
      table = table,
      format = format,
      path = path,
      allowEmpty = publishConf.flatMap(p => ConfigUtils.optBoolean(p, "allow_empty")).getOrElse(false),
      validationQuery = publishConf.flatMap(p => ConfigUtils.optString(p, "validation_query"))
    )

    val result =
      if (runMode.equalsIgnoreCase("FULL") || !spark.catalog.tableExists(fullTable))
        publishFull(prepared, fullTable, request, ctx)
      else
        publishIncremental(prepared, fullTable, request, ctx)

    logger.info(
      s"[CuratedService] published=${result.publishedCount} inserts=${result.insertCount} " +
        s"updates=${result.updateCount} deletes=${result.deleteCount}"
    )
    Some(result)
  }

  private def publishFull(
    df: DataFrame,
    fullTable: String,
    request: PublishRequest,
    ctx: RunContext
  ): CuratedResult = {
    val publishDf =
      if (spark.catalog.tableExists(fullTable)) transform.align(df, spark.table(fullTable).schema)
      else df

    val published = publisher.publish(publishDf, request, ctx)
    CuratedResult(published.publishedCount, insertCount = published.publishedCount, updateCount = 0L, deleteCount = 0L)
  }

  private def publishIncremental(
    incoming: DataFrame,
    fullTable: String,
    request: PublishRequest,
    ctx: RunContext
  ): CuratedResult = {
    val target = spark.table(fullTable)
    val merge = conf.getConfig("merge")
    val keys = ConfigUtils.stringList(merge, "keys")

    require(keys.nonEmpty, "INCR mode requires curated.merge.keys")

    val missingKeys = keys.filterNot { key =>
      incoming.columns.exists(_.equalsIgnoreCase(key)) &&
        target.columns.exists(_.equalsIgnoreCase(key))
    }
    require(missingKeys.isEmpty, s"Merge keys missing from incoming or target: ${missingKeys.mkString(",")}")

    val dropNull = ConfigUtils.optBoolean(merge, "null_handling.drop_null_keys").getOrElse(true)
    val blanksAsNull = ConfigUtils.optBoolean(merge, "null_handling.treat_blank_as_null").getOrElse(true)
    val orderBy = ConfigUtils.stringList(conf, "dedup.order_by")

    val cleaned = transform.filterNullKeys(incoming, keys, dropNull, blanksAsNull)
    val deduped = transform.deduplicate(cleaned, keys, orderBy)
    val alignedIncoming = transform.align(deduped, target.schema).persist()

    val incomingKeys = alignedIncoming.select(keys.map(col): _*).distinct().persist()
    val targetKeys = target.select(keys.map(col): _*).distinct()

    val updateCount = incomingKeys.join(targetKeys, keys, "inner").count()
    val insertCount = incomingKeys.count() - updateCount

    val unchanged = target.join(broadcast(incomingKeys), keys, "left_anti")
    val merged = unchanged.unionByName(alignedIncoming)

    val published = publisher.publish(merged, request, ctx)
    CuratedResult(published.publishedCount, insertCount, updateCount, deleteCount = 0L)
  }
}
