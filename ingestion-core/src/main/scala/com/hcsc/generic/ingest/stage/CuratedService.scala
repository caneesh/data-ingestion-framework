package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.publish.{PublishRequest, PublishService}
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.{CuratedContractValidator, SchemaContract, SchemaContractViolationException}
import com.hcsc.generic.ingest.transform.CuratedTransform
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

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
  private var contract: Option[SchemaContract] = None

  val enabled: Boolean =
    ConfigUtils.optBoolean(conf, "enabled").getOrElse(true)

  /** Legacy entry point; runs with a synthetic run context. */
  def process(rawDf: DataFrame, runMode: String): Option[CuratedResult] =
    process(rawDf, runMode, RunContext(UUID.randomUUID().toString, "unknown", runMode, "F"))

  def process(rawDf: DataFrame, runMode: String, ctx: RunContext): Option[CuratedResult] =
    process(rawDf, runMode, ctx, None)

  def process(rawDf: DataFrame, runMode: String, ctx: RunContext, contract: Option[SchemaContract]): Option[CuratedResult] = {
    this.contract = contract
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

    // Second validation gate (HDR_018): the CURATED contract is validated
    // independently of RAW, immediately before publication.
    contract.foreach { c =>
      val mergeKeys =
        if (runMode.equalsIgnoreCase("INCR")) ConfigUtils.optConfig(conf, "merge")
          .map(m => ConfigUtils.stringList(m, "keys")).getOrElse(Seq.empty)
        else Seq.empty
      val dedupOrder = ConfigUtils.stringList(conf, "dedup.order_by")
      val violations = CuratedContractValidator.validate(prepared, c, mergeKeys, dedupOrder, logger)
      if (violations.nonEmpty)
        throw new SchemaContractViolationException(violations)
    }

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
      if (spark.catalog.tableExists(fullTable)) transform.align(df, spark.table(fullTable).schema, contract)
      else df

    val published = publisher.publish(publishDf, request, ctx)
    CuratedResult(published.publishedCount, insertCount = published.publishedCount, updateCount = 0L, deleteCount = 0L)
  }

  /**
    * Preserves update-vs-insert audit semantics on the merge path: incoming
    * rows whose key exists in the target inherit the target's
    * create_timestamp and are marked last_modified_op = 'U'
    * (last_modified_ts stays this run's timestamp from ensureAudit). Feeds
    * without the audit columns pass through unchanged.
    */
  private[stage] def applyUpdateAudit(incoming: DataFrame, target: DataFrame, keys: Seq[String]): DataFrame = {
    import org.apache.spark.sql.functions.{coalesce, lit, when}
    def actual(df: DataFrame, name: String): Option[String] =
      df.columns.find(_.equalsIgnoreCase(name))

    (actual(incoming, "create_timestamp"), actual(target, "create_timestamp"),
      actual(incoming, "last_modified_op")) match {
      case (Some(incCreate), Some(tgtCreate), Some(incOp)) =>
        val original = target
          .select((keys.map(col) :+ col(tgtCreate).cast("timestamp").as("_orig_create_ts")): _*)
          .dropDuplicates(keys)
        incoming.join(original, keys, "left")
          .withColumn(incCreate, coalesce(col("_orig_create_ts"), col(incCreate)))
          .withColumn(incOp, when(col("_orig_create_ts").isNotNull, lit("U")).otherwise(col(incOp)))
          .drop("_orig_create_ts")
      case _ => incoming
    }
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
    val alignedIncoming0 = transform.align(deduped, target.schema, contract)
    // Update audit semantics: rows replacing an existing key keep the
    // ORIGINAL create_timestamp and are stamped operation 'U'; only truly
    // new keys carry 'I' and their fresh creation time.
    val alignedIncoming = applyUpdateAudit(alignedIncoming0, target, keys).persist()
    val incomingKeys = alignedIncoming.select(keys.map(col): _*).distinct().persist()
    try {
      val targetKeys = target.select(keys.map(col): _*).distinct()

      val totalIncoming = incomingKeys.count()
      val updateCount = incomingKeys.join(targetKeys, keys, "inner").count()
      val insertCount = totalIncoming - updateCount

      // No forced broadcast: incoming key sets can be large; the optimizer
      // still broadcasts automatically when under the threshold.
      val unchanged = target.join(incomingKeys, keys, "left_anti")
      val merged = unchanged.unionByName(alignedIncoming)

      val published = publisher.publish(merged, request, ctx)
      CuratedResult(published.publishedCount, insertCount, updateCount, deleteCount = 0L)
    } finally {
      incomingKeys.unpersist(false)
      alignedIncoming.unpersist(false)
    }
  }
}
