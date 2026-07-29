package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.reject.RejectService
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.UUID

final class CuratedStageRunner(
  spark: SparkSession,
  curatedConf: Option[Config],
  logger: Logger
) {

  def run(rawDf: DataFrame, mode: String): Option[CuratedResult] =
    run(rawDf, mode, RunContext(UUID.randomUUID().toString, "unknown", mode, "F"))

  def run(rawDf: DataFrame, mode: String, ctx: RunContext): Option[CuratedResult] =
    run(rawDf, mode, ctx, None)

  def run(rawDf: DataFrame, mode: String, ctx: RunContext, contract: Option[SchemaContract]): Option[CuratedResult] =
    run(rawDf, mode, ctx, contract, None)

  def run(
    rawDf: DataFrame,
    mode: String,
    ctx: RunContext,
    contract: Option[SchemaContract],
    rejects: Option[RejectService]
  ): Option[CuratedResult] = {
    curatedConf match {
      case Some(conf) =>
        val service = new CuratedService(spark, conf)
        if (service.enabled) {
          val result = service.process(rawDf, mode, ctx, contract, rejects)
          logger.info(s"[CuratedStageRunner] Curated completed mode=$mode")
          result
        } else {
          logger.info("[CuratedStageRunner] Curated stage disabled")
          None
        }
      case None =>
        logger.info("[CuratedStageRunner] No curated config, skipping")
        None
    }
  }
}
