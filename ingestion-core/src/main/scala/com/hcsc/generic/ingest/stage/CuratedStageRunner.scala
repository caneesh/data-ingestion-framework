package com.hcsc.generic.ingest.stage

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

final class CuratedStageRunner(
  spark: SparkSession,
  curatedConf: Option[Config],
  logger: Logger
) {
  def run(rawDf: DataFrame, mode: String): Unit = {
    curatedConf match {
      case Some(conf) =>
        val service = new CuratedService(spark, conf)
        if (service.enabled) {
          service.process(rawDf, mode)
          logger.info(s"[CuratedStageRunner] Curated completed mode=$mode")
        } else {
          logger.info("[CuratedStageRunner] Curated stage disabled")
        }
      case None =>
        logger.info("[CuratedStageRunner] No curated config, skipping")
    }
  }
}
