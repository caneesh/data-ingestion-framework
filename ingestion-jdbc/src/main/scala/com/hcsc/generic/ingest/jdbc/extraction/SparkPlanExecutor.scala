package com.hcsc.generic.ingest.jdbc.extraction

import com.hcsc.generic.ingest.jdbc.extraction.render.SqlPlanRenderer
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
  * ExtractionPlan -> DataFrame. The only class in the extraction subsystem
  * that imports Spark. Execution mechanics only — every decision was already
  * made by the strategy and is visible in the plan.
  */
final class SparkPlanExecutor(
  spark: SparkSession,
  url: String,
  driver: String,
  connectionProperties: Map[String, String],
  renderer: SqlPlanRenderer
) {

  def execute(plan: ExtractionPlan): DataFrame = {
    val rendered = renderer.render(plan)
    if (rendered.partitionPredicates.isEmpty) {
      var reader = spark.read.format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("dbtable", rendered.dbtable)
        .option("fetchsize", rendered.fetchSize.toString)
      connectionProperties.foreach { case (k, v) => reader = reader.option(k, v) }
      reader.load()
    } else {
      val props = new Properties()
      props.setProperty("driver", driver)
      props.setProperty("fetchsize", rendered.fetchSize.toString)
      connectionProperties.foreach { case (k, v) => props.setProperty(k, v) }
      spark.read.jdbc(url, rendered.dbtable, rendered.partitionPredicates.toArray, props)
    }
  }
}
