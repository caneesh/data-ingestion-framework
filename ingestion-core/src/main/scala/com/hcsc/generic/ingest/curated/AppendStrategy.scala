package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.runtime.RunContext
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * Append: every accepted row is added to the curated table — no matching,
  * no replacement. Suited to event-shaped curated data (the curated table
  * is itself a history). Idempotency relies on the pipeline's RAW replay
  * guards upstream; the append itself is intentionally unconditional.
  */
final class AppendStrategy(
  spark: SparkSession,
  preparation: CuratedPreparation,
  logger: Logger
) extends CuratedStrategy {

  val kind: String = CuratedStrategies.Append

  def process(input: DataFrame, target: CuratedTarget, ctx: RunContext): CuratedMetrics = {
    val (metrics, elapsed) = preparation.timed {
      val prepared = preparation.prepare(input, target)
      val rows = prepared.count()

      if (ctx.dryRun) {
        logger.info(s"[Curated] DRY-RUN: would append $rows row(s) to ${target.fullTable}")
      } else if (spark.catalog.tableExists(target.fullTable)) {
        val targetColumns = spark.table(target.fullTable).columns
        val present = prepared.columns.map(_.toLowerCase).toSet
        val missing = targetColumns.filterNot(c => present.contains(c.toLowerCase))
        require(missing.isEmpty, s"CUR_002 curated input is missing target columns: ${missing.mkString(", ")}")
        prepared.select(targetColumns.map(col): _*).write.mode(SaveMode.Append).insertInto(target.fullTable)
      } else {
        var writer = prepared.write.format(target.format).mode(SaveMode.Overwrite)
        target.path.foreach(p => writer = writer.option("path", p))
        writer.saveAsTable(target.fullTable)
      }
      CuratedMetrics(
        inputRows = rows, duplicateRows = 0L, publishedRows = rows,
        insertRows = rows, updateRows = 0L, deleteRows = 0L, durationMillis = 0L)
    }
    metrics.copy(durationMillis = elapsed)
  }
}
