package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.reject.RejectService
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * `foreachBatch`-compatible entry into the ONE curated writer — the same
  * CuratedService every batch source uses; no separate streaming merge
  * algorithm exists.
  *
  * Guarantees and limits (non-transactional Hive targets):
  * - An empty micro-batch is a successful no-op: nothing is read or written.
  * - Replaying a batch id is idempotent: the merge is content-driven
  *   (freshness + deterministic tie-breaks, target wins exact ties), so the
  *   same rows produce the same snapshot and count as `ignored`.
  * - FULL_OVERWRITE is refused for micro-batches unless explicitly allowed
  *   (`curated.publish.allow_full_overwrite_micro_batches = true`) — a
  *   normal micro-batch must never rebuild the table.
  * - Checkpoint coordination: Spark advances the checkpoint only after
  *   foreachBatch returns, so a crash mid-write replays the batch;
  *   combined with the idempotent merge this yields at-least-once delivery
  *   with a convergent target. Exactly-once is NOT achievable on this
  *   table format — a crash between the curated write and checkpoint
  *   advancement re-executes a batch whose effects are already applied
  *   (and are then ignored by the merge).
  */
object CuratedMicroBatch {

  def write(
    spark: SparkSession,
    entity: String,
    curatedConf: Config,
    contract: Option[SchemaContract],
    rejects: Option[RejectService],
    microBatch: DataFrame,
    batchId: String,
    logger: Logger
  ): Option[CuratedResult] = {
    val mode = ConfigUtils.optConfig(curatedConf, "publish")
      .flatMap(p => ConfigUtils.optString(p, "mode")).map(CuratedPublishMode.parse)
    val allowFull = ConfigUtils.optConfig(curatedConf, "publish")
      .flatMap(p => ConfigUtils.optBoolean(p, "allow_full_overwrite_micro_batches"))
      .getOrElse(false)
    require(!mode.contains(CuratedPublishMode.FullOverwrite) || allowFull,
      "CUR_007 curated.publish.mode = FULL_OVERWRITE in a micro-batch context would rebuild " +
        "the whole table from one micro-batch. Use MERGE or PARTITION_OVERWRITE, or set " +
        "curated.publish.allow_full_overwrite_micro_batches = true if this is truly intended.")

    if (microBatch.isEmpty) {
      logger.info(s"[CuratedMicroBatch] batch=$batchId entity=$entity: empty micro-batch, " +
        "no curated changes required")
      return None
    }

    // The batch id IS the run identity: a Spark-retried micro-batch reuses
    // it, so audit/reject guards see the replay as the same run.
    val ctx = RunContext(runId = s"stream-$batchId", entity = entity, mode = "INCR", rawFlag = "S")
    new CuratedService(spark, curatedConf).process(microBatch, "INCR", ctx, contract, rejects)
  }
}
