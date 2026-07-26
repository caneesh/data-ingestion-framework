package com.hcsc.generic.ingest.jdbc

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

/**
  * Lightweight in-process metrics for the JDBC subsystem. The framework has
  * no metrics backend; counters accumulate per JVM and are exposed for
  * tests, logged snapshots and future export (wire `snapshot` into your
  * metrics agent at the deployment boundary).
  *
  * Counter names follow the operational catalog:
  *   jdbc_connection_attempt_total, jdbc_connection_failure_total,
  *   jdbc_retry_total, jdbc_transient_failure_total,
  *   jdbc_permanent_failure_total, jdbc_watermark_commit_total,
  *   jdbc_watermark_conflict_total, jdbc_schema_drift_total,
  *   jdbc_partitions_total
  */
object JdbcMetrics {
  private val counters = new ConcurrentHashMap[String, LongAdder]()

  def increment(name: String, delta: Long = 1L): Unit =
    counters.computeIfAbsent(name, _ => new LongAdder()).add(delta)

  def value(name: String): Long =
    Option(counters.get(name)).map(_.sum()).getOrElse(0L)

  def snapshot: Map[String, Long] = {
    import scala.collection.JavaConverters._
    counters.asScala.map { case (k, v) => k -> v.sum() }.toMap
  }

  def logSummary(logger: org.apache.log4j.Logger): Unit = {
    val snap = snapshot
    if (snap.nonEmpty)
      logger.info("[JdbcMetrics] " + snap.toSeq.sortBy(_._1)
        .map { case (k, v) => s"$k=$v" }.mkString(" "))
  }

  def reset(): Unit = counters.clear()
}
