package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config

/** How the curated publish writes the target. Absent config keeps the
  * legacy behavior: INCR against an existing table merges (full staged
  * rewrite), everything else fully replaces. */
sealed trait CuratedPublishMode
object CuratedPublishMode {
  /** Freshness-compared merge materialized as a full staged rewrite (the
    * table format has no row-level MERGE). Correct for any layout; cost is
    * O(table) per run. */
  case object Merge extends CuratedPublishMode
  /** Merge scoped to the partitions the batch touches: only affected
    * partitions are read, contested and rewritten. Requires
    * curated.partitioning.keys and merge keys. */
  case object PartitionOverwrite extends CuratedPublishMode
  /** Explicit full rebuild of the current snapshot. */
  case object FullOverwrite extends CuratedPublishMode

  def parse(value: String): CuratedPublishMode = value.trim.toUpperCase match {
    case "MERGE"               => Merge
    case "PARTITION_OVERWRITE" => PartitionOverwrite
    case "FULL_OVERWRITE"      => FullOverwrite
    case other => throw new IllegalArgumentException(
      s"CUR_006 curated.publish.mode '$other' must be MERGE, PARTITION_OVERWRITE or FULL_OVERWRITE")
  }
}

/** Policy for NULL values in partition columns. */
sealed trait NullPartitionPolicy
object NullPartitionPolicy {
  /** Fail the run when any partition column is null (default: a null
    * partition value is almost always upstream data loss). */
  case object Reject extends NullPartitionPolicy
  /** Replace nulls with the configured default_value. */
  final case class Default(value: String) extends NullPartitionPolicy
  /** Pass nulls through to Hive/Spark default-partition handling
    * (__HIVE_DEFAULT_PARTITION__). */
  case object Hive extends NullPartitionPolicy
}

/**
  * curated.partitioning {
  *   keys = ["src_sys_nm", "corp_ent_cd"]   # empty/absent = unpartitioned
  *   null_values = REJECT | DEFAULT | HIVE  # default REJECT
  *   default_value = "UNKNOWN"              # required when DEFAULT
  *   max_affected_partitions = 1000         # literal-predicate pruning cap
  * }
  *
  * Partition keys are PHYSICAL layout only — they are independent of the
  * business keys and are never assumed to be part of them.
  */
final case class CuratedPartitionSpec(
  keys: Seq[String],
  nullPolicy: NullPartitionPolicy,
  maxAffectedPartitions: Int
) {
  def isPartitioned: Boolean = keys.nonEmpty
}

object CuratedPartitioning {

  val Unpartitioned: CuratedPartitionSpec =
    CuratedPartitionSpec(Seq.empty, NullPartitionPolicy.Reject, DefaultMaxAffectedPartitions)

  def DefaultMaxAffectedPartitions: Int = 1000

  def parse(curatedConf: Config): CuratedPartitionSpec =
    ConfigUtils.optConfig(curatedConf, "partitioning") match {
      case None => Unpartitioned
      case Some(p) =>
        val keys = ConfigUtils.stringList(p, "keys")
        keys.foreach(k => ConfigUtils.requireSqlIdentifier(k, "curated.partitioning.keys entry"))
        require(keys.map(_.toLowerCase).distinct.size == keys.size,
          s"CUR_006 curated.partitioning.keys contains duplicates: ${keys.mkString(", ")}")

        val nullPolicy = ConfigUtils.optString(p, "null_values").getOrElse("REJECT").toUpperCase match {
          case "REJECT" => NullPartitionPolicy.Reject
          case "HIVE"   => NullPartitionPolicy.Hive
          case "DEFAULT" =>
            val value = ConfigUtils.optString(p, "default_value").getOrElse(
              throw new IllegalArgumentException(
                "CUR_006 curated.partitioning.null_values = DEFAULT requires default_value"))
            NullPartitionPolicy.Default(value)
          case other => throw new IllegalArgumentException(
            s"CUR_006 curated.partitioning.null_values '$other' must be REJECT, DEFAULT or HIVE")
        }

        val maxAffected = ConfigUtils.optInt(p, "max_affected_partitions")
          .getOrElse(DefaultMaxAffectedPartitions)
        require(maxAffected > 0, "CUR_006 curated.partitioning.max_affected_partitions must be positive")

        CuratedPartitionSpec(keys, nullPolicy, maxAffected)
    }
}
