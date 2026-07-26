package com.hcsc.generic.ingest.transform

import com.typesafe.config.{Config, ConfigValueType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import scala.collection.JavaConverters._

final case class PartitionSpec(keys: Seq[String], expressions: Map[String, String])

object Partitioning {
  def parse(rawConf: Config): PartitionSpec = {
    if (!rawConf.hasPath("partitioning")) return PartitionSpec(Seq.empty, Map.empty)

    val p = rawConf.getConfig("partitioning")
    val keys =
      if (p.hasPath("keys")) p.getStringList("keys").asScala.toSeq else Seq.empty

    val expressions =
      if (!p.hasPath("derive")) Map.empty[String, String]
      else {
        val derive = p.getConfig("derive")
        derive.root().keySet().asScala.map { key =>
          val expression =
            if (derive.getValue(key).valueType() == ConfigValueType.STRING) {
              derive.getString(key)
            } else {
              val item = derive.getConfig(key)
              val kind = if (item.hasPath("kind")) item.getString("kind") else "expr"
              kind.toLowerCase match {
                case "expr"    => item.getString("expr")
                case "literal" => s"'${item.getString("value").replace("'", "''")}'"
                case other =>
                  throw new IllegalArgumentException(
                    s"Unsupported partition derive kind '$other' for key '$key'"
                  )
              }
            }
          key -> expression
        }.toMap
      }

    PartitionSpec(keys, expressions)
  }

  def apply(df: DataFrame, spec: PartitionSpec): DataFrame = {
    spec.keys.foldLeft(df) { (acc, key) =>
      spec.expressions.get(key) match {
        case Some(sql) => acc.withColumn(key, expr(sql))
        case None if acc.columns.exists(_.equalsIgnoreCase(key)) => acc
        case None =>
          throw new IllegalArgumentException(
            s"Partition key '$key' does not exist and no derive expression was configured"
          )
      }
    }
  }
}
