package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.source.{Source, SourceRegistry}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcSource extends Source {

  override def sourceType: String = "jdbc"

  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    val url = sourceConf.getString("url")
    val driver = sourceConf.getString("driver")

    val query = if (sourceConf.hasPath("query")) {
      s"(${sourceConf.getString("query")}) as subquery"
    } else {
      sourceConf.getString("table")
    }

    var reader = spark.read
      .format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("dbtable", query)

    ConfigUtils.optString(sourceConf, "user").foreach(v => reader = reader.option("user", v))
    ConfigUtils.optString(sourceConf, "password").foreach(v => reader = reader.option("password", v))
    ConfigUtils.optInt(sourceConf, "fetchsize").foreach(v => reader = reader.option("fetchsize", v))
    ConfigUtils.optInt(sourceConf, "numPartitions").foreach(v => reader = reader.option("numPartitions", v))

    ConfigUtils.optString(sourceConf, "partitionColumn").foreach { partCol =>
      reader = reader.option("partitionColumn", partCol)
      ConfigUtils.optLong(sourceConf, "lowerBound").foreach(v => reader = reader.option("lowerBound", v))
      ConfigUtils.optLong(sourceConf, "upperBound").foreach(v => reader = reader.option("upperBound", v))
    }

    reader.load()
  }

  def register(): Unit = SourceRegistry.register(this)
}

object JdbcDrivers {
  val SqlServer = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val Db2 = "com.ibm.db2.jcc.DB2Driver"
  val Oracle = "oracle.jdbc.OracleDriver"
  val PostgreSQL = "org.postgresql.Driver"
  val MySQL = "com.mysql.cj.jdbc.Driver"
}
