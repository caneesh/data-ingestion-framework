package com.hcsc.generic.ingest.transform

import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class AlignGuardTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private val transform = new CuratedTransform(spark)

  private val contract = SchemaContract.parse(ConfigFactory.parseString(
    """schema { version = "1.0", columns = [
      |  { name = "subscriber_id" },
      |  { name = "middle_name", required = false, default = "N/A" }
      |]}""".stripMargin)).get

  private val targetSchema = StructType(Seq(
    StructField("subscriber_id", StringType),
    StructField("middle_name", StringType),
    StructField("last_modified_op", StringType) // technical target-only column
  ))

  test("required contract column is never auto-created as null during alignment") {
    val df = Seq("x").toDF("middle_name") // subscriber_id missing entirely
    val ex = intercept[IllegalStateException] {
      transform.align(df, targetSchema, Some(contract))
    }
    assert(ex.getMessage.contains("HDR_001"))
    assert(ex.getMessage.contains("subscriber_id"))
  }

  test("optional contract column receives its configured default") {
    val df = Seq("S1").toDF("subscriber_id")
    val aligned = transform.align(df, targetSchema, Some(contract))
    assert(aligned.select("middle_name").collect().map(_.getString(0)).toSeq == Seq("N/A"))
  }

  test("non-contract technical columns are still null-filled") {
    val df = Seq(("S1", "M")).toDF("subscriber_id", "middle_name")
    val aligned = transform.align(df, targetSchema, Some(contract))
    assert(aligned.select("last_modified_op").collect().head.isNullAt(0))
  }

  test("without a contract, legacy null-fill alignment is unchanged") {
    val df = Seq("x").toDF("middle_name")
    val aligned = transform.align(df, targetSchema)
    assert(aligned.columns.toSet == Set("subscriber_id", "middle_name", "last_modified_op"))
    assert(aligned.select("subscriber_id").collect().head.isNullAt(0))
  }
}
