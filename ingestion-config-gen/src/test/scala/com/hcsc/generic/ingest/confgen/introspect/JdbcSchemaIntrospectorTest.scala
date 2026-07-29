package com.hcsc.generic.ingest.confgen.introspect

import com.hcsc.generic.ingest.confgen.ConfigGeneratorMain
import com.hcsc.generic.ingest.confgen.draft.Drafts
import com.hcsc.generic.ingest.confgen.io.ScriptedConsole
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, Types}
import scala.collection.JavaConverters._

/** Schema introspection: the generator connects to the source database and
  * emits one contract column per source column — the 350-column mapping
  * bootstrap. */
class JdbcSchemaIntrospectorTest extends AnyFunSuite {

  private val h2Url = "jdbc:h2:mem:introspect_e2e;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"

  private def h2(statements: String*): Unit = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try statements.foreach(stmt.execute)
      finally stmt.close()
    } finally conn.close()
  }

  private def answers(table: String): Answers = {
    val a = new Answers
    Seq(
      "entity" -> "wide_feed",
      "mode" -> "FULL",
      "source.url" -> h2Url,
      "source.dialect" -> "generic",
      "source.driver" -> "org.h2.Driver",
      "source.auth.type" -> "SQL_PASSWORD",
      "source.auth.user" -> "sa",
      "_auth.secret.provider" -> "inline",
      "_auth.secret.value" -> "",
      "source.mode" -> "FULL_TABLE",
      "source.table" -> table,
      "_schema.use_contract" -> "true",
      "_schema.introspect" -> "true",
      "_perf.partitioning" -> "NONE",
      "_audit.enabled" -> "false",
      "raw.database" -> "wide_raw",
      "raw.path" -> "/tmp/raw/wide",
      "curated.enabled" -> "false"
    ).foreach { case (k, v) => a.put(k, AnswerValue.Scalar(v)) }
    a
  }

  private def generate(table: String): (Int, ScriptedConsole, java.nio.file.Path) = {
    val answersFile = Files.createTempFile("answers-introspect", ".json").toString
    Drafts.save(answersFile, "jdbc", answers(table))
    val dir = Files.createTempDirectory("confgen-introspect")
    val console = new ScriptedConsole(Seq.empty)
    val exit = ConfigGeneratorMain.run(
      ConfigGeneratorMain.Options(
        outputDir = dir.toString,
        formats = Seq("hocon"),
        answersFile = Some(answersFile)),
      console)
    (exit, console, dir)
  }

  test("introspection generates the full column mapping into the schema file") {
    h2(
      "DROP TABLE IF EXISTS claims_wide",
      """CREATE TABLE claims_wide (
        |  claim_id VARCHAR(20) NOT NULL,
        |  member_id BIGINT NOT NULL,
        |  amount DECIMAL(12,2),
        |  visits INT,
        |  discharged BOOLEAN,
        |  score DOUBLE,
        |  service_date DATE,
        |  modified_ts TIMESTAMP NOT NULL,
        |  payload VARBINARY(100)
        |)""".stripMargin)

    val (exit, console, dir) = generate("claims_wide")
    assert(exit == 0, console.output)
    assert(console.output.contains("Introspected 9 column(s)"))

    val schemaPath = Paths.get(dir.toString, "wide_feed-schema.conf")
    assert(Files.exists(schemaPath), "mapping must land in the separate schema file")

    val main = ConfigFactory.parseFile(Paths.get(dir.toString, "wide_feed.conf").toFile)
    val columns = main.getConfigList("feeds.wide_feed.schema.columns").asScala
    assert(columns.size == 9)

    val byName = columns.map(c => c.getString("name").toLowerCase -> c).toMap
    assert(byName("claim_id").getString("type") == "string")
    assert(!byName("claim_id").getBoolean("nullable"))
    assert(byName("member_id").getString("type") == "bigint")
    assert(byName("amount").getString("type") == "decimal(12,2)")
    assert(byName("amount").getBoolean("nullable"))
    assert(byName("visits").getString("type") == "int")
    assert(byName("discharged").getString("type") == "boolean")
    assert(byName("score").getString("type") == "double")
    assert(byName("service_date").getString("type") == "date")
    assert(byName("modified_ts").getString("type") == "timestamp")
    assert(byName("payload").getString("type") == "binary")

    // Positions preserve the source column order, 0-based.
    assert(byName("claim_id").getInt("position") == 0)
    assert(byName("payload").getInt("position") == 8)

    // The schema file is where the mapping lives, ready for refinement.
    val schemaText = new String(Files.readAllBytes(schemaPath), UTF_8)
    assert(schemaText.contains("claim_id"))
    assert(schemaText.contains("decimal(12,2)"))
  }

  test("an unknown table fails generation with a clear message") {
    val (exit, console, _) = generate("does_not_exist")
    assert(exit == 1)
    assert(console.output.contains("introspection"), console.output)
    assert(console.output.contains("does_not_exist"))
  }

  test("an explicit schema.columns answer wins over introspection") {
    h2(
      "DROP TABLE IF EXISTS tiny",
      "CREATE TABLE tiny (a VARCHAR(5), b INT)")
    val a = answers("tiny")
    a.put("schema.columns", AnswerValue.Scalar("""[{"name": "only_this", "type": "string"}]"""))
    val answersFile = Files.createTempFile("answers-explicit", ".json").toString
    Drafts.save(answersFile, "jdbc", a)
    val dir = Files.createTempDirectory("confgen-explicit")
    val console = new ScriptedConsole(Seq.empty)
    val exit = ConfigGeneratorMain.run(
      ConfigGeneratorMain.Options(outputDir = dir.toString, formats = Seq("hocon"),
        answersFile = Some(answersFile)), console)
    assert(exit == 0, console.output)
    val main = ConfigFactory.parseFile(Paths.get(dir.toString, "wide_feed.conf").toFile)
    val columns = main.getConfigList("feeds.wide_feed.schema.columns").asScala
    assert(columns.map(_.getString("name")) == Seq("only_this"))
  }

  test("SQL type mapping covers the SQL Server surface") {
    import JdbcSchemaIntrospector.sparkType
    assert(sparkType(Types.NVARCHAR, "nvarchar", 100, 0) == "string")
    assert(sparkType(Types.NUMERIC, "numeric", 18, 4) == "decimal(18,4)")
    assert(sparkType(Types.NUMERIC, "numeric", 0, 0) == "decimal(38,18)")
    assert(sparkType(Types.TINYINT, "tinyint", 3, 0) == "tinyint")
    assert(sparkType(Types.BIT, "bit", 1, 0) == "boolean")
    assert(sparkType(-155, "datetimeoffset", 34, 7) == "string")
    assert(sparkType(Types.OTHER, "hierarchyid", 0, 0) == "string")
  }
}
