package com.hcsc.generic.ingest.jdbc

import java.sql.DriverManager

/** In-memory H2 database shared by JDBC tests. DB_CLOSE_DELAY=-1 keeps the
  * database alive for the JVM lifetime. */
object H2TestDatabase {
  Class.forName("org.h2.Driver")

  val Url = "jdbc:h2:mem:jdbc_tests;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
  val Driver = "org.h2.Driver"

  def execute(statements: String*): Unit = {
    val conn = DriverManager.getConnection(Url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try statements.foreach(stmt.execute)
      finally stmt.close()
    } finally conn.close()
  }

  /** Base source config for H2 (generic dialect requires explicit driver). */
  def sourceHocon(extra: String = ""): String =
    s"""
       |type = "jdbc"
       |url = "$Url"
       |dialect = "generic"
       |driver = "$Driver"
       |auth { user = "sa", password = { provider = "inline", value = "" } }
       |health_check { enabled = false }
       |$extra
    """.stripMargin
}
