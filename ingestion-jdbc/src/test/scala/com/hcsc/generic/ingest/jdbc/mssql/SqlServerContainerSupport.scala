package com.hcsc.generic.ingest.jdbc.mssql

import com.hcsc.generic.ingest.jdbc.watermark.InMemoryWatermarkStore
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.testcontainers.containers.MSSQLServerContainer

import java.sql.{Connection, DriverManager}

/**
  * Shared fixture for the Docker-gated SQL Server integration suite.
  *
  * Docker gating is mandatory: the presence of a working Docker daemon is
  * probed once (see [[SqlServerContainerSupport.dockerAvailable]]) and every
  * test starts with `assume(dockerAvailable, ...)` so the suite CANCELS
  * (ScalaTest "canceled", not "failed") when Docker is missing. This keeps
  * `mvn test` green in both CI-with-Docker and laptop-without-Docker
  * environments.
  *
  * Lifecycle: the container is started once per suite (lazy val forced in
  * beforeAll, only when Docker is available) and stopped in afterAll. Each
  * test gets a clean in-memory watermark store via beforeEach.
  */
trait SqlServerContainerSupport
  extends BeforeAndAfterAll
  with BeforeAndAfterEach { this: Suite =>

  import SqlServerContainerSupport._

  /** Materialized lazily so we never touch Docker when it is unavailable. */
  protected lazy val container: MSSQLServerContainer[_] = {
    val c = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-latest")
      .acceptLicense()
      .asInstanceOf[MSSQLServerContainer[_]]
    c.start()
    c
  }

  /** Only stand the container up when Docker is present; otherwise the tests
    * themselves cancel via `assume`, so we must not pay the startup cost or
    * risk a failure here. */
  override def beforeAll(): Unit = {
    super.beforeAll()
    if (dockerAvailable) container // force lazy init (one-time start)
  }

  override def afterAll(): Unit = {
    try {
      if (dockerAvailable) container.stop()
    } finally super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    InMemoryWatermarkStore.clear()
  }

  /** Base HOCON for a SQL Server feed against the running container.
    *
    * The Testcontainers image presents a self-signed TLS certificate, so we
    * cannot use the dialect default (encrypt=true;trustServerCertificate=false)
    * — that would fail the TLS handshake against the throwaway cert. Setting
    * encrypt=false disables TLS for the local, disposable container; production
    * feeds keep the secure dialect defaults. (trustServerCertificate=true would
    * be the encrypted-but-lenient alternative.)
    */
  protected def baseHocon(extra: String): String =
    s"""
       |type = "jdbc"
       |url = "${container.getJdbcUrl}"
       |dialect = "sqlserver"
       |auth {
       |  user = "${container.getUsername}"
       |  password = { provider = "inline", value = "${container.getPassword}" }
       |}
       |connection_properties { encrypt = "false" }
       |health_check { enabled = false }
       |$extra
     """.stripMargin

  /** Opens a raw JDBC connection to the container (encrypt disabled) for
    * DDL/DML setup that the framework itself does not perform. */
  protected def withConnection[A](f: Connection => A): A = {
    val props = new java.util.Properties()
    props.setProperty("user", container.getUsername)
    props.setProperty("password", container.getPassword)
    props.setProperty("encrypt", "false")
    val conn = DriverManager.getConnection(container.getJdbcUrl, props)
    try f(conn) finally conn.close()
  }

  protected def execute(statements: String*): Unit = withConnection { conn =>
    val stmt = conn.createStatement()
    try statements.foreach(stmt.execute) finally stmt.close()
  }
}

object SqlServerContainerSupport {

  /** Probed once for the whole JVM. Wrapped in try/catch: on machines without
    * Docker `isDockerAvailable()` can throw rather than return false. */
  lazy val dockerAvailable: Boolean =
    try org.testcontainers.DockerClientFactory.instance().isDockerAvailable()
    catch { case _: Throwable => false }
}
