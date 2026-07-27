package com.hcsc.generic.ingest.jdbc.read

import java.sql.{SQLException, SQLNonTransientException, SQLTransientException}

/**
  * Classifies JDBC failures so retries apply ONLY to transient errors.
  * Permanent failures (auth, syntax, missing objects, configuration) fail
  * immediately. Classification walks the cause chain and inspects the
  * SQLException subtype, SQLState class, and vendor error codes (including
  * Microsoft SQL Server / Azure SQL specifics).
  */
object SqlFailureClassifier {

  sealed trait Category { def retryable: Boolean }
  case object TransientNetwork extends Category { val retryable = true }
  case object TransientService extends Category { val retryable = true }
  case object Throttling extends Category { val retryable = true }
  case object Deadlock extends Category { val retryable = true }
  case object Timeout extends Category { val retryable = true } // query timeout
  /** Lock wait timeout (SQL Server 1222): retry only where explicitly
    * approved — blind retries can pile onto a blocked resource. */
  case object LockTimeout extends Category { val retryable = false }
  case object Authentication extends Category { val retryable = false }
  case object Authorization extends Category { val retryable = false }
  case object SqlSyntax extends Category { val retryable = false }
  case object ObjectNotFound extends Category { val retryable = false }
  case object SchemaMismatch extends Category { val retryable = false }
  case object Configuration extends Category { val retryable = false }
  case object Unknown extends Category { val retryable = false } // conservative: do not retry blind

  // Azure SQL / SQL Server vendor error codes
  private val SqlServerThrottling = Set(40501, 10928, 10929, 49918, 49919, 49920)
  private val SqlServerTransient = Set(4060, 40197, 40613, 40143, 233, 64)
  private val SqlServerDeadlock = Set(1205)
  private val SqlServerLockTimeout = Set(1222)
  private val SqlServerTimeout = Set(-2)
  private val SqlServerAuthFailed = Set(18456, 18452)
  private val SqlServerObjectMissing = Set(208, 2812)
  private val SqlServerColumnMissing = Set(207, 213)

  def classify(error: Throwable): Category = {
    causes(error).collectFirst {
      case e: IllegalArgumentException if e.getMessage != null && e.getMessage.startsWith("JDBC_") =>
        Configuration
      case e: SQLException => classifySql(e)
    }.getOrElse {
      causes(error).collectFirst {
        case _: java.net.SocketException => TransientNetwork
        case _: java.net.SocketTimeoutException => Timeout
        case _: java.net.UnknownHostException => TransientNetwork
        case _: java.util.concurrent.TimeoutException => Timeout
      }.getOrElse(Unknown)
    }
  }

  private def classifySql(e: SQLException): Category = {
    val state = Option(e.getSQLState).getOrElse("")
    val vendor = e.getErrorCode

    // Vendor codes first: most specific signal
    if (SqlServerThrottling.contains(vendor)) Throttling
    else if (SqlServerDeadlock.contains(vendor)) Deadlock
    else if (SqlServerLockTimeout.contains(vendor)) LockTimeout
    else if (SqlServerTimeout.contains(vendor)) Timeout
    else if (SqlServerAuthFailed.contains(vendor)) Authentication
    else if (SqlServerObjectMissing.contains(vendor)) ObjectNotFound
    else if (SqlServerColumnMissing.contains(vendor)) SchemaMismatch
    else if (SqlServerTransient.contains(vendor)) TransientService
    // SQLState classes (SQL standard)
    else if (state.startsWith("08")) TransientNetwork      // connection exception
    else if (state == "40001") Deadlock                    // serialization failure
    else if (state.startsWith("40")) TransientService      // transaction rollback
    else if (state.startsWith("28")) Authentication        // invalid authorization
    else if (state.startsWith("42")) classify42(state)     // syntax or access rule
    else if (state.startsWith("HYT")) Timeout              // ODBC-style timeouts
    else if (state.startsWith("22")) SchemaMismatch        // data exception
    // JDBC exception hierarchy as fallback
    else e match {
      case _: java.sql.SQLTimeoutException => Timeout
      case _: java.sql.SQLInvalidAuthorizationSpecException => Authentication
      case _: java.sql.SQLSyntaxErrorException => SqlSyntax
      case _: SQLTransientException => TransientService
      case _: java.sql.SQLRecoverableException => TransientNetwork
      case _: SQLNonTransientException => Unknown
      case _ => Unknown
    }
  }

  private def classify42(state: String): Category = state match {
    case "42S02" => ObjectNotFound       // base table not found
    case "42S22" => SchemaMismatch       // column not found
    case "42501" => Authorization        // insufficient privilege
    case _        => SqlSyntax
  }

  private def causes(t: Throwable): Seq[Throwable] = {
    val buffer = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    var current = t
    while (current != null && !buffer.contains(current)) {
      buffer += current
      current = current.getCause
    }
    buffer.toList
  }
}
