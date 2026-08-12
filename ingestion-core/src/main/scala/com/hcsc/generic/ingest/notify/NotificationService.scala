package com.hcsc.generic.ingest.notify

import com.hcsc.generic.ingest.audit.AuditService
import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import java.io.OutputStreamWriter
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import org.apache.log4j.Logger

/** One terminal run outcome, as delivered to a sink. */
final case class NotificationEvent(
  entity: String,
  runId: String,
  outcome: String,
  stage: String,
  message: String,
  host: String
) {
  /** Hand-built JSON: the payload is five known strings, so a dependency
    * would buy nothing. Values are escaped, never interpolated raw. */
  def toJson: String = {
    def esc(s: String) = s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c if c.isControl => f"\\u${c.toInt}%04x"
      case c    => c.toString
    }
    s"""{"entity":"${esc(entity)}","run_id":"${esc(runId)}",""" +
      s""""outcome":"${esc(outcome)}","stage":"${esc(stage)}",""" +
      s""""message":"${esc(message)}","host":"${esc(host)}"}"""
  }

  def summary: String = s"[$outcome] entity=$entity run=$runId stage=$stage: $message"
}

/**
  * Delivers terminal run outcomes to an operator.
  *
  * The framework already DETECTS failures precisely — stage status, the
  * reconciliation checks, the reject thresholds — and until now told nobody:
  * a 3am Control-M failure was visible only to whoever thought to query the
  * ledger. This closes that half.
  *
  * EVERY delivery path is best-effort and swallows its own failure. A
  * pipeline must never fail, or succeed, because of an alert: the run's
  * outcome is already decided by the time this is called, and an
  * unreachable webhook is an operations problem, not a data problem.
  *
  * Messages pass through AuditService.sanitizeMessage first — driver
  * exceptions embed JDBC URLs, which can carry credentials, and a webhook
  * is a far wider audience than a log file.
  *
  * Configuration reference: docs/architecture/CONFIGURATION_MODEL.md.
  */
final class NotificationService(conf: Option[Config], logger: Logger) {

  /** Absent block = disabled, so existing feeds are untouched. */
  val enabled: Boolean =
    conf.exists(c => ConfigUtils.optBoolean(c, "enabled").getOrElse(true))

  /** Which outcomes are delivered. FAILURE only by default: success on
    * every run of every feed is how alerting gets muted wholesale. */
  private lazy val outcomes: Set[String] =
    conf.map(c => ConfigUtils.stringList(c, "on") match {
      case Nil  => Set("FAILURE")
      case list => list.map(_.trim.toUpperCase).toSet
    }).getOrElse(Set("FAILURE"))

  private lazy val webhookUrl: Option[String] =
    conf.flatMap(c => ConfigUtils.optConfig(c, "webhook"))
      .flatMap(w => ConfigUtils.optString(w, "url")).map(_.trim).filter(_.nonEmpty)

  private lazy val webhookTimeoutMs: Int =
    conf.flatMap(c => ConfigUtils.optConfig(c, "webhook"))
      .flatMap(w => ConfigUtils.optLong(w, "timeout_ms")).getOrElse(5000L).toInt

  private lazy val command: Option[String] =
    conf.flatMap(c => ConfigUtils.optString(c, "command")).map(_.trim).filter(_.nonEmpty)

  private lazy val host: String =
    try java.net.InetAddress.getLocalHost.getHostName
    catch { case _: Throwable => "unknown" }

  def notifyFailure(entity: String, runId: String, stage: String, message: String): Unit =
    deliver("FAILURE", entity, runId, stage, message)

  def notifySuccess(entity: String, runId: String, stage: String, message: String): Unit =
    deliver("SUCCESS", entity, runId, stage, message)

  private def deliver(
    outcome: String, entity: String, runId: String, stage: String, message: String
  ): Unit = {
    if (!enabled || !outcomes.contains(outcome.toUpperCase)) return
    val event = NotificationEvent(
      entity, runId, outcome, stage,
      AuditService.sanitizeMessage(String.valueOf(message)), host)

    // Always leave a local trace, even when every sink fails: the log is
    // the sink of last resort.
    logger.info(s"[Notify] ${event.summary}")

    webhookUrl.foreach(url => attempt(s"webhook $url")(postJson(url, event.toJson)))
    command.foreach(cmd => attempt(s"command $cmd")(runCommand(cmd, event)))
  }

  /** Runs a sink, converting ANY failure into a warning. */
  private def attempt(what: String)(f: => Unit): Unit =
    try f
    catch {
      case e: Throwable =>
        logger.warn(s"[Notify] delivery to $what failed (the run outcome is unaffected): " +
          s"${AuditService.sanitizeMessage(String.valueOf(e.getMessage))}")
    }

  private def postJson(url: String, body: String): Unit = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("POST")
      connection.setConnectTimeout(webhookTimeoutMs)
      connection.setReadTimeout(webhookTimeoutMs)
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", "application/json; charset=utf-8")
      val out = new OutputStreamWriter(connection.getOutputStream, StandardCharsets.UTF_8)
      try { out.write(body); out.flush() } finally out.close()
      val status = connection.getResponseCode
      if (status < 200 || status >= 300)
        logger.warn(s"[Notify] webhook returned HTTP $status")
    } finally connection.disconnect()
  }

  /** The event is passed as ARGUMENTS, never interpolated into a shell
    * string: a message containing shell metacharacters would otherwise be
    * executed, and these messages are built from source exception text. */
  private def runCommand(cmd: String, event: NotificationEvent): Unit = {
    val process = new ProcessBuilder(
      cmd, event.outcome, event.entity, event.runId, event.stage, event.message)
      .redirectErrorStream(true)
      .start()
    process.getOutputStream.close()
    val finished = process.waitFor(webhookTimeoutMs.toLong, java.util.concurrent.TimeUnit.MILLISECONDS)
    if (!finished) {
      process.destroyForcibly()
      logger.warn(s"[Notify] command '$cmd' did not finish within ${webhookTimeoutMs}ms; killed")
    } else if (process.exitValue() != 0)
      logger.warn(s"[Notify] command '$cmd' exited ${process.exitValue()}")
  }
}

object NotificationService {
  def apply(feedConf: Config, logger: Logger): NotificationService =
    new NotificationService(ConfigUtils.optConfig(feedConf, "notifications"), logger)
}
