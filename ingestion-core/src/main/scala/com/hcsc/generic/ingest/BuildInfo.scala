package com.hcsc.generic.ingest

/**
  * Identity of the running build, read from the assembly jar's manifest.
  *
  * The deployment path is: download the repository as a ZIP (which strips
  * `.git`), build the jar on a laptop, scp the jar to the server. Neither
  * the laptop nor the server is a checkout, so nothing outside the jar can
  * say which build is running — and three separate debugging rounds were
  * spent on a stale jar that looked identical to a current one.
  *
  * The jar therefore carries its own answer. Values come from
  * `Implementation-Version` and `Build-Time`, stamped by the assembly
  * plugin; both degrade to "unknown" when running from classes (tests, an
  * IDE), which is the honest answer there rather than a misleading one.
  */
object BuildInfo {

  private lazy val attributes: Map[String, String] =
    try {
      val source = getClass.getProtectionDomain.getCodeSource
      Option(source).map(_.getLocation).map(_.toURI).map(new java.io.File(_)) match {
        case Some(file) if file.isFile =>
          val jar = new java.util.jar.JarFile(file)
          try {
            Option(jar.getManifest).map { manifest =>
              import scala.collection.JavaConverters._
              manifest.getMainAttributes.asScala.map {
                case (k, v) => k.toString -> String.valueOf(v)
              }.toMap
            }.getOrElse(Map.empty)
          } finally jar.close()
        // A directory code source means classes, not a jar: no manifest.
        case _ => Map.empty
      }
    } catch {
      // Identity is diagnostic, never load-bearing: a security manager or an
      // unusual classloader must not prevent the pipeline from starting.
      case _: Throwable => Map.empty
    }

  private def attribute(name: String): String =
    attributes.get(name).map(_.trim).filter(_.nonEmpty).getOrElse("unknown")

  /** Maven project version, e.g. "1.0.0-SNAPSHOT". */
  lazy val version: String = attribute("Implementation-Version")

  /** UTC build timestamp — the value that actually distinguishes two builds
    * of the same SNAPSHOT version, which is every build here. */
  lazy val buildTime: String = attribute("Build-Time")

  /** One line for the startup log and the audit ledger. */
  lazy val summary: String = s"$version (built $buildTime)"
}
