package com.hcsc.generic.ingest.files

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
  * Configurable pre-read file validation. Every check is optional and driven
  * by the source.validation block:
  *
  * validation {
  *   filename_pattern = "member_.*\\.csv"
  *   allowed_extensions = ["csv", "txt"]
  *   encoding = "UTF-8"
  *   min_size_bytes = 1
  *   max_size_bytes = 1073741824
  *   delimiter = ","
  *   checksum { sidecar_suffix = ".sha256", required = false, algorithm = "SHA-256" }
  *   header { expected_columns = 2, expected_names = ["subscriber_id", "hios_id"] }
  *   trailer { required = true, marker = "TRL" }
  * }
  *
  * Returns every failed check so quarantined files carry the full reason.
  */
final class FileValidator(validationConf: Option[Config]) {

  def validate(fs: FileSystem, status: FileStatus): Seq[String] = {
    val conf = validationConf.getOrElse(return Seq.empty)
    val file = status.getPath
    val name = file.getName
    val failures = scala.collection.mutable.ArrayBuffer.empty[String]

    ConfigUtils.optString(conf, "filename_pattern").foreach { pattern =>
      if (!name.matches(pattern))
        failures += s"Filename '$name' does not match pattern '$pattern'"
    }

    val extensions = ConfigUtils.stringList(conf, "allowed_extensions")
    if (extensions.nonEmpty) {
      val ext = name.lastIndexOf('.') match {
        case -1 => ""
        case i  => name.substring(i + 1).toLowerCase
      }
      if (!extensions.map(_.toLowerCase.stripPrefix(".")).contains(ext))
        failures += s"Extension '$ext' not in allowed extensions ${extensions.mkString(",")}"
    }

    ConfigUtils.optLong(conf, "min_size_bytes").foreach { min =>
      if (status.getLen < min)
        failures += s"File size ${status.getLen} below minimum $min bytes"
    }
    ConfigUtils.optLong(conf, "max_size_bytes").foreach { max =>
      if (status.getLen > max)
        failures += s"File size ${status.getLen} above maximum $max bytes"
    }

    val encoding = ConfigUtils.optString(conf, "encoding")
    encoding.foreach { charset =>
      FsUtils.encodingError(fs, file, charset).foreach(failures += _)
    }

    ConfigUtils.optConfig(conf, "checksum").foreach { c =>
      val suffix = ConfigUtils.optString(c, "sidecar_suffix").getOrElse(".sha256")
      val algorithm = ConfigUtils.optString(c, "algorithm").getOrElse("SHA-256")
      val required = ConfigUtils.optBoolean(c, "required").getOrElse(false)
      val sidecar = new Path(file.getParent, name + suffix)
      if (fs.exists(sidecar)) {
        val expected = FsUtils.readFirstLine(fs, sidecar).map(_.trim.toLowerCase).getOrElse("")
        val actual = FsUtils.checksum(fs, file, algorithm)
        if (expected != actual)
          failures += s"Checksum mismatch: sidecar declares '$expected', computed '$actual'"
      } else if (required) {
        failures += s"Required checksum sidecar '${sidecar.getName}' not found"
      }
    }

    val delimiter = ConfigUtils.optString(conf, "delimiter").getOrElse(",")
    ConfigUtils.optConfig(conf, "header").foreach { h =>
      val charset = encoding.getOrElse("UTF-8")
      FsUtils.readFirstLine(fs, file, charset) match {
        case None =>
          failures += "File is empty; header validation failed"
        case Some(line) =>
          val cols = line.split(java.util.regex.Pattern.quote(delimiter), -1)
          ConfigUtils.optInt(h, "expected_columns").foreach { expected =>
            if (cols.length != expected)
              failures += s"Header column count ${cols.length} does not match expected $expected"
          }
          val expectedNames = ConfigUtils.stringList(h, "expected_names")
          if (expectedNames.nonEmpty) {
            val actualNames = cols.map(_.trim.toLowerCase).toSeq
            if (actualNames != expectedNames.map(_.trim.toLowerCase))
              failures += s"Header names [${actualNames.mkString(",")}] do not match expected [${expectedNames.mkString(",")}]"
          }
      }
    }

    ConfigUtils.optConfig(conf, "trailer").foreach { t =>
      if (ConfigUtils.optBoolean(t, "required").getOrElse(false)) {
        val charset = encoding.getOrElse("UTF-8")
        val marker = ConfigUtils.optString(t, "marker").getOrElse("")
        FsUtils.readLastLine(fs, file, charset) match {
          case None =>
            failures += "File is empty; required trailer not found"
          case Some(line) if marker.nonEmpty && !line.trim.toLowerCase.startsWith(marker.trim.toLowerCase) =>
            failures += s"Last line does not start with required trailer marker '$marker'"
          case _ => ()
        }
      }
    }

    failures.toList
  }
}
