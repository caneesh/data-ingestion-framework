package com.hcsc.generic.ingest.files

import com.hcsc.generic.ingest.schema.{SchemaViolation, ViolationKind}
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedReader, InputStreamReader}

/** Result of inspecting a physical file's first logical record. */
final case class PhysicalHeader(
  fields: Seq[String],
  rawLine: String,
  hasDataRows: Boolean,
  issues: Seq[SchemaViolation]
)

/**
  * Reads and validates the PHYSICAL header of a delimited file before any
  * Spark read. Spark deduplicates repeated header names silently, so
  * duplicate physical headers must be detected here, on the raw bytes.
  *
  * The parser is quote-aware (handles delimiters inside quoted fields,
  * doubled quotes and escape characters) — never a naive split(delimiter).
  * Limitation: multi-line (embedded newline) header fields are treated as
  * malformed; vendors do not legitimately ship those in header rows.
  */
object PhysicalHeaderReader {

  private val CandidateDelimiters = Seq(',', ';', '|', '\t')

  def read(
    fs: FileSystem,
    file: Path,
    delimiter: Char,
    quote: Char,
    escape: Char,
    encoding: String
  ): PhysicalHeader = {
    val issues = scala.collection.mutable.ArrayBuffer.empty[SchemaViolation]

    val (firstLine, hasDataRows) = readFirstAndProbeSecond(fs, file, encoding)

    firstLine match {
      case None =>
        issues += SchemaViolation(ViolationKind.BlankHeader, "File is empty; no header record found")
        PhysicalHeader(Seq.empty, "", hasDataRows = false, issues.toList)

      case Some(raw) if raw.trim.isEmpty =>
        issues += SchemaViolation(ViolationKind.BlankHeader, "Header record is blank")
        PhysicalHeader(Seq.empty, raw, hasDataRows, issues.toList)

      case Some(raw) =>
        parseLine(raw, delimiter, quote, escape) match {
          case Left(error) =>
            issues += SchemaViolation(ViolationKind.MalformedQuote, s"Malformed quoted header: $error")
            PhysicalHeader(Seq.empty, raw, hasDataRows, issues.toList)

          case Right(fields) =>
            // Blank fields cover consecutive delimiters and trailing delimiters
            fields.zipWithIndex.filter(_._1.trim.isEmpty).foreach { case (_, idx) =>
              issues += SchemaViolation(
                ViolationKind.BlankHeader,
                s"Header field at position $idx is blank (consecutive or trailing delimiter)"
              )
            }

            // Duplicate PHYSICAL headers, before Spark renames them
            fields.map(_.trim).groupBy(identity).filter(v => v._1.nonEmpty && v._2.size > 1)
              .foreach { case (name, dupes) =>
                issues += SchemaViolation(
                  ViolationKind.DuplicatePhysicalHeader,
                  s"Physical header '$name' appears ${dupes.size} times"
                )
              }

            // Delimiter-mismatch heuristic, applied to the RAW line (never
            // the parsed field, whose quotes have been stripped — a valid
            // quoted single-column header may legitimately contain other
            // delimiter characters). Requires 2+ occurrences and no quoting
            // to keep false positives out of hard-fail territory.
            if (fields.length == 1 && !raw.contains(quote)) {
              CandidateDelimiters.filterNot(_ == delimiter)
                .find(c => raw.count(_ == c) >= 2)
                .foreach { candidate =>
                  issues += SchemaViolation(
                    ViolationKind.DelimiterMismatch,
                    s"Configured delimiter '$delimiter' produced one field but header contains repeated '$candidate'; " +
                      "suspected delimiter mismatch"
                  )
                }
            }

            PhysicalHeader(fields, raw, hasDataRows, issues.toList)
        }
    }
  }

  private def readFirstAndProbeSecond(fs: FileSystem, file: Path, encoding: String): (Option[String], Boolean) = {
    val reader = new BufferedReader(new InputStreamReader(fs.open(file), encoding))
    try {
      var first = Option(reader.readLine())
      // Strip a UTF-8 BOM that survives decoding as
      first = first.map(l => if (l.nonEmpty && l.charAt(0) == '\uFEFF') l.substring(1) else l)
      var second = Option(reader.readLine())
      while (second.exists(_.trim.isEmpty)) second = Option(reader.readLine())
      (first, second.isDefined)
    } finally reader.close()
  }

  /** Quote-aware single-line CSV field parser. Supports doubled quotes and a
    * distinct escape character. Returns Left on unterminated quotes. */
  private[files] def parseLine(line: String, delimiter: Char, quote: Char, escape: Char): Either[String, Seq[String]] = {
    val fields = scala.collection.mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var inQuotes = false
    var fieldStarted = false
    var i = 0
    val n = line.length

    while (i < n) {
      val c = line.charAt(i)
      if (inQuotes) {
        if (c == escape && escape != quote && i + 1 < n && line.charAt(i + 1) == quote) {
          current.append(quote); i += 2
        } else if (c == quote) {
          if (i + 1 < n && line.charAt(i + 1) == quote) { current.append(quote); i += 2 }
          else { inQuotes = false; i += 1 }
        } else { current.append(c); i += 1 }
      } else {
        if (c == quote && !fieldStarted && current.isEmpty) {
          inQuotes = true; fieldStarted = true; i += 1
        } else if (c == delimiter) {
          fields += current.toString; current.clear(); fieldStarted = false; i += 1
        } else { current.append(c); fieldStarted = true; i += 1 }
      }
    }

    if (inQuotes) Left(s"Unterminated quote in header record: $line")
    else { fields += current.toString; Right(fields.toList) }
  }
}
