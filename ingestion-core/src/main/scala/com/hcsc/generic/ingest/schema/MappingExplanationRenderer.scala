package com.hcsc.generic.ingest.schema

/** Human-readable header mapping explanation for --explain-mapping and
  * production support (HDR troubleshooting). */
object MappingExplanationRenderer {

  def render(
    entity: String,
    file: String,
    contract: SchemaContract,
    physicalHeaders: Seq[String],
    resolution: Option[HeaderResolution]
  ): String = {
    val sb = new StringBuilder
    sb.append(s"Entity: $entity\n")
    sb.append(s"File: $file\n")
    sb.append(s"Schema version: ${contract.version}\n\n")

    val rows = physicalHeaders.map { h =>
      val normalized = HeaderNormalizer.normalize(h)
      val canonical = contract.resolve(h)
      val resolutionType = canonical match {
        case Some(c) if HeaderNormalizer.normalize(c) == normalized => "EXACT"
        case Some(_)                                                => "ALIAS"
        case None                                                   => "UNEXPECTED"
      }
      (h, normalized, canonical.getOrElse("-"), resolutionType)
    }

    val headers = ("Source Header", "Normalized Header", "Canonical Column", "Resolution")
    val all = headers +: rows
    def w(f: ((String, String, String, String)) => String): Int = all.map(f(_).length).max + 2

    val (w1, w2, w3) = (w(_._1), w(_._2), w(_._3))
    def line(r: (String, String, String, String)): String =
      r._1.padTo(w1, ' ') + r._2.padTo(w2, ' ') + r._3.padTo(w3, ' ') + r._4

    sb.append(line(headers)).append('\n')
    sb.append("-" * (w1 + w2 + w3 + 12)).append('\n')
    rows.foreach(r => sb.append(line(r)).append('\n'))

    resolution.foreach { r =>
      sb.append('\n')
      sb.append(s"Missing required columns: ${if (r.missingRequired.isEmpty) "none" else r.missingRequired.mkString(", ")}\n")
      sb.append(s"Missing optional columns: ${if (r.missingOptional.isEmpty) "none" else r.missingOptional.map(_.name).mkString(", ")}\n")
      sb.append(s"Positional fallback used: ${r.positionalFallbackUsed}\n")
      val status =
        if (r.violations.isEmpty) "PASSED"
        else if (r.violations.forall(_.kind.policyOf(contract.policies) != PolicyAction.Fail)) "PASSED_WITH_WARNINGS"
        else "FAILED"
      sb.append(s"Validation status: $status\n")
      if (r.violations.nonEmpty) {
        sb.append("Violations:\n")
        r.violations.foreach(v => sb.append(s"  - $v\n"))
      }
    }
    sb.toString
  }
}
