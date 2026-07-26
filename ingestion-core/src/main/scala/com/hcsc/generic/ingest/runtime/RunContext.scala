package com.hcsc.generic.ingest.runtime

/** Immutable context describing a single ingestion run, threaded through all
  * stages so audit records, file lifecycle moves and replay logic share one
  * identity. */
final case class RunContext(
  runId: String,
  entity: String,
  mode: String,
  rawFlag: String,
  dryRun: Boolean = false,
  forceReprocess: Boolean = false,
  fileIdFilter: Option[String] = None,
  resume: Boolean = false
)

object Stages {
  val Validate = "validate"
  val Raw = "raw"
  val Curated = "curated"
  val Publish = "publish"

  val ordered: Seq[String] = Seq(Validate, Raw, Curated, Publish)
}

object StageStatus {
  val Started = "STARTED"
  val Success = "SUCCESS"
  val Failed = "FAILED"
  val Skipped = "SKIPPED"
}
