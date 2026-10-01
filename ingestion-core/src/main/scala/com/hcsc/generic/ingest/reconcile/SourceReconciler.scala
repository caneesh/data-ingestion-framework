package com.hcsc.generic.ingest.reconcile

/**
  * An independent SOURCE-vs-CURATED comparison for one source type. The
  * stage driver (`--stage reconcile`) owns the entity lock, the ledger rows,
  * the notification and the REPORT/FAIL decision; an implementation only
  * produces the checks and reports the configured policy.
  */
trait SourceReconciler {
  def configured: Boolean

  /** REPORT (default) or FAIL. */
  def onMismatch: String

  def run(): Seq[ReconcileCheck]
}
