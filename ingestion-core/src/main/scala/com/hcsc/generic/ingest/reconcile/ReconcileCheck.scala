package com.hcsc.generic.ingest.reconcile

/** One comparison, in the shape the reconciliation ledger already stores. */
final case class ReconcileCheck(name: String, expected: String, actual: String, passed: Boolean)
