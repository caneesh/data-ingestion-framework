package com.hcsc.generic.ingest.runtime

/**
  * Records that this JVM applied an operational override layer, so the audit
  * ledger can say so.
  *
  * WHY THIS EXISTS AT ALL: `config_fingerprint` hashes the feed's KEY
  * STRUCTURE, deliberately never its values. An override that changes a
  * lease from 30 to 120 minutes, or a reject threshold from 1% to 20%,
  * leaves the key set identical — so without this the ledger would record
  * two runs as configured identically when one of them ran under materially
  * different settings. That is precisely the drift an audit trail exists to
  * catch.
  *
  * Process-scoped by design: one driver JVM is one run, and the override is
  * decided once at startup before any audit row is written. Values are
  * digested, never stored — an override file may carry a credential.
  */
object OverrideContext {

  @volatile private var appliedDigest: Option[String] = None
  @volatile private var appliedPaths: Seq[String] = Nil

  /**
    * Records the override that is in effect. `paths` names what was
    * overridden and `digest` distinguishes two different override files that
    * touch the same paths.
    */
  def record(paths: Seq[String], digest: String): Unit = {
    appliedPaths = paths
    appliedDigest = Some(digest)
  }

  /** Digest of the applied override, if any. */
  def digest: Option[String] = appliedDigest

  /** Config paths the override replaced or introduced — names only. */
  def paths: Seq[String] = appliedPaths

  /** Suffix appended to `config_fingerprint`; empty when no override applied. */
  def fingerprintSuffix: String = appliedDigest.map("+ovr:" + _).getOrElse("")

  /** Test hook: restores the "no override" state. */
  private[ingest] def reset(): Unit = {
    appliedDigest = None
    appliedPaths = Nil
  }
}
