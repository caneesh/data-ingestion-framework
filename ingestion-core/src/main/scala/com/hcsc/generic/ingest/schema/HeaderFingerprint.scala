package com.hcsc.generic.ingest.schema

import java.security.MessageDigest

/** SHA-256 fingerprint over the CANONICAL headers in source order (falling
  * back to the normalized header for unresolved columns), so files whose
  * headers are alias-equivalent share a fingerprint and group together for
  * a single Spark read. */
object HeaderFingerprint {

  def of(headers: Seq[String], contract: SchemaContract): String = {
    val canonicalOrdered = headers.map(h => contract.resolve(h).getOrElse(HeaderNormalizer.normalize(h)))
    of(canonicalOrdered)
  }

  def of(canonicalOrderedHeaders: Seq[String]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes = digest.digest(canonicalOrderedHeaders.mkString("\u0001").getBytes("UTF-8"))
    bytes.map("%02x".format(_)).mkString
  }
}
