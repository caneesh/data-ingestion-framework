package com.hcsc.generic.ingest.raw

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.Timestamp

/**
  * Identity of one Raw write unit. A run may carry several batches (file
  * groups, offset ranges, extraction windows); the batch id is what makes a
  * write attributable and replays detectable. `loadTimestamp` is injected —
  * the writer never reads the clock itself.
  */
final case class BatchContext(
  feedName: String,
  sourceSystem: String,
  runId: String,
  batchId: String,
  rawFlag: String = "F",
  loadTimestamp: Timestamp,
  dryRun: Boolean = false
)

object BatchContext {

  /** Deterministic batch id: the same run and the same discriminators (file
    * group, offset range, extraction window) always produce the same id, so
    * a replayed batch is recognizable instead of being double-loaded. */
  def deterministicBatchId(runId: String, discriminators: Seq[String]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    // Length-prefixed parts: no separator character to collide with or to
    // embed as a raw control byte in source.
    (runId +: discriminators).foreach { part =>
      val bytes = part.getBytes(StandardCharsets.UTF_8)
      digest.update((bytes.length.toString + ":").getBytes(StandardCharsets.UTF_8))
      digest.update(bytes)
    }
    digest.digest()
      .map("%02x".format(_)).mkString.take(32)
  }
}
