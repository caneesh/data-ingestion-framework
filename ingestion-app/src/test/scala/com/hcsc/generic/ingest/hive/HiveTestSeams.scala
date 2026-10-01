package com.hcsc.generic.ingest.hive

/** Bridges `HiveSource`'s package-private test seams to the app-module
  * pipeline specs, which must live in `com.hcsc.generic.ingest.app` to reach
  * `IngestMain.execute` (private[app]). Test code only. */
object HiveTestSeams {
  /** Forget every in-memory read window — the state a NEW driver JVM starts
    * with, so `--resume` / `--pending` must recover the window from the
    * ledger instead. */
  def clearReadWindows(): Unit = HiveSource.clearReadWindows()
}
