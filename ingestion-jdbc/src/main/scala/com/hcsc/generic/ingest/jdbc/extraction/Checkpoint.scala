package com.hcsc.generic.ingest.jdbc.extraction

import java.sql.Timestamp

/**
  * Durable extraction position. `version` supports optimistic compare-and-set
  * so two concurrent runs of one entity cannot both advance (the loser fails
  * with EXT_005 instead of silently overwriting).
  */
final case class Checkpoint(
  entity: String,
  strategyKind: String,
  value: BoundaryValue,
  version: Long,
  runId: String,
  committedAt: Timestamp
)

/** What the plan proposes to commit after the run fully succeeds. Strategies
  * that track no position propose NoCheckpoint. Future variants (extracted
  * max, CDC offsets) extend this ADT without touching the strategy trait. */
sealed trait CheckpointProposal
object CheckpointProposal {
  case object NoCheckpoint extends CheckpointProposal
  /** Commit the upper boundary captured on the driver before extraction —
    * never the extracted max — so the committed position equals the window
    * that was actually read. Carries the columns so advance-only comparison
    * needs no external context. */
  final case class CommitCapturedUpper(columns: Seq[BoundaryColumn], value: BoundaryValue)
    extends CheckpointProposal
}

/** Persistence port for checkpoints. Implementations must be safe to call
  * repeatedly (latest is read-only; commit is CAS-guarded). */
trait CheckpointStore {
  def latest(entity: String): Option[Checkpoint]

  /** Commits `checkpoint` only if the store's current version for the entity
    * equals `expectedVersion` (0 when no checkpoint exists yet).
    * @throws CheckpointConflictException when a concurrent run advanced first */
  def commit(checkpoint: Checkpoint, expectedVersion: Long): Unit
}

final class CheckpointConflictException(entity: String, expected: Long, actual: Long)
  extends RuntimeException(
    s"EXT_005 checkpoint for '$entity' was advanced concurrently (expected version $expected, found $actual); " +
      "this run must not overwrite it")

/**
  * Applies a plan's proposal after a fully successful run: advance-only (a
  * proposal at or behind the stored position is a no-op) and CAS-guarded.
  * Returns the committed checkpoint, or None when nothing was written.
  */
final class CheckpointCommitter(store: CheckpointStore) {

  def commit(
    entity: String,
    runId: String,
    strategyKind: String,
    proposal: CheckpointProposal,
    committedAt: Timestamp
  ): Option[Checkpoint] = proposal match {
    case CheckpointProposal.NoCheckpoint => None
    case CheckpointProposal.CommitCapturedUpper(columns, value) =>
      val current = store.latest(entity)
      val regressed = current.exists(c => BoundaryOrdering.compare(columns, c.value, value) >= 0)
      if (regressed) None
      else {
        val expectedVersion = current.map(_.version).getOrElse(0L)
        val next = Checkpoint(entity, strategyKind, value, expectedVersion + 1, runId, committedAt)
        store.commit(next, expectedVersion)
        Some(next)
      }
  }
}
