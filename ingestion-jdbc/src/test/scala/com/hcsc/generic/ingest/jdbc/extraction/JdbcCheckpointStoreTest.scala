package com.hcsc.generic.ingest.jdbc.extraction

import com.hcsc.generic.ingest.jdbc.extraction.store.{JdbcCheckpointStore, RetryingCheckpointStore}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class JdbcCheckpointStoreTest extends AnyFunSuite {

  Class.forName("org.h2.Driver")
  private val url = "jdbc:h2:mem:ckpt_store;DB_CLOSE_DELAY=-1;MODE=LEGACY"
  private val store = new JdbcCheckpointStore(url, Map("user" -> "sa", "password" -> ""))

  private def checkpoint(entity: String, value: String, version: Long) =
    Checkpoint(entity, "TIMESTAMP", BoundaryValue.deserialize(value), version, s"run-$version",
      Timestamp.valueOf("2026-07-28 12:00:00"))

  test("first commit inserts; latest round-trips the checkpoint") {
    store.commit(checkpoint("e1", "2026-07-28 10:00:00", 1L), expectedVersion = 0L)
    val loaded = store.latest("e1").get
    assert(loaded.value.serialized == "2026-07-28 10:00:00")
    assert(loaded.version == 1L && loaded.strategyKind == "TIMESTAMP")
  }

  test("CAS update advances only from the expected version") {
    store.commit(checkpoint("e1", "2026-07-28 11:00:00", 2L), expectedVersion = 1L)
    assert(store.latest("e1").get.version == 2L)

    val stale = intercept[CheckpointConflictException] {
      store.commit(checkpoint("e1", "2026-07-28 09:00:00", 2L), expectedVersion = 1L)
    }
    assert(stale.getMessage.contains("EXT_005"))
    assert(store.latest("e1").get.value.serialized == "2026-07-28 11:00:00",
      "a lost CAS must not overwrite the winner")
  }

  test("concurrent first-commit loses cleanly via the primary key") {
    store.commit(checkpoint("e2", "1", 1L), expectedVersion = 0L)
    intercept[CheckpointConflictException] {
      store.commit(checkpoint("e2", "2", 1L), expectedVersion = 0L)
    }
  }

  test("unknown entities have no checkpoint") {
    assert(store.latest("never-seen").isEmpty)
  }

  test("retry decorator retries transient failures but never a lost CAS") {
    var attempts = 0
    var commitCalls = 0
    val flaky = new CheckpointStore {
      def latest(entity: String): Option[Checkpoint] = {
        attempts += 1
        if (attempts < 3) throw new java.sql.SQLException("connection reset")
        store.latest(entity)
      }
      def commit(cp: Checkpoint, expectedVersion: Long): Unit = {
        commitCalls += 1
        throw new CheckpointConflictException(cp.entity, expectedVersion, 99L)
      }
    }
    val retrying = new RetryingCheckpointStore(flaky, maxAttempts = 3, backoffMs = 1L)

    assert(retrying.latest("e1").exists(_.version == 2L), "third attempt succeeds")
    assert(attempts == 3)

    // A conflict triggers ONE ambiguity read-back (was our commit already
    // applied?) and, when the stored row is someone else's, rethrows without
    // ever re-driving the commit itself.
    intercept[CheckpointConflictException] {
      retrying.commit(checkpoint("e1", "x", 3L), expectedVersion = 2L)
    }
    assert(commitCalls == 1, "conflicts are terminal — commit is never re-driven")
  }

  test("new boundary types: CDC offsets order numerically and allow overlap; snapshot ids reject overlap") {
    val offsetColumns = Seq(BoundaryColumn("lsn", BoundaryType.CdcOffset))
    assert(BoundaryOrdering.compare(offsetColumns,
      BoundaryValue(Seq("100")), BoundaryValue(Seq("99"))) > 0)
    assert(OverlapPolicy.applyTo(BoundaryValue(Seq("100")), offsetColumns, Some(BigDecimal(10)))
      == BoundaryValue(Seq("90")))

    val snapshotColumns = Seq(BoundaryColumn("snap", BoundaryType.SnapshotId))
    assert(BoundaryOrdering.compare(snapshotColumns,
      BoundaryValue(Seq("20260728")), BoundaryValue(Seq("20260727"))) > 0)
    val ex = intercept[IllegalArgumentException] {
      OverlapPolicy.applyTo(BoundaryValue(Seq("20260728")), snapshotColumns, Some(BigDecimal(1)))
    }
    assert(ex.getMessage.contains("SNAPSHOT_ID"))
  }
}
