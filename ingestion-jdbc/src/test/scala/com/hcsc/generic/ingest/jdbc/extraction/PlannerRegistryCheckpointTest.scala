package com.hcsc.generic.ingest.jdbc.extraction

import com.hcsc.generic.ingest.jdbc.extraction.store.InMemoryCheckpointStore
import com.hcsc.generic.ingest.jdbc.extraction.strategies.StrategyKinds
import org.scalatest.funsuite.AnyFunSuite

class PlannerRegistryCheckpointTest extends AnyFunSuite {
  import ExtractionTestFixtures._

  test("registry resolves case-insensitively and lists available kinds on unknowns") {
    val registry = ExtractionStrategyRegistry(ExtractionFramework.builtInStrategies(new StubProbe()))
    assert(registry.resolve("timestamp").kind == StrategyKinds.Timestamp)
    val ex = intercept[IllegalArgumentException](registry.resolve("CDC"))
    assert(ex.getMessage.contains("EXT_003"))
    assert(ex.getMessage.contains(StrategyKinds.FullSnapshot))
  }

  test("registry rejects duplicate kinds") {
    val s = ExtractionFramework.builtInStrategies(new StubProbe())
    intercept[IllegalArgumentException](ExtractionStrategyRegistry(s ++ s.take(1)))
  }

  test("planner loads the previous checkpoint and plans from it") {
    val store = new InMemoryCheckpointStore
    store.commit(checkpoint("2026-07-28 09:00:00", version = 1L), expectedVersion = 0L)
    val probe = new StubProbe(maxAnswer = Some(BoundaryValue(Seq("2026-07-28 10:00:00.0"))))
    val planner = ExtractionFramework.planner(probe, store)

    val plan = planner.plan("claims_feed", "run-2",
      spec(StrategyKinds.Timestamp, Seq(tsColumn), initial = Some("1900-01-01 00:00:00")), capturedAt)

    plan.boundary match {
      case ExtractionBoundary.HalfOpen(lower, _) =>
        assert(lower.values.head.startsWith("2026-07-28 09:00:00"),
          "the stored checkpoint, not the initial value, drives the lower edge")
      case other => fail(s"unexpected boundary $other")
    }
  }

  test("planner aggregates validation errors into one EXT_001 failure") {
    val planner = ExtractionFramework.planner(new StubProbe(), new InMemoryCheckpointStore)
    val ex = intercept[IllegalArgumentException] {
      planner.plan("claims_feed", "run-1", spec(StrategyKinds.Timestamp), capturedAt)
    }
    assert(ex.getMessage.contains("EXT_001"))
    assert(ex.getMessage.contains("boundary.columns"))
    assert(ex.getMessage.contains("initial"))
  }

  test("committer is advance-only and CAS-guarded") {
    val store = new InMemoryCheckpointStore
    val committer = ExtractionFramework.committer(store)
    val columns = Seq(tsColumn)

    val first = committer.commit("claims_feed", "run-1", "TIMESTAMP",
      CheckpointProposal.CommitCapturedUpper(columns, BoundaryValue(Seq("2026-07-28 10:00:00"))), capturedAt)
    assert(first.exists(_.version == 1L))

    // A proposal behind the stored position is a no-op
    val stale = committer.commit("claims_feed", "run-2", "TIMESTAMP",
      CheckpointProposal.CommitCapturedUpper(columns, BoundaryValue(Seq("2026-07-28 09:00:00"))), capturedAt)
    assert(stale.isEmpty)
    assert(store.latest("claims_feed").get.value.values.head == "2026-07-28 10:00:00")

    // NoCheckpoint commits nothing
    assert(committer.commit("claims_feed", "run-3", "TIMESTAMP",
      CheckpointProposal.NoCheckpoint, capturedAt).isEmpty)

    // Direct CAS violation is detected
    intercept[CheckpointConflictException] {
      store.commit(checkpoint("2026-07-28 11:00:00", version = 9L), expectedVersion = 5L)
    }
  }

  test("extra strategies register without modifying the built-ins (OCP)") {
    val cdcLike = new ExtractionStrategy {
      val kind = "CDC"
      def validate(context: ExtractionContext): Seq[String] = Seq.empty
      def plan(context: ExtractionContext): ExtractionPlan =
        ExtractionPlan(context.entity, kind, context.relation, Seq.empty, Seq.empty,
          Seq.empty, ExtractionBoundary.Unbounded, Seq.empty, CheckpointProposal.NoCheckpoint, 1000)
    }
    val planner = ExtractionFramework.planner(new StubProbe(), new InMemoryCheckpointStore, extra = Seq(cdcLike))
    assert(planner.plan("e", "r", spec("CDC"), capturedAt).strategyKind == "CDC")
  }
}
