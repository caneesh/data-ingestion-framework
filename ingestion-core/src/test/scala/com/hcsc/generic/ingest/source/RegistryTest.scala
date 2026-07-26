package com.hcsc.generic.ingest.source

import com.hcsc.generic.ingest.sink.{Sink, SinkRegistry}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

// ---------------------------------------------------------------------------
// Minimal stubs used only in registry tests — no Spark execution needed.
// ---------------------------------------------------------------------------

object TestSource1 extends Source {
  override def sourceType: String = "testsrc1"
  override def read(spark: SparkSession, sourceConf: Config): DataFrame =
    throw new UnsupportedOperationException("stub")
}

object TestSource2 extends Source {
  override def sourceType: String = "TestSrc2" // intentionally mixed-case
  override def read(spark: SparkSession, sourceConf: Config): DataFrame =
    throw new UnsupportedOperationException("stub")
}

object TestSink1 extends Sink {
  override def sinkType: String = "testsink1"
  override def write(spark: SparkSession, df: DataFrame, sinkConf: Config): Unit =
    throw new UnsupportedOperationException("stub")
}

class RegistryTest extends AnyFunSuite {

  // Each test uses fresh registry instances backed by ConcurrentHashMap.
  // Because the registries are global objects we register unique type names
  // per test to avoid cross-test contamination.

  // ---------------------------------------------------------------------------
  // SourceRegistry
  // ---------------------------------------------------------------------------

  test("SourceRegistry: register then resolve returns same object") {
    SourceRegistry.register(TestSource1)
    assert(SourceRegistry.resolve("testsrc1") eq TestSource1)
  }

  test("SourceRegistry: resolve is case-insensitive") {
    SourceRegistry.register(TestSource1)
    assert(SourceRegistry.resolve("TESTSRC1") eq TestSource1)
    assert(SourceRegistry.resolve("TestSrc1") eq TestSource1)
  }

  test("SourceRegistry: get returns Some for registered type") {
    SourceRegistry.register(TestSource1)
    assert(SourceRegistry.get("testsrc1") == Some(TestSource1))
  }

  test("SourceRegistry: get returns None for unknown type") {
    assert(SourceRegistry.get("no_such_source_xyz").isEmpty)
  }

  test("SourceRegistry: resolve throws for unknown type and mentions available types") {
    SourceRegistry.register(TestSource1)
    val ex = intercept[IllegalArgumentException] {
      SourceRegistry.resolve("absolutely_unknown_source_abc")
    }
    assert(ex.getMessage.contains("absolutely_unknown_source_abc"))
    assert(ex.getMessage.toLowerCase.contains("available"))
  }

  test("SourceRegistry: availableTypes includes registered type (lowercased)") {
    SourceRegistry.register(TestSource2) // registered with "TestSrc2" → stored as "testsrc2"
    assert(SourceRegistry.availableTypes.contains("testsrc2"))
  }

  // ---------------------------------------------------------------------------
  // SinkRegistry
  // ---------------------------------------------------------------------------

  test("SinkRegistry: register then resolve returns same object") {
    SinkRegistry.register(TestSink1)
    assert(SinkRegistry.resolve("testsink1") eq TestSink1)
  }

  test("SinkRegistry: resolve is case-insensitive") {
    SinkRegistry.register(TestSink1)
    assert(SinkRegistry.resolve("TESTSINK1") eq TestSink1)
  }

  test("SinkRegistry: get returns None for unknown type") {
    assert(SinkRegistry.get("no_such_sink_xyz").isEmpty)
  }

  test("SinkRegistry: resolve throws for unknown type and mentions available types") {
    val ex = intercept[IllegalArgumentException] {
      SinkRegistry.resolve("absolutely_unknown_sink_abc")
    }
    assert(ex.getMessage.contains("absolutely_unknown_sink_abc"))
    assert(ex.getMessage.toLowerCase.contains("available"))
  }
}
