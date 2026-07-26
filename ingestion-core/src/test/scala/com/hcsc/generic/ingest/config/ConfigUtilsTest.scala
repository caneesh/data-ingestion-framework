package com.hcsc.generic.ingest.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class ConfigUtilsTest extends AnyFunSuite {

  // ---------------------------------------------------------------------------
  // optString
  // ---------------------------------------------------------------------------

  test("optString returns Some when path exists") {
    val c = ConfigFactory.parseString("""foo = "bar" """)
    assert(ConfigUtils.optString(c, "foo") == Some("bar"))
  }

  test("optString returns None when path is absent") {
    val c = ConfigFactory.parseString("{}")
    assert(ConfigUtils.optString(c, "missing") == None)
  }

  // ---------------------------------------------------------------------------
  // optBoolean
  // ---------------------------------------------------------------------------

  test("optBoolean returns Some(true) when path exists") {
    val c = ConfigFactory.parseString("flag = true")
    assert(ConfigUtils.optBoolean(c, "flag") == Some(true))
  }

  test("optBoolean returns Some(false) when path is false") {
    val c = ConfigFactory.parseString("flag = false")
    assert(ConfigUtils.optBoolean(c, "flag") == Some(false))
  }

  test("optBoolean returns None when path is absent") {
    val c = ConfigFactory.parseString("{}")
    assert(ConfigUtils.optBoolean(c, "flag") == None)
  }

  // ---------------------------------------------------------------------------
  // optInt
  // ---------------------------------------------------------------------------

  test("optInt returns Some when path exists") {
    val c = ConfigFactory.parseString("n = 42")
    assert(ConfigUtils.optInt(c, "n") == Some(42))
  }

  test("optInt returns None when path is absent") {
    val c = ConfigFactory.parseString("{}")
    assert(ConfigUtils.optInt(c, "n") == None)
  }

  // ---------------------------------------------------------------------------
  // optLong
  // ---------------------------------------------------------------------------

  test("optLong returns Some for a large value") {
    val c = ConfigFactory.parseString("big = 9876543210")
    assert(ConfigUtils.optLong(c, "big") == Some(9876543210L))
  }

  test("optLong returns None when path is absent") {
    val c = ConfigFactory.parseString("{}")
    assert(ConfigUtils.optLong(c, "big") == None)
  }

  // ---------------------------------------------------------------------------
  // stringList
  // ---------------------------------------------------------------------------

  test("stringList returns elements when path exists") {
    val c = ConfigFactory.parseString("""items = ["a", "b", "c"]""")
    assert(ConfigUtils.stringList(c, "items") == Seq("a", "b", "c"))
  }

  test("stringList returns empty Seq when path is absent") {
    val c = ConfigFactory.parseString("{}")
    assert(ConfigUtils.stringList(c, "items") == Seq.empty)
  }

  // ---------------------------------------------------------------------------
  // stringMap
  // ---------------------------------------------------------------------------

  test("stringMap returns map of string values") {
    val c = ConfigFactory.parseString("""m { a = "x", b = "y" }""")
    assert(ConfigUtils.stringMap(c, "m") == Map("a" -> "x", "b" -> "y"))
  }

  test("stringMap returns empty Map when path is absent") {
    val c = ConfigFactory.parseString("{}")
    assert(ConfigUtils.stringMap(c, "m") == Map.empty)
  }

  test("stringMap throws IllegalArgumentException when a value is a list (not a string)") {
    // A list value causes ConfigException.WrongType when retrieved via getString;
    // stringMap must wrap this in an IllegalArgumentException.
    val c = ConfigFactory.parseString("""m { a = [1, 2, 3] }""")
    val ex = intercept[IllegalArgumentException] {
      ConfigUtils.stringMap(c, "m")
    }
    assert(ex.getMessage.contains("m"))
  }

  // ---------------------------------------------------------------------------
  // configList
  // ---------------------------------------------------------------------------

  test("configList returns list of Config objects") {
    val c = ConfigFactory.parseString("""items = [{k = "v1"}, {k = "v2"}]""")
    val list = ConfigUtils.configList(c, "items")
    assert(list.size == 2)
    assert(list(0).getString("k") == "v1")
    assert(list(1).getString("k") == "v2")
  }

  test("configList returns empty Seq when path is absent") {
    val c = ConfigFactory.parseString("{}")
    assert(ConfigUtils.configList(c, "items") == Seq.empty)
  }

  // ---------------------------------------------------------------------------
  // sqlIdentifier
  // ---------------------------------------------------------------------------

  test("sqlIdentifier accepts a plain alphanumeric name") {
    val c = ConfigFactory.parseString("""table = "my_table" """)
    assert(ConfigUtils.sqlIdentifier(c, "table") == "my_table")
  }

  test("sqlIdentifier accepts name starting with underscore") {
    val c = ConfigFactory.parseString("""table = "_priv" """)
    assert(ConfigUtils.sqlIdentifier(c, "table") == "_priv")
  }

  test("sqlIdentifier rejects name with a dot") {
    val c = ConfigFactory.parseString("""table = "my.table" """)
    intercept[IllegalArgumentException] {
      ConfigUtils.sqlIdentifier(c, "table")
    }
  }

  test("sqlIdentifier rejects name with a backtick") {
    val c = ConfigFactory.parseString("""table = "`foo`" """)
    intercept[IllegalArgumentException] {
      ConfigUtils.sqlIdentifier(c, "table")
    }
  }

  test("sqlIdentifier rejects name with a semicolon") {
    val c = ConfigFactory.parseString("""table = "foo;bar" """)
    intercept[IllegalArgumentException] {
      ConfigUtils.sqlIdentifier(c, "table")
    }
  }

  test("sqlIdentifier rejects name with a space") {
    val c = ConfigFactory.parseString("""table = "my table" """)
    intercept[IllegalArgumentException] {
      ConfigUtils.sqlIdentifier(c, "table")
    }
  }

  test("sqlIdentifier rejects name starting with a digit") {
    val c = ConfigFactory.parseString("""table = "1table" """)
    intercept[IllegalArgumentException] {
      ConfigUtils.sqlIdentifier(c, "table")
    }
  }
}
