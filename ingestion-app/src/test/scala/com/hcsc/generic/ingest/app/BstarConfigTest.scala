package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.config.{FeedCompatibilityValidator, IngestionPattern}
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

/**
  * Canary for the bstar example (docs/examples/bstar): the two production
  * feeds and their two lower-environment twins must parse, pass compatibility
  * validation, derive the hive pattern, and carry the owner-confirmed
  * decisions (keys, DL soft delete, freshness column, SOURCE raw mode,
  * audit-category partition columns) — so the example cannot rot against
  * the framework. Skipped when the docs tree is absent.
  */
class BstarConfigTest extends AnyFunSuite {

  private val Root = "../docs/examples/bstar"

  private case class Feed(path: String, entity: String, table: String, keys: Seq[String],
                          lcts: String, columns: Int, pii: Set[String])

  private val PrivAddrKeys = Seq("corp_ent_cd", "acct_grp_nbr", "sub_seq_nbr", "mem_nbr", "addr_seq_nbr")
  private val SubPremDetKeys = Seq("corp_ent_cd", "acct_nbr", "ben_agmt_nbr", "sub_seq_nbr",
    "sub_prm_hst_sq_nbr", "sub_prm_eff_sq_nbr", "sub_prm_det_sq_nbr")

  private val prod = Seq(
    Feed(s"$Root/params/feed-bstar-priv-addr.conf", "bstar_priv_addr", "priv_addr", PrivAddrKeys,
      "priv_addr_lcts", 24, Set("str_ln_1", "str_ln_2", "zip_cd", "phn_nbr")),
    Feed(s"$Root/params/feed-bstar-sub-prem-det.conf", "bstar_sub_prem_det", "sub_prem_det", SubPremDetKeys,
      "sub_prem_det_lcts", 30, Set("dob")))
  private val e2e = Seq(
    Feed(s"$Root/lower-env/params/feed-bstar-priv-addr-e2e.conf", "bstar_priv_addr_e2e", "priv_addr", PrivAddrKeys,
      "priv_addr_lcts", 24, Set("str_ln_1", "str_ln_2", "zip_cd", "phn_nbr")),
    Feed(s"$Root/lower-env/params/feed-bstar-sub-prem-det-e2e.conf", "bstar_sub_prem_det_e2e", "sub_prem_det",
      SubPremDetKeys, "sub_prem_det_lcts", 30, Set("dob")))

  private def load(f: Feed): Config = {
    val file = new File(f.path)
    assume(file.exists(), s"docs tree not present at ${file.getAbsolutePath}")
    ConfigFactory.parseFile(file).resolve().getConfig(s"feeds.${f.entity}")
  }

  private def contract(feed: Config): SchemaContract =
    SchemaContract.parse(feed).getOrElse(fail("contract must parse"))

  (prod ++ e2e).foreach { f =>
    test(s"${f.entity}: parses, validates clean, derives PARTITION_INCREMENTAL over a hive source") {
      val feed = load(f)
      assert(feed.getString("entity") == f.entity)
      assert(feed.getString("source.type") == "hive")
      // Production reads the real table; the e2e twin reads the synthetic
      // <table>_src from lower-env/ddl/source_test_data.sql.
      val expectedSource = if (f.entity.endsWith("_e2e")) s"${f.table}_src" else f.table
      assert(feed.getString("source.table") == expectedSource)
      val problems = FeedCompatibilityValidator.validate(feed)
      assert(problems.isEmpty, s"feed must pass CFG validation, got: ${problems.mkString("; ")}")
      val (spec, cfgProblems) = IngestionPattern.derive(feed)
      assert(cfgProblems.isEmpty, cfgProblems.mkString("; "))
      assert(spec.pattern == IngestionPattern.PartitionIncremental, spec.toString)
      assert(spec.extractionMode == "HIVE", spec.toString)
      assert(spec.curatedStrategy == "KEYED_MERGE" && spec.deleteStrategy == "SOFT", spec.toString)
    }

    test(s"${f.entity}: the contract carries the confirmed key, CDC columns and ${f.columns} columns") {
      val feed = load(f)
      val c = contract(feed)
      assert(c.columns.size == f.columns, s"expected ${f.columns} columns, got ${c.columns.size}")
      // Business key: owner-confirmed 2026-10-01 from the CDC key markers,
      // and merge.keys must say the same (HDR_017 otherwise).
      assert(c.businessKeyColumns == f.keys, "business_key must be the confirmed DB2 primary key")
      assert(feed.getStringList("curated.merge.keys").asScalaSeq == f.keys, "merge.keys must equal the contract key")
      // Owner-confirmed non-keys: addr_type_cd (priv_addr); mem_nbr,
      // corp_tier_nbr, tier_agrgt_seq_nbr (sub_prem_det).
      assert(!c.businessKeyColumns.contains("addr_type_cd"))
      if (f.table == "sub_prem_det")
        Seq("mem_nbr", "corp_tier_nbr", "tier_agrgt_seq_nbr").foreach(n =>
          assert(!c.businessKeyColumns.contains(n), s"$n is NOT a key column"))
      // CDC columns are required: a row without them cannot be merged.
      Seq("cdc_src_last_updt_ts", "cdc_src_actn_cd").foreach { n =>
        assert(c.column(n).exists(_.required), s"$n must be required")
      }
      assert(c.incrementalColumns == Seq("cdc_src_last_updt_ts"), "lineage source_modified_ts column")
      // Partition columns: present, required, audit category (out of the hash, kept in curated).
      Seq("inc_ful_flag", "file_date", "file_time").foreach { n =>
        val col = c.column(n).getOrElse(fail(s"$n must be declared"))
        assert(col.required && col.category == "audit" && col.dataType == "string", s"$n: $col")
      }
      // PII tags drive reject-payload masking.
      val sensitive = c.sensitiveColumns.map(_.toLowerCase).toSet
      assert(f.pii.subsetOf(sensitive), s"PII columns must be tagged: missing ${f.pii -- sensitive}")
      // Numeric source types are preserved, CHAR/VARCHAR are trimmed strings.
      assert(c.column("sub_seq_nbr").exists(_.dataType == "int"))
      assert(c.columns.filter(_.dataType == "string").filterNot(_.category == "audit")
        .forall(_.transform.contains("nullif(trim({col}), '')")), "every string business column is trimmed")
    }

    test(s"${f.entity}: raw.mode = SOURCE, CDC soft delete, timestamp freshness, no retention.raw") {
      val feed = load(f)
      assert(feed.getString("raw.mode") == "SOURCE")
      assert(feed.getBoolean("raw.record_hash") && feed.getBoolean("raw.lineage_extended"))
      assert(feed.getString("raw.source_modified_column") == "cdc_src_last_updt_ts")
      Seq("raw.database", "raw.table", "raw.partitioning", "retention.raw").foreach(k =>
        assert(!feed.hasPath(k), s"$k has no effect under SOURCE and is CFG_028"))
      assert(feed.getStringList("source.incremental.watermark_columns").asScalaSeq == Seq("file_date", "file_time"))
      assert(feed.getString("source.incremental.initial_value") == "1900-01-01|00.00.00")
      assert(feed.getStringList("source.incremental.watermark_formats").asScalaSeq == Seq("yyyy-MM-dd", "HH.mm.ss"))
      assert(feed.hasPath("source.incremental.lookback.days") && !feed.hasPath("source.incremental.lookback.partitions"))
      assert(feed.getString("curated.merge.freshness.column") == "cdc_src_last_updt_ts")
      assert(feed.getString("curated.merge.freshness.compare_as") == "timestamp")
      assert(feed.getStringList("curated.merge.freshness.tie_breakers").asScalaSeq == Seq(f.lcts, "file_date", "file_time"))
      assert(feed.getString("curated.merge.deletes.mode") == "SOFT")
      assert(feed.getString("curated.merge.deletes.indicator_column") == "cdc_src_actn_cd")
      assert(feed.getStringList("curated.merge.deletes.indicator_values").asScalaSeq == Seq("dl"),
        "owner-confirmed vocabulary DL/PT/UP; lowercase because the match is lower(trim(value))")
      assert(feed.getString("reconcile.on_mismatch") == "REPORT")
      assert(feed.getInt("retention.watermarks_keep_last") == 10)
      assert(feed.getString("rejects.payload") == "MASKED")
    }
  }

  test("production feeds: source bstar_raw, control tables in bluestar_raw ONLY, curated in bluestar_current") {
    prod.foreach { f =>
      val feed = load(f)
      assert(feed.getString("source.database") == "bstar_raw")
      assert(feed.getString("curated.database") == "bluestar_current")
      assert(feed.getString("curated.table") == f.table)
      Seq("source.incremental.watermark_store.database", "rejects.database", "audit.database").foreach(k =>
        assert(feed.getString(k) == "bluestar_raw", s"$k must be the control database"))
    }
  }

  test("production feeds: the curated DDL creates exactly the tables the feeds publish, with every contract column") {
    def ddl(name: String): String = {
      val f = new File(s"$Root/ddl/$name"); assume(f.exists(), s"missing $name")
      val src = scala.io.Source.fromFile(f)
      try src.getLines().mkString("\n") finally src.close()
    }
    val text = ddl("curated_ddl.sql")
    val tables = "(?i)CREATE\\s+EXTERNAL\\s+TABLE\\s+IF\\s+NOT\\s+EXISTS\\s+(\\S+)\\s*\\(".r
      .findAllMatchIn(text).map(_.group(1)).toSet
    assert(tables == Set("bluestar_current.priv_addr", "bluestar_current.sub_prem_det"), tables.toString)
    prod.foreach { f =>
      val feed = load(f)
      assert(tables.contains(s"${feed.getString("curated.database")}.${feed.getString("curated.table")}"))
      // Column parity per table block
      val block = text.substring(text.indexOf(s"bluestar_current.${f.table} ("))
      // Cut at STORED AS, not at ';' — column COMMENTs legitimately contain semicolons.
      val stmt = block.substring(0, block.indexOf("STORED AS"))
      val cols = "(?m)^\\s*`([^`]+)`".r.findAllMatchIn(stmt).map(_.group(1)).toSet
      val contractCols = contract(feed).columns.map(_.name).toSet
      assert((contractCols -- cols).isEmpty, s"${f.table}: contract columns missing from DDL: ${contractCols -- cols}")
      Seq("record_hash", "is_deleted", "last_modified_op").foreach(c => assert(cols.contains(c), s"${f.table}: $c"))
    }
    assert(!text.linesIterator.filterNot(_.trim.startsWith("--")).mkString("\n").contains("PARTITIONED BY"),
      "latest-per-key curated tables are unpartitioned")
  }

  test("lower-env feeds are faithful twins: same contract columns, own entity and database") {
    prod.zip(e2e).foreach { case (p, t) =>
      val pf = load(p); val tf = load(t)
      val pc = contract(pf); val tc = contract(tf)
      assert(tc.columns.map(c => (c.name, c.dataType, c.required, c.category, c.businessKey, c.transform, c.sensitivity)) ==
        pc.columns.map(c => (c.name, c.dataType, c.required, c.category, c.businessKey, c.transform, c.sensitivity)),
        s"${t.entity}: contract drifted from ${p.entity}")
      assert(tc.version != pc.version, "the e2e contract is labelled as such")
      assert(tf.getString("entity") != pf.getString("entity"))
      assert(tf.getString("source.database") != pf.getString("source.database"),
        "the e2e feed must never read the production source")
      assert(tf.getString("curated.database") != pf.getString("curated.database"),
        "the e2e feed must never publish over the production curated table")
      assert(tf.getString("source.table") == s"${p.table}_src", "synthetic source table from source_test_data.sql")
    }
    // The synthetic source DDL creates exactly the tables the e2e feeds read.
    val sql = new File(s"$Root/lower-env/ddl/source_test_data.sql")
    assume(sql.exists())
    val src = scala.io.Source.fromFile(sql)
    val created = try "(?i)CREATE\\s+TABLE\\s+(\\S+)\\s*\\(".r.findAllMatchIn(src.mkString).map(_.group(1)).toSet
      finally src.close()
    e2e.foreach(t => assert(created.contains(s"bluestar_e2e.${t.table}_src"), s"${t.table}_src must be created"))
  }

  test("the hive reference config validates clean and derives the hive pattern") {
    val file = new File("../docs/examples/feed-hive-reference.conf")
    assume(file.exists())
    val feed = ConfigFactory.parseFile(file).resolve().getConfig("feeds.events_hive_feed")
    val problems = FeedCompatibilityValidator.validate(feed)
    assert(problems.isEmpty, problems.mkString("; "))
    assert(IngestionPattern.derive(feed)._1.pattern == IngestionPattern.PartitionIncremental)
    assert(contract(feed).columns.count(_.category == "audit") == 2)
  }

  private implicit class JavaListOps[A](l: java.util.List[A]) {
    def asScalaSeq: Seq[A] = { import scala.collection.JavaConverters._; l.asScala.toList }
  }
}
