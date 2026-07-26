package com.hcsc.generic.ingest.schema

import org.scalatest.funsuite.AnyFunSuite

class HeaderNormalizerTest extends AnyFunSuite {

  test("spec examples normalize correctly") {
    assert(HeaderNormalizer.normalize(" HIOS ID ") == "hios_id")
    assert(HeaderNormalizer.normalize("Plan HIOS-ID") == "plan_hios_id")
    assert(HeaderNormalizer.normalize("Subscriber.ID") == "subscriber_id")
    assert(HeaderNormalizer.normalize("  MEMBER  NUMBER ") == "member_number")
  }

  test("trims whitespace and lowercases") {
    assert(HeaderNormalizer.normalize("  Foo Bar  ") == "foo_bar")
    assert(HeaderNormalizer.normalize("UPPER") == "upper")
  }

  test("replaces special characters with single underscores") {
    assert(HeaderNormalizer.normalize("a!@#$b") == "a_b")
    assert(HeaderNormalizer.normalize("a - _ - b") == "a_b")
  }

  test("collapses repeated separators and strips edge underscores") {
    assert(HeaderNormalizer.normalize("__foo__bar__") == "foo_bar")
    assert(HeaderNormalizer.normalize("...leading.trailing...") == "leading_trailing")
  }

  test("already-normalized names pass through unchanged") {
    assert(HeaderNormalizer.normalize("subscriber_id") == "subscriber_id")
  }

  test("invisible unicode characters are normalized away") {
    assert(HeaderNormalizer.normalize("\uFEFFSubscriber ID") == "subscriber_id") // BOM
    assert(HeaderNormalizer.normalize("Subscriber\u200BID") == "subscriber_id") // zero-width space
    assert(HeaderNormalizer.normalize("HIOS\u00A0ID") == "hios_id")             // non-breaking space
    assert(HeaderNormalizer.normalize("HIOS ID\r") == "hios_id")                // carriage return
    assert(HeaderNormalizer.normalize("HIOS ID\r\n") == "hios_id")
  }
}
