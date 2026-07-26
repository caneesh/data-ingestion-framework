package com.hcsc.generic.ingest.schema

/**
  * Centralized header normalization, used by both the alias resolution in
  * SchemaContract and connector-side header cleanup so every comparison in
  * the framework agrees on what a header "means".
  *
  * Rules:
  *   - trim leading/trailing whitespace
  *   - lowercase
  *   - replace runs of spaces / non-alphanumeric characters with a single '_'
  *   - strip leading/trailing underscores
  *
  * Examples:
  *   " HIOS ID "         -> "hios_id"
  *   "Plan HIOS-ID"      -> "plan_hios_id"
  *   "Subscriber.ID"     -> "subscriber_id"
  *   "  MEMBER  NUMBER " -> "member_number"
  */
object HeaderNormalizer {
  def normalize(header: String): String =
    header.trim.toLowerCase
      .replaceAll("[^a-z0-9]+", "_")
      .replaceAll("^_+|_+$", "")
}
