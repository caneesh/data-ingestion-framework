package com.hcsc.generic.ingest.confgen.flow

import com.hcsc.generic.ingest.confgen.model.{Answers, Question, QuestionGroups => G, QuestionKind => K, Validators => V}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * File feed questionnaire: landing/managed folders, parse options, the
  * schema contract (columns accepted as CSV names or full JSON objects),
  * idempotency and record rejects.
  */
object FileQuestionFlow extends SourceQuestionFlow {

  override val sourceType: String = "file"

  private def managed(a: Answers) = a.isTrue("_file.managed")
  private def csv(a: Answers) = a.is("source.file_type", "csv") || a.is("source.file_type", "text")
  private def contract(a: Answers) = a.isTrue("_schema.use_contract")

  /** Plain CSV items become minimal column contracts. */
  private[confgen] def plainColumn(name: String): Config = CommonQuestions.plainColumn(name)

  override def questions: Seq[Question] =
    CommonQuestions.general ++ source ++ validation ++
      CommonQuestions.schemaContract(
        gateDefault = "true",
        gateHelp = "Strongly recommended: validates headers by name/alias and detects every drift.") ++
      CommonQuestions.audit ++ governance ++ CommonQuestions.destination

  private def source = Seq(
    Question("_file.managed", G.Source, "Use the managed folder lifecycle?",
      help = "Managed: landing -> inprogress -> processed (+quarantine). Otherwise a static glob path.",
      kind = K.BoolKind, default = Some("true")),
    Question("source.folders.landing", G.Source, "Landing folder",
      validate = V.nonEmpty, appliesWhen = managed),
    Question("source.folders.inprogress", G.Source, "In-progress folder",
      validate = V.nonEmpty, appliesWhen = managed),
    Question("source.folders.processed", G.Source, "Processed folder",
      validate = V.nonEmpty, appliesWhen = managed),
    Question("source.folders.quarantine", G.Source, "Quarantine folder",
      validate = V.nonEmpty, appliesWhen = managed),
    Question("source.folders.archive", G.Source, "Archive folder (blank = no archive copy)",
      required = false, default = Some(""), appliesWhen = managed),
    Question("source.path", G.Source, "Source file glob",
      help = "e.g. hdfs:///data/incoming/membership/vendor/*.csv",
      validate = V.nonEmpty, appliesWhen = a => !managed(a)),

    Question("source.file_type", G.Source, "File format",
      kind = K.Choice(Seq("csv", "json", "parquet", "orc", "text")), default = Some("csv"),
      validate = V.oneOf(Seq("csv", "json", "parquet", "orc", "text"))),
    Question("source.header", G.Source, "First row is a header?",
      kind = K.BoolKind, default = Some("true"), appliesWhen = csv),
    Question("source.delimiter", G.Source, "Field delimiter",
      default = Some(","), appliesWhen = csv,
      validate = v => if (v.nonEmpty) Right(v) else Left("delimiter must not be empty")),
    Question("source.multiline", G.Source, "Values may span multiple lines (quoted newlines)?",
      kind = K.BoolKind, default = Some("false"), appliesWhen = csv),
    Question("source.drop_trailer", G.Source, "Drop a trailer record?",
      kind = K.BoolKind, default = Some("false"), appliesWhen = csv)
  )

  private def validation = Seq(
    Question("_file.validation", G.Source, "Add pre-read file validation (quarantines bad files)?",
      kind = K.BoolKind, default = Some("false")),
    Question("source.validation.filename_pattern", G.Source, "Required filename regex (blank = any)",
      required = false, default = Some(""), appliesWhen = _.isTrue("_file.validation")),
    Question("source.validation.allowed_extensions", G.Source, "Allowed extensions",
      kind = K.ListKind, required = false,
      defaultFrom = a => a.scalar("source.file_type"),
      appliesWhen = _.isTrue("_file.validation")),
    Question("source.validation.min_size_bytes", G.Source, "Minimum file size (bytes)",
      kind = K.IntKind, default = Some("1"), validate = V.positiveInt,
      appliesWhen = _.isTrue("_file.validation"))
  )

  /** Content idempotency and record-level rejects (file feeds only). */
  private def governance = Seq(
    Question("_idem.enabled", G.Audit, "Enable content idempotency (SHA-256 file registry)?",
      kind = K.BoolKind, default = Some("true")),
    Question("idempotency.duplicate_policy", G.Audit, "Duplicate file policy",
      kind = K.Choice(Seq("SKIP", "REJECT", "REPROCESS_WITH_APPROVAL")), default = Some("SKIP"),
      validate = V.oneOf(Seq("SKIP", "REJECT", "REPROCESS_WITH_APPROVAL")),
      appliesWhen = _.isTrue("_idem.enabled")),
    Question("_rejects.enabled", G.Audit, "Enable record-level reject handling?",
      kind = K.BoolKind, default = Some("true")),
    Question("rejects.use_contract_nullability", G.Audit,
      "Derive reject rules from contract nullability?",
      kind = K.BoolKind, default = Some("true"),
      appliesWhen = a => a.isTrue("_rejects.enabled") && contract(a)),
    Question("rejects.max_reject_percent", G.Audit, "Abort when rejects exceed (% of rows)",
      kind = K.DecimalText, default = Some("5.0"), validate = V.nonNegativeNumber,
      appliesWhen = _.isTrue("_rejects.enabled"))
  )
}
