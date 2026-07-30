package com.hcsc.generic.ingest.confgen.wizard

import com.hcsc.generic.ingest.confgen.draft.Drafts
import com.hcsc.generic.ingest.confgen.input.CollectionInput
import com.hcsc.generic.ingest.confgen.io.ConsoleIO
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue, Question, QuestionKind}

/**
  * Drives one questionnaire session: shows only applicable questions, walks
  * them in group order with a progress indicator, validates every answer as
  * it is typed, and auto-saves a draft after each answer so a session can be
  * resumed. Special inputs at any prompt:
  *
  *   `?`      show the question's help text and re-ask
  *   `:save`  save the draft and stop (resume later with --draft)
  */
class Wizard(io: ConsoleIO, draftPath: Option[String] = None) {

  class SessionSaved extends RuntimeException("session saved")

  /** Runs the flow; `answers` may be pre-populated from a draft or an
    * answers file — those questions are not asked again. */
  def run(sourceType: String, questions: Seq[Question], answers: Answers): Answers = {
    val groups = questions.map(_.group).distinct
    try {
      groups.zipWithIndex.foreach { case (group, index) =>
        val pending = questions.filter(q => q.group == group)
        var headerShown = false
        pending.foreach { q =>
          if (q.appliesWhen(answers) && !answers.contains(q.id)) {
            if (!headerShown) {
              io.header(s"\n[${index + 1}/${groups.size}] $group")
              headerShown = true
            }
            ask(q, answers)
            // Drafts never persist secret answers: a resumed session re-asks
            // them instead of storing credentials in a plaintext JSON file.
            val secretIds = questions.filter(_.secret).map(_.id).toSet
            draftPath.foreach(Drafts.save(_, sourceType, answers, excludeIds = secretIds))
          }
        }
      }
      answers
    } catch {
      case _: SessionSaved =>
        io.success(s"Draft saved${draftPath.fold("")(p => s" to $p")}. Resume with --draft.")
        answers
    }
  }

  private def ask(q: Question, answers: Answers): Unit = {
    val effectiveDefault = q.defaultFrom(answers).orElse(q.default)
    val choiceHint = q.kind match {
      case QuestionKind.Choice(options) => s" (${options.filter(_.nonEmpty).mkString("/")})"
      case QuestionKind.BoolKind => " (y/n)"
      case QuestionKind.ListKind | _: QuestionKind.BlockList => " (CSV, JSON or @file)"
      case _ => ""
    }
    val defaultHint = effectiveDefault.map(d => s" [${if (d.isEmpty) "blank" else d}]").getOrElse("")
    val prompt = s"${q.prompt}$choiceHint$defaultHint: "

    var done = false
    while (!done) {
      val raw = (if (q.secret) io.readSecret(prompt) else io.readLine(prompt)).trim
      raw match {
        case "?" =>
          io.info(if (q.help.nonEmpty) q.help else "(no additional help for this question)")
        case ":save" =>
          throw new SessionSaved
        case "" if effectiveDefault.isDefined =>
          done = accept(q, effectiveDefault.get, answers)
        case "" if !q.required =>
          done = true // optional question skipped: no answer recorded
        case "" =>
          io.error("An answer is required.")
        case value =>
          done = accept(q, value, answers)
      }
    }
  }

  /** Normalizes + validates one raw answer; false re-asks the question. */
  private def accept(q: Question, raw: String, answers: Answers): Boolean = q.kind match {
    case QuestionKind.ListKind | _: QuestionKind.BlockList =>
      CollectionInput.parse(raw) match {
        case Left(problem) =>
          io.error(problem); false
        case Right(parsed) =>
          val value = (q.kind, parsed.value) match {
            // Plain items offered to a BlockList question are wrapped into blocks.
            case (QuestionKind.BlockList(wrap), AnswerValue.Items(items)) =>
              AnswerValue.Blocks(items.map(wrap))
            case (_, v) => v
          }
          val flat = value match {
            case AnswerValue.Items(items) => items.mkString(",")
            case _ => raw
          }
          q.validate(flat) match {
            case Left(problem) => io.error(problem); false
            case Right(_) =>
              if (parsed.preview.nonEmpty)
                io.info(s"  -> ${parsed.sourceDescription}: ${parsed.preview.mkString(", ")}")
              answers.put(q.id, value)
              true
          }
      }

    case QuestionKind.BoolKind =>
      raw.toLowerCase match {
        case "y" | "yes" | "true" => finish(q, "true", answers)
        case "n" | "no" | "false" => finish(q, "false", answers)
        case other => io.error(s"'$other' is not y/n"); false
      }

    case _ => finish(q, raw, answers)
  }

  private def finish(q: Question, value: String, answers: Answers): Boolean =
    q.validate(value) match {
      case Left(problem) => io.error(problem); false
      case Right(normalized) => answers.put(q.id, AnswerValue.Scalar(normalized)); true
    }
}
