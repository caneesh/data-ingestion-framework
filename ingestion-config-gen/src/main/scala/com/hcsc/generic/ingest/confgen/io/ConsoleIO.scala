package com.hcsc.generic.ingest.confgen.io

/**
  * Console abstraction so the wizard is fully testable: production uses
  * [[AnsiConsole]] (colorized when the terminal supports it), tests use
  * [[ScriptedConsole]] with pre-programmed answers.
  */
trait ConsoleIO {
  def readLine(prompt: String): String
  def readSecret(prompt: String): String
  def println(text: String = ""): Unit

  // Semantic styles; implementations may ignore them.
  def info(text: String): Unit = println(text)
  def success(text: String): Unit = println(text)
  def warn(text: String): Unit = println(text)
  def error(text: String): Unit = println(text)
  def header(text: String): Unit = println(text)
}

/**
  * Colorized stdin/stdout console. Colors engage only when stdout is a real
  * terminal (`System.console() != null`), `NO_COLOR` is unset and TERM is not
  * "dumb" — or when explicitly forced on/off.
  */
class AnsiConsole(forceColor: Option[Boolean] = None) extends ConsoleIO {
  private val colorized: Boolean = forceColor.getOrElse {
    System.console() != null &&
      System.getenv("NO_COLOR") == null &&
      !"dumb".equalsIgnoreCase(Option(System.getenv("TERM")).getOrElse(""))
  }

  private val Esc = "\u001b"

  private def paint(code: String, text: String): String =
    if (colorized) s"$Esc[${code}m$text$Esc[0m" else text

  override def readLine(prompt: String): String = {
    Console.out.print(paint("36", prompt))
    Console.out.flush()
    Option(scala.io.StdIn.readLine()).getOrElse("")
  }

  override def readSecret(prompt: String): String = {
    val console = System.console()
    if (console != null) {
      Console.out.print(paint("36", prompt))
      Console.out.flush()
      new String(console.readPassword())
    } else readLine(prompt)
  }

  override def println(text: String): Unit = Console.out.println(text)
  override def info(text: String): Unit = Console.out.println(paint("34", text))
  override def success(text: String): Unit = Console.out.println(paint("32", text))
  override def warn(text: String): Unit = Console.out.println(paint("33", text))
  override def error(text: String): Unit = Console.out.println(paint("31", text))
  override def header(text: String): Unit = Console.out.println(paint("1;35", text))
}

/** Deterministic console for tests: answers are consumed in order and all
  * output is captured for assertions. */
class ScriptedConsole(answers: Seq[String]) extends ConsoleIO {
  private val queue = scala.collection.mutable.Queue(answers: _*)
  val transcript = new scala.collection.mutable.ArrayBuffer[String]()

  override def readLine(prompt: String): String = {
    transcript += prompt
    if (queue.isEmpty)
      throw new IllegalStateException(s"ScriptedConsole ran out of answers at prompt: $prompt")
    queue.dequeue()
  }

  override def readSecret(prompt: String): String = readLine(prompt)
  override def println(text: String): Unit = transcript += text

  def output: String = transcript.mkString("\n")
}
