package com.hcsc.generic.ingest.files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.ByteBuffer
import java.nio.charset.{Charset, CodingErrorAction}
import java.security.MessageDigest

/** Hadoop FileSystem helpers shared by validation, lifecycle and idempotency.
  * Work identically against HDFS and local file:// paths (tests). */
object FsUtils {

  def fileSystem(path: Path, conf: Configuration): FileSystem = path.getFileSystem(conf)

  def listFiles(fs: FileSystem, dir: Path): Seq[FileStatus] = {
    if (!fs.exists(dir)) Seq.empty
    else fs.listStatus(dir)
      .filter(_.isFile)
      // Skip hidden/system files (checksum sidecars, _SUCCESS markers, etc.)
      .filterNot(s => s.getPath.getName.startsWith(".") || s.getPath.getName.startsWith("_"))
      .toSeq.sortBy(_.getPath.getName)
  }

  /** Moves a file into destDir (creating it if needed), returning the new path.
    * A same-named file already in destDir is never overwritten — the incoming
    * file gets a numeric suffix instead (member.csv -> member.csv.1), so a
    * re-delivered file can never destroy an earlier processed/quarantined one.
    * Falls back to copy+delete when rename is not supported across volumes. */
  def moveToDir(fs: FileSystem, file: Path, destDir: Path): Path = {
    if (!fs.exists(destDir)) fs.mkdirs(destDir)
    var dest = new Path(destDir, file.getName)
    if (fs.makeQualified(dest) == fs.makeQualified(file)) return file // already in place
    var suffix = 1
    while (fs.exists(dest)) {
      dest = new Path(destDir, s"${file.getName}.$suffix")
      suffix += 1
    }
    if (!fs.rename(file, dest)) {
      FileUtil.copy(fs, file, fs, dest, true, fs.getConf)
    }
    dest
  }

  def copyToDir(fs: FileSystem, file: Path, destDir: Path): Path = {
    if (!fs.exists(destDir)) fs.mkdirs(destDir)
    val dest = new Path(destDir, file.getName)
    FileUtil.copy(fs, file, fs, dest, false, true, fs.getConf)
    dest
  }

  /** Streaming content digest (default SHA-256), returned as lowercase hex. */
  def checksum(fs: FileSystem, file: Path, algorithm: String = "SHA-256"): String = {
    val digest = MessageDigest.getInstance(algorithm)
    val in = fs.open(file)
    try {
      val buffer = new Array[Byte](64 * 1024)
      var read = in.read(buffer)
      while (read > 0) {
        digest.update(buffer, 0, read)
        read = in.read(buffer)
      }
    } finally in.close()
    digest.digest().map("%02x".format(_)).mkString
  }

  def readFirstLine(fs: FileSystem, file: Path, charset: String = "UTF-8"): Option[String] = {
    val in = fs.open(file)
    val reader = new BufferedReader(new InputStreamReader(in, charset))
    try Option(reader.readLine())
    finally reader.close()
  }

  /** Last non-empty line, read from a bounded tail window of the file. */
  def readLastLine(fs: FileSystem, file: Path, charset: String = "UTF-8", window: Int = 64 * 1024): Option[String] = {
    val len = fs.getFileStatus(file).getLen
    if (len == 0) return None
    val start = math.max(0L, len - window)
    val in = fs.open(file)
    try {
      in.seek(start)
      val bytes = new Array[Byte]((len - start).toInt)
      in.readFully(bytes)
      new String(bytes, charset)
        .split("\r?\n")
        .reverseIterator
        .find(_.trim.nonEmpty)
    } finally in.close()
  }

  /** Strictly decodes up to sampleBytes with the given charset; returns the
    * decoding error message if the content is not valid in that encoding. */
  def encodingError(fs: FileSystem, file: Path, charsetName: String, sampleBytes: Int = 1024 * 1024): Option[String] = {
    val len = fs.getFileStatus(file).getLen
    val toRead = math.min(len, sampleBytes.toLong).toInt
    if (toRead == 0) return None
    val bytes = new Array[Byte](toRead)
    val in = fs.open(file)
    try in.readFully(bytes)
    finally in.close()

    val decoder = Charset.forName(charsetName).newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try {
      decoder.decode(ByteBuffer.wrap(bytes))
      None
    } catch {
      case e: Exception => Some(s"Content is not valid $charsetName: ${e.getMessage}")
    }
  }
}
