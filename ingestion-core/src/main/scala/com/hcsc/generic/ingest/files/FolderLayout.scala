package com.hcsc.generic.ingest.files

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path

/**
  * Managed folder lifecycle for file-based feeds:
  *
  *   landing -> inprogress -> processed (and optionally archive)
  *                        \-> quarantine (validation / duplicate failures)
  *
  * Configured under source.folders; feeds using a static source.path keep the
  * legacy unmanaged behavior.
  */
final case class FolderLayout(
  landing: Path,
  inprogress: Path,
  processed: Path,
  quarantine: Path,
  archive: Option[Path]
)

object FolderLayout {
  def parse(sourceConf: Config): Option[FolderLayout] = {
    if (!sourceConf.hasPath("folders")) return None
    val f = sourceConf.getConfig("folders")
    Some(FolderLayout(
      landing = new Path(f.getString("landing")),
      inprogress = new Path(f.getString("inprogress")),
      processed = new Path(f.getString("processed")),
      quarantine = new Path(f.getString("quarantine")),
      archive = ConfigUtils.optString(f, "archive").map(new Path(_))
    ))
  }
}
