#!/usr/bin/env bash
#
# Build the deployable bundle: everything the server needs, in one archive,
# stamped with the commit it came from.
#
#   scripts/build_bundle.sh                  # -> ./smartiq_bundle_<commit>.zip
#   scripts/build_bundle.sh --out /tmp       # write it somewhere else
#   scripts/build_bundle.sh --with-jar       # include the assembly jar (~65MB)
#
# WHY: the runtime server is not a git checkout. Artifacts are exported from
# git and copied across, which leaves no way to answer the two questions
# that have repeatedly cost a debugging round — WHICH COMMIT is this, and
# HAS ANYTHING BEEN EDITED SINCE. The bundle answers both: SOURCE_COMMIT
# records the origin, MANIFEST.sha256 lets the server verify every file
# without needing git.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"

OUT_DIR="$PWD"; WITH_JAR=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="${2:?--out needs a directory}"; shift 2 ;;
    --with-jar) WITH_JAR=1; shift ;;
    -h|--help) sed -n '3,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "ERROR: unknown argument '$1'" >&2; exit 2 ;;
  esac
done
[[ -d "$OUT_DIR" ]] || { echo "ERROR: --out is not a directory: $OUT_DIR" >&2; exit 2; }

COMMIT="$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)"
DIRTY=""
if ! git -C "$REPO" diff --quiet 2>/dev/null || ! git -C "$REPO" diff --cached --quiet 2>/dev/null; then
  DIRTY=" (UNCOMMITTED CHANGES PRESENT)"
  echo "WARN: the working tree has uncommitted changes; the bundle will not" >&2
  echo "      correspond exactly to commit $COMMIT." >&2
fi

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/scripts"

EX="$REPO/docs/examples/smartiq_pdp"
cp "$EX/feed-smartiq-pdp.conf" "$EX/smartiq-pdp-schema.conf" \
   "$EX/raw_ddl.sql" "$EX/curated_ddl.sql" \
   "$EX/lower-env/feed-smartiq-pdp-e2e.conf" "$EX/lower-env/smartiq-pdp-e2e-schema.conf" \
   "$EX/lower-env/raw_ddl_e2e.sql" "$EX/lower-env/curated_ddl_e2e.sql" \
   "$EX/lower-env/source_test_data.sql" "$EX/lower-env/LOWER_ENV_TEST_PLAN.md" \
   "$REPO/ddl/ingest_audit.sql" "$STAGE/"
cp "$SCRIPT_DIR"/ingest_submit_common.sh "$SCRIPT_DIR"/run_ingest.sh \
   "$SCRIPT_DIR"/run_raw.sh "$SCRIPT_DIR"/run_curated_pending.sh \
   "$SCRIPT_DIR"/run_smartiq.sh "$SCRIPT_DIR"/sync_artifacts.sh \
   "$SCRIPT_DIR"/smartiq.env.example "$STAGE/scripts/"

if [[ $WITH_JAR -eq 1 ]]; then
  JAR="$REPO/ingestion-app/target/ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
  [[ -f "$JAR" ]] || { echo "ERROR: jar not built: $JAR" >&2
                       echo "       (cd $REPO && mvn clean install)" >&2; exit 1; }
  cp "$JAR" "$STAGE/"
fi

echo "$COMMIT$DIRTY" > "$STAGE/SOURCE_COMMIT"
date -u +"%Y-%m-%dT%H:%M:%SZ" >> "$STAGE/SOURCE_COMMIT"

# Checksums so the server can prove nothing was edited after export.
( cd "$STAGE" && find . -type f ! -name MANIFEST.sha256 -print0 \
    | sort -z | xargs -0 sha256sum > MANIFEST.sha256 )

ZIP="$OUT_DIR/smartiq_bundle_${COMMIT}.zip"
rm -f "$ZIP"
( cd "$STAGE" && zip -q -r "$ZIP" . )

echo "bundle  $ZIP"
echo "commit  $COMMIT$DIRTY"
echo "files   $(grep -c . "$STAGE/MANIFEST.sha256")"
[[ $WITH_JAR -eq 0 ]] && echo "note    jar NOT included; use --with-jar, or copy it separately"
echo
echo "On the server:"
echo "  unzip -o $(basename "$ZIP") -d /path/to/bundle"
echo "  cd /path/to/bundle && sha256sum -c MANIFEST.sha256   # verify the transfer"
echo "  scripts/sync_artifacts.sh --from . --to \$SMARTIQ_CONF_DIR"
