#!/usr/bin/env bash
#
# Copy the run-time artifacts from this checkout into the directory the
# launcher reads, and say exactly what changed.
#
#   scripts/sync_artifacts.sh                 # sync into $SMARTIQ_CONF_DIR
#   scripts/sync_artifacts.sh --check         # report drift, change nothing
#   scripts/sync_artifacts.sh --to /path      # sync somewhere else
#   scripts/sync_artifacts.sh --from /bundle  # source is an extracted bundle,
#                                             # not a git checkout
#
# TWO DEPLOYMENT SHAPES. On a machine with the repo, sources are read from
# the checkout. On a server that only ever receives an EXPORTED bundle —
# no git, no repo — point --from at the extracted directory (or set
# SMARTIQ_BUNDLE_DIR). The bundle carries MANIFEST.sha256 and
# SOURCE_COMMIT, so the server can still say which commit it is running and
# whether a file was modified after export.
#
# WHY THIS EXISTS: four consecutive lower-environment runs failed because a
# file in the run directory was older than the repo — a stale jar, then a
# feed config still deriving the previous partition column, then a schema
# file with a since-removed contract rule. Each failed minutes into a YARN
# submission with an error describing a symptom (a missing column, an
# unresolvable include) rather than the staleness that caused it. Copying by
# hand is what kept going wrong, so this does the copying and, more
# importantly, TELLS YOU when something was out of date.
#
# It never touches scripts/smartiq.env (site settings, gitignored) and never
# builds — an absent or stale jar is reported, not silently rebuilt, because
# rebuilding is a decision about which commit you are deploying.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${SMARTIQ_ENV_FILE:-$SCRIPT_DIR/smartiq.env}"

MODE="sync"; TARGET=""; BUNDLE="${SMARTIQ_BUNDLE_DIR:-}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) MODE="check"; shift ;;
    --to)    TARGET="${2:?--to needs a directory}"; shift 2 ;;
    --from)  BUNDLE="${2:?--from needs a directory}"; shift 2 ;;
    -h|--help) sed -n '3,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "ERROR: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

# Destination: --to wins, else SMARTIQ_CONF_DIR from the settings file.
if [[ -z "$TARGET" ]]; then
  [[ -f "$ENV_FILE" ]] || { echo "ERROR: no $ENV_FILE and no --to <dir>." >&2
    echo "       cp $SCRIPT_DIR/smartiq.env.example $ENV_FILE  and set SMARTIQ_CONF_DIR" >&2; exit 2; }
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a
  TARGET="${SMARTIQ_CONF_DIR:-}"
  [[ -n "$TARGET" ]] || { echo "ERROR: SMARTIQ_CONF_DIR is not set in $ENV_FILE" >&2; exit 2; }
fi
[[ -d "$TARGET" ]] || { echo "ERROR: destination is not a directory: $TARGET" >&2; exit 2; }

# An operator's INGEST_OVERRIDE_FILE is deliberately NOT in this list. It is
# authored on the server for a situation the repo does not know about, so a
# sync must never overwrite it — and it must never be resurrected by a sync
# after the operator deletes it.
ARTIFACTS=(
  feed-smartiq-pdp-e2e.conf
  smartiq-pdp-e2e-schema.conf
  feed-smartiq-pdp.conf
  smartiq-pdp-schema.conf
)
JAR_NAME="ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

# A bundle is FLAT (as exported); a checkout keeps the repo layout. Fall
# back to the bundle automatically when this is not a git checkout, so the
# server needs no extra flag once SMARTIQ_BUNDLE_DIR is set.
if [[ -z "$BUNDLE" && ! -d "$REPO/.git" ]]; then
  # The scripts themselves ship inside the bundle, so their parent is it.
  [[ -f "$SCRIPT_DIR/../feed-smartiq-pdp.conf" ]] && BUNDLE="$(cd "$SCRIPT_DIR/.." && pwd)"
fi

SOURCES=()
if [[ -n "$BUNDLE" ]]; then
  [[ -d "$BUNDLE" ]] || { echo "ERROR: --from is not a directory: $BUNDLE" >&2; exit 2; }
  for a in "${ARTIFACTS[@]}"; do SOURCES+=("$BUNDLE/$a"); done
  [[ -f "$BUNDLE/$JAR_NAME" ]] && SOURCES+=("$BUNDLE/$JAR_NAME")
  SOURCE_DESC="bundle $BUNDLE"
  [[ -f "$BUNDLE/SOURCE_COMMIT" ]] &&
    SOURCE_DESC="$SOURCE_DESC (exported from $(head -1 "$BUNDLE/SOURCE_COMMIT"))"
else
  EX="$REPO/docs/examples/smartiq_pdp"
  SOURCES=(
    "$EX/lower-env/params/feed-smartiq-pdp-e2e.conf"
    "$EX/lower-env/params/smartiq-pdp-e2e-schema.conf"
    "$EX/params/feed-smartiq-pdp.conf"
    "$EX/params/smartiq-pdp-schema.conf"
    "${SMARTIQ_JAR_SOURCE:-$REPO/ingestion-app/target/$JAR_NAME}"
  )
  SOURCE_DESC="repo $REPO ($(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo 'no git'))"
fi

echo "source $SOURCE_DESC"
echo "target $TARGET"
[[ "$MODE" == "check" ]] && echo "(--check: reporting only)"
echo

CHANGED=0; MISSING=0
for SRC in "${SOURCES[@]}"; do
  NAME="$(basename "$SRC")"
  DST="$TARGET/$NAME"
  if [[ ! -f "$SRC" ]]; then
    printf "  MISSING SOURCE  %-52s %s\n" "$NAME" "$SRC"
    # An absent jar is the common case (never built, or built elsewhere) and
    # is worth failing on: every other artifact is useless without it.
    MISSING=1; continue
  fi
  if [[ ! -f "$DST" ]]; then
    STATUS="NEW"
  elif cmp -s "$SRC" "$DST"; then
    printf "  same            %s\n" "$NAME"; continue
  else
    STATUS="STALE"
  fi
  CHANGED=1
  if [[ "$MODE" == "check" ]]; then
    printf "  %-15s %s\n" "$STATUS" "$NAME"
  else
    cp -p "$SRC" "$DST"
    printf "  %-15s %s\n" "${STATUS}→copied" "$NAME"
  fi
done

# Settings drift: smartiq.env is intentionally never overwritten (it holds
# site values), so a key ADDED to the template after the file was created
# is invisible — which is exactly how a needed setting goes missing.
TEMPLATE="$SCRIPT_DIR/smartiq.env.example"
[[ -f "$TEMPLATE" ]] || TEMPLATE="${BUNDLE:-}/scripts/smartiq.env.example"
if [[ -f "$ENV_FILE" && -f "$TEMPLATE" ]]; then
  MISSING_KEYS=""
  while IFS= read -r key; do
    grep -qE "^[[:space:]]*${key}=" "$ENV_FILE" || MISSING_KEYS="$MISSING_KEYS $key"
  done < <(grep -oE '^[A-Z_]+=' "$TEMPLATE" | tr -d '=')
  if [[ -n "$MISSING_KEYS" ]]; then
    echo
    echo "  settings in the template but NOT in $(basename "$ENV_FILE"):"
    for k in $MISSING_KEYS; do echo "    $k"; done
    echo "  (this file is never overwritten — add anything you need by hand)"
  fi
fi

echo
if [[ $MISSING -eq 1 ]]; then
  echo "One or more sources are missing. If it is the assembly jar, build it:" >&2
  echo "  (cd $REPO && mvn clean install)" >&2
  exit 1
fi
if [[ $CHANGED -eq 0 ]]; then
  echo "Everything in $TARGET matches the repo."
elif [[ "$MODE" == "check" ]]; then
  echo "Drift found. Run without --check to copy."
  exit 1
else
  echo "Sync complete."
  echo "NOTE: a refreshed jar or feed config can change the RAW/CURATED schema"
  echo "      (partition column, contract columns). If a run then fails with a"
  echo "      missing or unexpected column, the Hive table predates the config —"
  echo "      see the pre-flight checklist in the lower-env test plan."
fi
