#!/usr/bin/env bash
#
# One command to run a bstar load (Hive partitioned source -> curated).
#
#   scripts/run_bstar.sh e2e  priv_addr                 # synthetic source, INCR
#   scripts/run_bstar.sh prod priv_addr                 # bstar_raw.priv_addr, INCR
#   scripts/run_bstar.sh prod sub_prem_det INCR --run-id sp-prod-1
#   scripts/run_bstar.sh prod priv_addr INCR --validate-only
#   scripts/run_bstar.sh prod priv_addr INCR --stage reconcile
#   scripts/run_bstar.sh prod priv_addr INCR --stage retention --dry-run
#
# Site settings come from scripts/bstar.env (copy bstar.env.example).
# Anything after the mode is passed straight to the application.
#
# WHY THIS EXISTS: same reason as run_smartiq.sh — the generic wrapper needs
# the right feed, the schema file BESIDE it, the jar and the environment
# names assembled on every run, and each mistake fails minutes later inside
# YARN with an error that names a symptom. The hive source has LESS to get
# wrong (no password, no JDBC driver) and one thing MORE: FULL is refused
# once a watermark exists, so the mode is checked here, not after submit.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${BSTAR_ENV_FILE:-$SCRIPT_DIR/bstar.env}"

die() { echo "ERROR: $*" >&2; exit 1; }
note() { echo "  $*" >&2; }

# ---- arguments --------------------------------------------------------------
TARGET="${1:-}"
case "$TARGET" in
  e2e|prod) shift ;;
  ""|-h|--help)
    sed -n '3,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "$([ -z "$TARGET" ] && echo 2 || echo 0)" ;;
  *) die "first argument must be 'e2e' or 'prod', got '$TARGET'" ;;
esac

TABLE="${1:-}"
case "$TABLE" in
  priv_addr|sub_prem_det) shift ;;
  *) die "second argument must be 'priv_addr' or 'sub_prem_det', got '${TABLE:-<none>}'" ;;
esac

MODE="INCR"
if [[ "${1:-}" == "FULL" || "${1:-}" == "INCR" ]]; then MODE="$1"; shift; fi

# ---- required siblings ------------------------------------------------------
for sibling in run_ingest.sh ingest_submit_common.sh; do
  [[ -f "$SCRIPT_DIR/$sibling" ]] || die "missing $SCRIPT_DIR/$sibling
  This launcher needs three scripts side by side:
    run_bstar.sh  run_ingest.sh  ingest_submit_common.sh
  Copy the whole scripts/ directory, not just run_bstar.sh."
done

# ---- site settings ----------------------------------------------------------
[[ -f "$ENV_FILE" ]] || die "no site settings at $ENV_FILE
  Create it once:  cp $SCRIPT_DIR/bstar.env.example $ENV_FILE && chmod 600 $ENV_FILE"
# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

# ---- per-target identity ----------------------------------------------------
# Feed, contract and entity move together: a prod run cannot pick up the e2e
# contract (or the reverse) through a stale exported variable. The table
# name is spelled with a hyphen in file names and an underscore in entities.
FILE_TABLE="${TABLE//_/-}"
if [[ "$TARGET" == "e2e" ]]; then
  ENTITY="bstar_${TABLE}_e2e"
  FEED="feed-bstar-${FILE_TABLE}-e2e.conf"
  SCHEMA="bstar-${FILE_TABLE}-e2e-schema.conf"
else
  ENTITY="bstar_${TABLE}"
  FEED="feed-bstar-${FILE_TABLE}.conf"
  SCHEMA="bstar-${FILE_TABLE}-schema.conf"
fi

# ---- preflight --------------------------------------------------------------
FAILED=0
check() { if ! eval "$1"; then echo "  ✗ $2" >&2; FAILED=1; else echo "  ✓ $3" >&2; fi; }

echo "Preflight — $TARGET ($ENTITY, $MODE)" >&2

[[ -n "${BSTAR_CONF_DIR:-}" ]] || die "BSTAR_CONF_DIR is not set in $ENV_FILE"
CONF="$BSTAR_CONF_DIR/$FEED"
SCHEMA_PATH="$BSTAR_CONF_DIR/$SCHEMA"

check '[[ -f "$CONF" ]]'        "feed config not found: $CONF"          "feed config      $FEED"
# The feed `include`s the schema by relative name, so it must sit BESIDE it.
check '[[ -f "$SCHEMA_PATH" ]]' "schema config not found: $SCHEMA_PATH
      It must sit in the same directory as the feed config — the feed
      includes it by relative name." \
                                "schema config    $SCHEMA"
check '[[ -f "${BSTAR_JAR:-}" ]]' "jar not found: ${BSTAR_JAR:-<unset>}" \
                                "application jar  $(basename "${BSTAR_JAR:-none}")"

# No credential, no driver: the hive source reads through the Spark
# session's own metastore access. What CAN silently go wrong is the
# database: an unset override falls back to the feed's default, which is
# exactly right in prod and exactly wrong when you meant a lower env.
if [[ "$TARGET" == "prod" ]]; then
  for v in BSTAR_SOURCE_DB BSTAR_CONTROL_DB BSTAR_CURATED_DB; do
    if [[ -n "${!v:-}" ]]; then
      note "NOTE: $v='${!v}' overrides the feed's production default"
    fi
  done
  note "databases        ${BSTAR_SOURCE_DB:-bstar_raw} -> ${BSTAR_CONTROL_DB:-bluestar_raw} (control) / ${BSTAR_CURATED_DB:-bluestar_current} (curated)"
else
  note "databases        ${BSTAR_E2E_DB:-bluestar_e2e} (source, control and curated)"
  if [[ -z "${BSTAR_E2E_DB:-}" ]]; then
    note "                 (BSTAR_E2E_DB unset — the feed's default applies)"
  fi
fi

[[ $FAILED -eq 0 ]] || die "preflight failed; nothing was submitted"

# ---- FULL guard -------------------------------------------------------------
# The framework refuses FULL once a watermark row exists (HIVE_007, exit 30),
# because a FULL re-read on top of a live watermark is a reload that nobody
# asked for. Say so here — before the YARN round trip — and name the way to
# do it deliberately.
if [[ "$MODE" == "FULL" ]]; then
  note "WARN: FULL reads the ENTIRE source table. It is accepted only while"
  note "      no watermark row exists for $ENTITY; afterwards the run fails"
  note "      with HIVE_007 (exit 30). To reload from a point in time, rewind"
  note "      the watermark instead (OPERATIONS_RUNBOOK.md §2.3) and run INCR."
fi

# Staleness: a config that parses perfectly can still be an older revision
# than the repo. sync_artifacts.sh knows only the smartiq file set, so the
# check here is a direct comparison against the repo copies when the repo
# is to hand. Warn, never block — a pinned copy may be deliberate.
REPO_PARAMS="$SCRIPT_DIR/../docs/examples/bstar/params"
[[ "$TARGET" == "e2e" ]] && REPO_PARAMS="$SCRIPT_DIR/../docs/examples/bstar/lower-env/params"
if [[ -d "$REPO_PARAMS" ]]; then
  for f in "$FEED" "$SCHEMA"; do
    if [[ -f "$REPO_PARAMS/$f" ]] && ! cmp -s "$REPO_PARAMS/$f" "$BSTAR_CONF_DIR/$f"; then
      note "WARN: $BSTAR_CONF_DIR/$f differs from the repo copy ($REPO_PARAMS/$f)"
    fi
  done
fi

# ---- driver heap ------------------------------------------------------------
# sub_prem_det's first light parses ~30 GB of uncompressed text; the driver
# plan is modest but the executors are not. Nudge, never block.
if [[ "$TARGET" == "prod" && "$TABLE" == "sub_prem_det" && -z "${INGEST_EXECUTOR_MEMORY:-}" ]]; then
  note "WARN: INGEST_EXECUTOR_MEMORY is not set. The sub_prem_det source is ~30 GB"
  note "      of uncompressed text; first light parses all of it. Add to $ENV_FILE:"
  note "        INGEST_EXECUTOR_MEMORY=8g"
fi

# ---- operational override ---------------------------------------------------
if [[ -n "${INGEST_OVERRIDE_FILE:-}" ]]; then
  [[ -f "$INGEST_OVERRIDE_FILE" ]] || die "INGEST_OVERRIDE_FILE does not exist: $INGEST_OVERRIDE_FILE
  Remove it from $ENV_FILE, or point it at a real file."
  note "OVERRIDE ACTIVE  $INGEST_OVERRIDE_FILE"
  note "      Its values take precedence over the feed config. Paths set:"
  # KEY NAMES ONLY — everything after the first '=' or ':' is cut.
  grep -vE '^\s*(#|//|$)' "$INGEST_OVERRIDE_FILE" \
    | sed -e 's/[=:].*//' -e 's/^/        /' >&2
  note "      Remove it once the underlying change is deployed."
fi

# ---- submit -----------------------------------------------------------------
# No INGEST_JARS: there is no vendor driver. No --raw-flag: under
# raw.mode = SOURCE nothing is written to stamp it on.
export INGEST_DEPLOY_MODE="${BSTAR_DEPLOY_MODE:-client}"
export INGEST_EXTRA_FILES="$SCHEMA_PATH"
export INGEST_JAR="$BSTAR_JAR"
# Only cluster mode needs these forwarded; the wrapper ignores it in client
# mode, where the driver already inherits this shell. Unset names are
# reported, not forwarded — the feed default then applies.
export INGEST_ENV_VARS="BSTAR_SOURCE_DB,BSTAR_CONTROL_DB,BSTAR_CURATED_DB,BSTAR_E2E_DB,BSTAR_ALERT_WEBHOOK"

echo "Submitting ($INGEST_DEPLOY_MODE mode)" >&2
exec "$SCRIPT_DIR/run_ingest.sh" "$CONF" "$ENTITY" "$MODE" "$@"
