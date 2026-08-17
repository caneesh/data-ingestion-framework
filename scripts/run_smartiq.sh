#!/usr/bin/env bash
#
# One command to run a SmartIQ_PDP load.
#
#   scripts/run_smartiq.sh e2e                    # 11-column test feed, INCR
#   scripts/run_smartiq.sh prod                   # 364-column feed, INCR
#   scripts/run_smartiq.sh e2e  INCR --run-id e2e-1
#   scripts/run_smartiq.sh prod INCR --validate-only
#   scripts/run_smartiq.sh prod INCR --dry-run
#
# Site settings come from scripts/smartiq.env (copy smartiq.env.example).
# Anything after the mode is passed straight to the application.
#
# WHY THIS EXISTS: the generic wrapper needs five environment variables and
# three paths assembled correctly on every run. Getting any of them wrong
# fails minutes later with an error that names a symptom rather than the
# cause — a missing schema file reports "include was not found", a missing
# driver reports JDBC_001, an unexported variable silently connects to the
# wrong database. Every check below corresponds to a failure that actually
# happened during lower-environment testing, and each one costs milliseconds
# here instead of a round trip through YARN.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SMARTIQ_ENV_FILE:-$SCRIPT_DIR/smartiq.env}"

die() { echo "ERROR: $*" >&2; exit 1; }
note() { echo "  $*" >&2; }

# ---- arguments --------------------------------------------------------------
TARGET="${1:-}"
case "$TARGET" in
  e2e|prod) shift ;;
  ""|-h|--help)
    sed -n '3,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "$([ -z "$TARGET" ] && echo 2 || echo 0)" ;;
  *) die "first argument must be 'e2e' or 'prod', got '$TARGET'" ;;
esac

MODE="INCR"
if [[ "${1:-}" == "FULL" || "${1:-}" == "INCR" ]]; then MODE="$1"; shift; fi

# ---- required siblings ------------------------------------------------------
# This launcher delegates to run_ingest.sh, which sources
# ingest_submit_common.sh. A deployment that copies only THIS file gets a
# bare "No such file or directory" naming a path, with no hint that two
# more scripts belong beside it.
for sibling in run_ingest.sh ingest_submit_common.sh; do
  [[ -f "$SCRIPT_DIR/$sibling" ]] || die "missing $SCRIPT_DIR/$sibling
  This launcher needs three scripts side by side:
    run_smartiq.sh  run_ingest.sh  ingest_submit_common.sh
  Copy the whole scripts/ directory, not just run_smartiq.sh."
done

# ---- site settings ----------------------------------------------------------
[[ -f "$ENV_FILE" ]] || die "no site settings at $ENV_FILE
  Create it once:  cp $SCRIPT_DIR/smartiq.env.example $ENV_FILE && chmod 600 $ENV_FILE"
# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

# ---- per-target identity ----------------------------------------------------
# Feed, contract, entity and SOURCE TABLE move together, so a prod run cannot
# read the test table (or the reverse) through a stale exported variable.
if [[ "$TARGET" == "e2e" ]]; then
  ENTITY="smartiq_pdp_e2e"
  FEED="feed-smartiq-pdp-e2e.conf"
  SCHEMA="smartiq-pdp-e2e-schema.conf"
  export SMARTIQ_TABLE="${SMARTIQ_TABLE_E2E:-dbo.SmartIQ_PDP_E2E}"
else
  ENTITY="smartiq_pdp"
  FEED="feed-smartiq-pdp.conf"
  SCHEMA="smartiq-pdp-schema.conf"
  export SMARTIQ_TABLE="${SMARTIQ_TABLE_PROD:-dbo.SmartIQ_PDP}"
fi

# ---- preflight --------------------------------------------------------------
FAILED=0
check() { if ! eval "$1"; then echo "  ✗ $2" >&2; FAILED=1; else echo "  ✓ $3" >&2; fi; }

echo "Preflight — $TARGET ($ENTITY, $MODE)" >&2

[[ -n "${SMARTIQ_CONF_DIR:-}" ]] || die "SMARTIQ_CONF_DIR is not set in $ENV_FILE"
CONF="$SMARTIQ_CONF_DIR/$FEED"
SCHEMA_PATH="$SMARTIQ_CONF_DIR/$SCHEMA"

check '[[ -f "$CONF" ]]'        "feed config not found: $CONF"          "feed config      $FEED"
# The feed `include`s the schema by relative name, so it must sit BESIDE it.
check '[[ -f "$SCHEMA_PATH" ]]' "schema config not found: $SCHEMA_PATH
      It must sit in the same directory as the feed config — the feed
      includes it by relative name." \
                                "schema config    $SCHEMA"
check '[[ -f "${SMARTIQ_JAR:-}" ]]' "jar not found: ${SMARTIQ_JAR:-<unset>}" \
                                "application jar  $(basename "${SMARTIQ_JAR:-none}")"
check '[[ -f "${SMARTIQ_JDBC_DRIVER:-}" ]]' \
      "JDBC driver not found: ${SMARTIQ_JDBC_DRIVER:-<unset>}
      Without it the run fails with JDBC_001 after connecting." \
                                "JDBC driver      $(basename "${SMARTIQ_JDBC_DRIVER:-none}")"

for v in SMARTIQ_HOST SMARTIQ_DB SMARTIQ_USER; do
  if [[ -z "${!v:-}" ]]; then
    echo "  ✗ $v is empty — the feed would silently fall back to its built-in default" >&2
    FAILED=1
  fi
done
# The placeholder is what appears in the JDBC URL when nothing was forwarded.
if [[ "${SMARTIQ_HOST:-}" == UNSET-* || "${SMARTIQ_HOST:-}" == SQLHOST* ]]; then
  echo "  ✗ SMARTIQ_HOST is still the placeholder '$SMARTIQ_HOST'" >&2
  FAILED=1
fi
[[ $FAILED -eq 0 ]] && echo "  ✓ connection      $SMARTIQ_USER@$SMARTIQ_HOST:${SMARTIQ_PORT:-1433}/$SMARTIQ_DB → $SMARTIQ_TABLE" >&2

[[ $FAILED -eq 0 ]] || die "preflight failed; nothing was submitted"

# Staleness is the failure this preflight cannot see by inspecting one file:
# a config that parses perfectly can still be an older revision than the
# table it writes to. Warn (never block — the operator may be running a
# deliberately pinned copy) and only when the repo is actually to hand.
if [[ -x "$SCRIPT_DIR/sync_artifacts.sh" ]] && [[ -d "$SCRIPT_DIR/../.git" ]]; then
  if ! "$SCRIPT_DIR/sync_artifacts.sh" --check --to "$SMARTIQ_CONF_DIR" >/dev/null 2>&1; then
    note "WARN: $SMARTIQ_CONF_DIR differs from the repo — run"
    note "      scripts/sync_artifacts.sh --check   to see what, or"
    note "      scripts/sync_artifacts.sh           to refresh"
  fi
fi

# ---- credential -------------------------------------------------------------
# Prompted, never echoed, never written to a file or a command line.
if [[ -z "${SMARTIQ_DB_PASSWORD:-}" ]]; then
  [[ -t 0 ]] || die "SMARTIQ_DB_PASSWORD is unset and there is no terminal to prompt on.
  For unattended runs, export it from your secret store in the job definition,
  or switch the feed's password provider to cyberark / azure_keyvault / conjur."
  read -rs -p "  SQL password for $SMARTIQ_USER: " SMARTIQ_DB_PASSWORD; echo >&2
  [[ -n "$SMARTIQ_DB_PASSWORD" ]] || die "empty password"
  export SMARTIQ_DB_PASSWORD
fi

# ---- driver heap ------------------------------------------------------------
# The 364-column feed builds a large ANALYZER plan on the driver before any
# row is read. In cluster mode the driver is a YARN container at the queue
# default, which is routinely too small — the symptom is an
# OutOfMemoryError in driver threads (heartbeater, BlockManagerMaster) with
# no reference to the pipeline at all, so it reads like a cluster fault
# rather than a sizing one.
if [[ "$TARGET" == "prod" && -z "${INGEST_DRIVER_MEMORY:-}" ]]; then
  note "WARN: INGEST_DRIVER_MEMORY is not set for the 364-column feed."
  note "      The driver builds a large plan before reading any row; the"
  note "      default heap has not been enough. Add to $ENV_FILE:"
  note "        INGEST_DRIVER_MEMORY=4g"
fi

# ---- operational override ---------------------------------------------------
# Surfaced BEFORE submitting, because a file that silently changes behaviour is
# exactly the drift this pipeline has spent weeks chasing. The wrapper ships it
# and the driver logs every overridden path; this note is so the operator at
# the terminal knows it is in play without reading the driver log.
if [[ -n "${INGEST_OVERRIDE_FILE:-}" ]]; then
  [[ -f "$INGEST_OVERRIDE_FILE" ]] || die "INGEST_OVERRIDE_FILE does not exist: $INGEST_OVERRIDE_FILE
  Remove it from $ENV_FILE, or point it at a real file. It is not skipped
  silently: the values it carries are the whole reason it was set."
  note "OVERRIDE ACTIVE  $INGEST_OVERRIDE_FILE"
  note "      Its values take precedence over the feed config. Paths set:"
  # KEY NAMES ONLY — an override may legitimately carry a credential, so
  # everything after the first '=' or ':' is cut before anything is printed.
  grep -vE '^\s*(#|//|$)' "$INGEST_OVERRIDE_FILE" \
    | sed -e 's/[=:].*//' -e 's/^/        /' >&2
  note "      Remove it once the underlying change is deployed."
fi

# ---- raw flag ---------------------------------------------------------------
# Stamped on every RAW row as file_type, so a full load and an incremental are
# distinguishable in the data itself rather than only by joining the ledger.
# Derived from the mode so the two cannot drift apart.
#
# It is NOT a partition key on purpose: RAW retention drops partitions on a
# single-key ingest_dt layout only, so a second key would disable purging
# altogether, and a two-value column prunes almost nothing.
#
# CONSISTENCY MATTERS: --resume-ingest-dt replays a RAW partition filtered by
# `file_type = <raw flag>`, so a replay must use the SAME mode as the run that
# wrote the rows, or it selects nothing. Deriving the flag from the mode is
# what keeps that true without anyone having to remember it.
RAW_FLAG_ARGS=()
if [[ " $* " == *" --raw-flag "* ]]; then
  note "--raw-flag given explicitly; not deriving one from the mode"
else
  if [[ "$MODE" == "FULL" ]]; then RAW_FLAG="${SMARTIQ_RAW_FLAG_FULL:-F}"
  else                            RAW_FLAG="${SMARTIQ_RAW_FLAG_INCR:-I}"; fi
  RAW_FLAG_ARGS=(--raw-flag "$RAW_FLAG")
  note "raw flag         file_type='$RAW_FLAG' (from mode $MODE)"
fi

# ---- submit -----------------------------------------------------------------
export INGEST_DEPLOY_MODE="${SMARTIQ_DEPLOY_MODE:-client}"
export INGEST_JARS="$SMARTIQ_JDBC_DRIVER"
export INGEST_EXTRA_FILES="$SCHEMA_PATH"
export INGEST_JAR="$SMARTIQ_JAR"
# Only cluster mode needs these forwarded; the wrapper ignores it in client
# mode, where the driver already inherits this shell.
export INGEST_ENV_VARS="SMARTIQ_HOST,SMARTIQ_PORT,SMARTIQ_DB,SMARTIQ_TABLE,SMARTIQ_USER,SMARTIQ_DB_PASSWORD"

echo "Submitting ($INGEST_DEPLOY_MODE mode)" >&2
exec "$SCRIPT_DIR/run_ingest.sh" "$CONF" "$ENTITY" "$MODE" \
  ${RAW_FLAG_ARGS[@]+"${RAW_FLAG_ARGS[@]}"} "$@"
