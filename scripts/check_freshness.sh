#!/usr/bin/env bash
#
# Ledger freshness check — the Control-M audit-folder job that closes the
# one gap no framework job can: a run that never happens writes NOTHING (no
# ledger row, no failed job, no alert), so the ledger cannot detect its own
# absence. This asks the question from outside.
#
#   check_freshness.sh <entity> <max_age_hours>
#
#   exit 0  fresh    — newest curated SUCCESS is within the window
#   exit 1  STALE    — older than the window, or NO success has ever landed
#   exit 2  broken   — the check itself could not run (Hive unreachable...)
#
# Exit 2 is deliberately distinct from 1: "the feed is stale" and "I could
# not tell" must not share an alert, or a Hive outage masquerades as a feed
# outage and the wrong team gets paged.
#
# Environment:
#   INGEST_HIVE_JDBC   beeline JDBC url (required), e.g. jdbc:hive2://host:10000/default
#   INGEST_AUDIT_DB    database holding ingest_run_audit (default membership_common_raw)
#   INGEST_AUDIT_TABLE run-ledger table name             (default ingest_run_audit)
#
# Choose <max_age_hours> from ledger history: it must exceed the LONGEST
# legitimate gap between successful loads (weekends, holidays), or this job
# becomes the thing that pages someone on a quiet Sunday.
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 2; }

ENTITY="${1:-}"; MAX_AGE_HOURS="${2:-}"
[[ -n "$ENTITY" && -n "$MAX_AGE_HOURS" ]] || die "usage: check_freshness.sh <entity> <max_age_hours>"
[[ "$MAX_AGE_HOURS" =~ ^[0-9]+$ ]] || die "max_age_hours must be a whole number, got '$MAX_AGE_HOURS'"
[[ -n "${INGEST_HIVE_JDBC:-}" ]] || die "INGEST_HIVE_JDBC is not set (beeline JDBC url)"

DB="${INGEST_AUDIT_DB:-membership_common_raw}"
TABLE="${INGEST_AUDIT_TABLE:-ingest_run_audit}"

# NULL handling matters: an entity with NO curated SUCCESS at all is the
# STALEST possible state (a feed that never worked, or a ledger pointed at
# the wrong database), not a reason to report healthy.
SQL="SELECT CASE
       WHEN MAX(event_ts) IS NULL THEN 'FRESHNESS:NEVER'
       WHEN MAX(event_ts) < current_timestamp() - INTERVAL ${MAX_AGE_HOURS} HOURS
         THEN CONCAT('FRESHNESS:STALE last_success=', CAST(MAX(event_ts) AS STRING))
       ELSE CONCAT('FRESHNESS:OK last_success=', CAST(MAX(event_ts) AS STRING))
     END
     FROM ${DB}.${TABLE}
     WHERE entity='${ENTITY}' AND stage='curated' AND status='SUCCESS'"

# beeline failing (connection refused, auth, missing table) lands in the
# die() path via set -e -> exit 2, never a false STALE.
OUT="$(beeline -u "$INGEST_HIVE_JDBC" --silent=true --showHeader=false \
        --outputformat=tsv2 -e "$SQL" 2>/dev/null)" \
  || die "beeline query failed against $INGEST_HIVE_JDBC (check connectivity/auth)"

# `|| true`: grep exits 1 on no match, and under set -e that would kill the
# script with exit 1 — unparseable output masquerading as STALE. It must
# reach the case arm below and exit 2 instead.
VERDICT="$(echo "$OUT" | grep -o 'FRESHNESS:[A-Z]*' | head -1 || true)"
DETAIL="$(echo "$OUT" | grep 'FRESHNESS:' | head -1 || true)"

case "$VERDICT" in
  FRESHNESS:OK)
    echo "OK: $ENTITY curated SUCCESS within ${MAX_AGE_HOURS}h ($DETAIL)"
    exit 0 ;;
  FRESHNESS:STALE)
    echo "STALE: $ENTITY newest curated SUCCESS is older than ${MAX_AGE_HOURS}h ($DETAIL)" >&2
    echo "       The framework cannot raise this itself: a run that never" >&2
    echo "       happened wrote nothing. Check the Control-M ingest job first." >&2
    exit 1 ;;
  FRESHNESS:NEVER)
    echo "STALE: $ENTITY has NO curated SUCCESS in ${DB}.${TABLE} at all" >&2
    echo "       Either the feed has never completed, or this check points at" >&2
    echo "       the wrong ledger — both deserve a person." >&2
    exit 1 ;;
  *)
    die "unrecognized query output: $OUT" ;;
esac
