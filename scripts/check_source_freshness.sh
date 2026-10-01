#!/usr/bin/env bash
#
# Source-partition freshness check — for a hive-source feed, the question
# check_freshness.sh cannot ask. That script watches the LEDGER: "did a run
# succeed recently?" A run can succeed every morning on an EMPTY window
# while the upstream landing has silently stopped — the ledger is green, the
# curated table is stale, and nobody knows. This asks the SOURCE directly:
# "has a new partition appeared within the window?"
#
#   check_source_freshness.sh <database> <table> <max_age_days>
#
#   exit 0  fresh    — the newest file_date partition is within the window
#   exit 1  STALE    — older than the window, or the table has NO partitions
#   exit 2  broken   — the check itself could not run (Hive unreachable,
#                      table missing, unparseable partition names ...)
#
# Metadata only: SHOW PARTITIONS never reads data, so this is safe against a
# 30 GB text table at any hour. It therefore also cannot see files that
# landed WITHOUT a partition being registered — that is the source owner's
# MSCK REPAIR obligation, and this check is what makes its absence visible.
#
# Environment:
#   INGEST_HIVE_JDBC   beeline JDBC url (required)
#   INGEST_FILE_DATE_KEY  partition key holding yyyy-MM-dd (default file_date)
#
# Choose <max_age_days> from the source's rhythm: a daily landing with a
# weekend gap needs 3, not 1.
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 2; }

DB="${1:-}"; TABLE="${2:-}"; MAX_AGE_DAYS="${3:-}"
[[ -n "$DB" && -n "$TABLE" && -n "$MAX_AGE_DAYS" ]] \
  || die "usage: check_source_freshness.sh <database> <table> <max_age_days>"
[[ "$MAX_AGE_DAYS" =~ ^[0-9]+$ ]] || die "max_age_days must be a whole number, got '$MAX_AGE_DAYS'"
[[ "$DB" =~ ^[A-Za-z0-9_]+$ && "$TABLE" =~ ^[A-Za-z0-9_]+$ ]] \
  || die "database and table must be plain identifiers, got '$DB'.'$TABLE'"
[[ -n "${INGEST_HIVE_JDBC:-}" ]] || die "INGEST_HIVE_JDBC is not set (beeline JDBC url)"
KEY="${INGEST_FILE_DATE_KEY:-file_date}"

# beeline failing (connection, auth, missing table) is exit 2, never STALE.
OUT="$(beeline -u "$INGEST_HIVE_JDBC" --silent=true --showHeader=false \
        --outputformat=tsv2 -e "SHOW PARTITIONS ${DB}.${TABLE}" 2>/dev/null)" \
  || die "SHOW PARTITIONS failed for ${DB}.${TABLE} against $INGEST_HIVE_JDBC (connectivity/auth/table)"

# Partition specs look like inc_ful_flag=I/file_date=2026-03-01/file_time=04.00.00.
# Pull every <KEY>=yyyy-MM-dd value and take the lexicographic max — the
# zero-padded format is what makes string order equal date order; a value
# that is not yyyy-MM-dd is a broken source and must not be compared.
DATES="$(printf '%s\n' "$OUT" | grep -o "${KEY}=[^/]*" | sed "s/^${KEY}=//" || true)"
if [[ -z "$DATES" ]]; then
  if [[ -z "$(printf '%s' "$OUT" | tr -d '[:space:]')" ]]; then
    echo "STALE: ${DB}.${TABLE} has NO partitions at all" >&2
    echo "       Either the landing has never run, or this check points at the" >&2
    echo "       wrong table — both deserve a person." >&2
    exit 1
  fi
  die "no '${KEY}=' component in the partition specs of ${DB}.${TABLE}; first spec: $(printf '%s\n' "$OUT" | head -1)"
fi
BAD="$(printf '%s\n' "$DATES" | grep -vE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' | head -1 || true)"
[[ -z "$BAD" ]] || die "partition value '${KEY}=${BAD}' is not yyyy-MM-dd; the source is not what the feed expects (HIVE_006 would fail the load too)"

NEWEST="$(printf '%s\n' "$DATES" | sort | tail -1)"
CUTOFF="$(date -d "-${MAX_AGE_DAYS} days" +%F)" \
  || die "this check needs GNU date (-d); install coreutils or adapt the cutoff"

if [[ "$NEWEST" < "$CUTOFF" ]]; then
  echo "STALE: ${DB}.${TABLE} newest ${KEY} partition is ${NEWEST}, older than ${MAX_AGE_DAYS} day(s) (cutoff ${CUTOFF})" >&2
  echo "       The ingest ledger can stay green through this: an empty window is a" >&2
  echo "       successful run. Check the upstream landing (Sqoop / MSCK REPAIR) first." >&2
  exit 1
fi
echo "OK: ${DB}.${TABLE} newest ${KEY} partition is ${NEWEST} (within ${MAX_AGE_DAYS} day(s), cutoff ${CUTOFF})"
exit 0
