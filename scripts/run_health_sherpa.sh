#!/usr/bin/env bash
# COUPLED execution for the health_sherpa_member file feed (one Control-M
# job). Kept for backward compatibility — it now delegates to the shared
# wrapper so it picks up the same config handling as every other job.
#
# Split configs: if this feed's HOCON uses `include required("...")`, pass
# the included file(s) too:
#   INGEST_EXTRA_FILES=/path/health-sherpa-schema.conf \
#     scripts/run_health_sherpa.sh /path/application.conf
#
# Usage: run_health_sherpa.sh <application.conf> [jar-path] [extra flags...]
# Extra IngestMain flags (e.g. --run-id, --dry-run, --validate-only) may
# follow; anything starting with "--" is passed straight through. The jar
# can also come from INGEST_JAR instead of the positional slot.
set -euo pipefail
source "$(dirname "$0")/ingest_submit_common.sh"

CONF_FILE="${1:?Usage: $0 <application.conf> [jar-path]}"
JAR_FILE="${INGEST_JAR:-ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"
if [[ $# -ge 2 && "$2" != --* ]]; then JAR_FILE="$2"; shift 2
else shift $(( $# > 1 ? 1 : $# )); fi

submit_ingest "$CONF_FILE" "health_sherpa_member" "$JAR_FILE" --mode FULL "$@"
