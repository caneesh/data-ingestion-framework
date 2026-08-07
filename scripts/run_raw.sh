#!/usr/bin/env bash
# DECOUPLED raw job (Control-M job 1 of 2): extract + land RAW + advance
# the extraction watermark at raw success. Never touches curated.
# Usage: run_raw.sh <application.conf> <entity> [FULL|INCR] [jar] [extra flags...]
# Extra IngestMain flags (e.g. --run-id, --dry-run, --validate-only) may
# follow; anything starting with "--" is passed straight through. The jar
# can also come from INGEST_JAR instead of the positional slot.
# Feed config MUST declare: ingestion.execution = DECOUPLED and
# watermark { advance_after = RAW } (CFG_016 validates both).
set -euo pipefail
source "$(dirname "$0")/ingest_submit_common.sh"

CONF_FILE="${1:?Usage: $0 <application.conf> <entity> [FULL|INCR] [jar]}"
ENTITY="${2:?entity required}"
MODE="${3:-INCR}"
JAR_FILE="${INGEST_JAR:-ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"
# A 4th positional is the jar only when it is not a flag, so extra
# IngestMain flags (--run-id, --dry-run, ...) can follow either form.
if [[ $# -ge 4 && "$4" != --* ]]; then JAR_FILE="$4"; shift 4
else shift $(( $# > 3 ? 3 : $# )); fi

submit_ingest "$CONF_FILE" "$ENTITY" "$JAR_FILE" --mode "$MODE" --stage raw "$@"
