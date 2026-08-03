#!/usr/bin/env bash
# COUPLED execution (one Control-M job): raw then curated in one process.
# Usage: run_ingest.sh <application.conf> <entity> [FULL|INCR] [jar]
# Feed config: ingestion.execution = COUPLED (or absent — the default).
set -euo pipefail
source "$(dirname "$0")/ingest_submit_common.sh"

CONF_FILE="${1:?Usage: $0 <application.conf> <entity> [FULL|INCR] [jar]}"
ENTITY="${2:?entity required}"
MODE="${3:-INCR}"
JAR_FILE="${4:-ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"

submit_ingest "$CONF_FILE" "$ENTITY" "$JAR_FILE" --mode "$MODE"
