#!/usr/bin/env bash
# DECOUPLED curated job (Control-M job 2 of 2): drain all raw-SUCCESS
# batches not yet curated, per batch, checkpointed in the ledger. Never
# reads the source, never touches the watermark. An empty pending set
# exits 0 (clean no-op). Failed batches stay pending: rerunning this job
# after a failure resumes exactly where it stopped.
# Usage: run_curated_pending.sh <application.conf> <entity> [jar] [extra flags...]
# Extra IngestMain flags (e.g. --run-id, --dry-run, --validate-only) may
# follow; anything starting with "--" is passed straight through. The jar
# can also come from INGEST_JAR instead of the positional slot.
set -euo pipefail
source "$(dirname "$0")/ingest_submit_common.sh"

CONF_FILE="${1:?Usage: $0 <application.conf> <entity> [jar]}"
ENTITY="${2:?entity required}"
JAR_FILE="${INGEST_JAR:-ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"
if [[ $# -ge 3 && "$3" != --* ]]; then JAR_FILE="$3"; shift 3
else shift $(( $# > 2 ? 2 : $# )); fi

submit_ingest "$CONF_FILE" "$ENTITY" "$JAR_FILE" --stage curated --pending "$@"
