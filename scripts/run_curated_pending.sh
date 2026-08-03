#!/usr/bin/env bash
# DECOUPLED curated job (Control-M job 2 of 2): drain all raw-SUCCESS
# batches not yet curated, per batch, checkpointed in the ledger. Never
# reads the source, never touches the watermark. An empty pending set
# exits 0 (clean no-op). Failed batches stay pending: rerunning this job
# after a failure resumes exactly where it stopped.
# Usage: run_curated_pending.sh <application.conf> <entity> [jar]
set -euo pipefail
source "$(dirname "$0")/ingest_submit_common.sh"

CONF_FILE="${1:?Usage: $0 <application.conf> <entity> [jar]}"
ENTITY="${2:?entity required}"
JAR_FILE="${3:-ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"

submit_ingest "$CONF_FILE" "$ENTITY" "$JAR_FILE" --stage curated --pending
