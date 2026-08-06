#!/usr/bin/env bash
# Shared spark-submit wrapper for Control-M jobs.
#
# Exit codes: 0 = success (including a clean empty/pending no-op);
# non-zero = failure — the driver's PIPE_/CFG_/CUR_ errors propagate
# through spark-submit, and failed curated batches stay PENDING, so a
# Control-M rerun of the same job is always safe.
#
# SPLIT CONFIGS: a feed that uses `include required("<name>-schema.conf")`
# needs BOTH files in the container. Set INGEST_EXTRA_FILES to a
# comma-separated list of the extra files and they are shipped alongside
# the feed config, e.g.
#
#   INGEST_EXTRA_FILES=/path/smartiq-pdp-schema.conf \
#     scripts/run_ingest.sh /path/feed-smartiq-pdp.conf smartiq_pdp INCR
#
# The config is handed to the app with --conf-path (not -Dconfig.file) so
# the framework parses it as a file and resolves the include relative to
# the container working directory, which is where --files lands them.
set -euo pipefail

submit_ingest() {
  local CONF_FILE="$1"; local ENTITY="$2"; local JAR_FILE="$3"; shift 3
  local CONF_NAME; CONF_NAME="$(basename "$CONF_FILE")"
  local FILES="$CONF_FILE"
  if [[ -n "${INGEST_EXTRA_FILES:-}" ]]; then
    FILES="$FILES,$INGEST_EXTRA_FILES"
  fi
  spark-submit \
    --class com.hcsc.generic.ingest.app.IngestMain \
    --name "ingest-${ENTITY}" \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.sql.caseSensitive=false \
    --conf spark.speculation=false \
    --conf spark.sql.sources.partitionOverwriteMode=dynamic \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.sql.shuffle.partitions=200 \
    --files "$FILES" \
    "$JAR_FILE" \
    --entity "$ENTITY" \
    --conf-path "./$CONF_NAME" \
    "$@"
}
