#!/usr/bin/env bash
# Shared spark-submit wrapper for Control-M jobs.
# Exit codes: 0 = success (including a clean empty/pending no-op);
# non-zero = failure — the driver's PIPE_/CFG_/CUR_ errors propagate
# through spark-submit, and failed curated batches stay PENDING, so a
# Control-M rerun of the same job is always safe.
set -euo pipefail

submit_ingest() {
  local CONF_FILE="$1"; local ENTITY="$2"; local JAR_FILE="$3"; shift 3
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
    --files "$CONF_FILE" \
    --driver-java-options "-Dconfig.file=$(basename "$CONF_FILE")" \
    --conf "spark.executor.extraJavaOptions=-Dconfig.file=$(basename "$CONF_FILE")" \
    "$JAR_FILE" \
    --entity "$ENTITY" \
    "$@"
}
