#!/usr/bin/env bash
set -euo pipefail

CONF_FILE="${1:?Usage: $0 <application.conf> [jar-path]}"
JAR_FILE="${2:-ingestion-app-1.0.0-SNAPSHOT-jar-with-dependencies.jar}"

spark-submit \
  --class com.hcsc.generic.ingest.app.IngestMain \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.caseSensitive=false \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.sql.shuffle.partitions=200 \
  --files "$CONF_FILE" \
  --driver-java-options "-Dconfig.file=$(basename "$CONF_FILE")" \
  --conf "spark.executor.extraJavaOptions=-Dconfig.file=$(basename "$CONF_FILE")" \
  "$JAR_FILE" \
  --entity health_sherpa_member \
  --mode FULL
