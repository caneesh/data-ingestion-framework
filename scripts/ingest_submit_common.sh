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
# The config is handed to the app with --conf-path so the framework parses
# it as a file and resolves the include relative to the container working
# directory, which is where --files lands them. (-Dconfig.file is also
# absolutised by the app as of 2026-08-06, but --conf-path is preferred:
# it is explicit and reports a missing file as CFG_018.)
#
# ENVIRONMENT IN CLUSTER MODE: with --deploy-mode cluster the driver runs
# in a YARN container, so `export FOO=bar` in THIS shell does NOT reach it.
# Both the `env` secret provider (JDBC_002) and HOCON `${?FOO}` overrides
# read the DRIVER's environment. List the variable NAMES to forward:
#
#   INGEST_ENV_VARS="SMARTIQ_HOST,SMARTIQ_DB,SMARTIQ_DB_PASSWORD" \
#     scripts/run_ingest.sh /path/feed.conf my_feed INCR
#
# A listed-but-unset name is reported rather than silently skipped: a HOCON
# override that quietly falls back to its default points the run at the
# wrong database without raising any error at all.
set -euo pipefail

submit_ingest() {
  local CONF_FILE="$1"; local ENTITY="$2"; local JAR_FILE="$3"; shift 3
  local CONF_NAME; CONF_NAME="$(basename "$CONF_FILE")"
  local FILES="$CONF_FILE"
  if [[ -n "${INGEST_EXTRA_FILES:-}" ]]; then
    FILES="$FILES,$INGEST_EXTRA_FILES"
  fi

  # Forward the named variables into the driver container's environment.
  local ENV_CONFS=()
  if [[ -n "${INGEST_ENV_VARS:-}" ]]; then
    local NAMES=() NAME
    IFS=',' read -ra NAMES <<< "$INGEST_ENV_VARS"
    for NAME in "${NAMES[@]}"; do
      NAME="${NAME//[[:space:]]/}"
      [[ -z "$NAME" ]] && continue
      if [[ -z "${!NAME:-}" ]]; then
        echo "WARN: INGEST_ENV_VARS names '$NAME' but it is unset here — NOT forwarded." >&2
        echo "      The feed will fall back to its built-in default for this value." >&2
        continue
      fi
      case "$NAME" in
        *PASSWORD*|*SECRET*|*TOKEN*|*CREDENTIAL*|*PASSWD*)
          # The value becomes part of the spark-submit argv.
          echo "WARN: forwarding '$NAME' places its VALUE on the spark-submit command" >&2
          echo "      line — readable via 'ps' on this host and in the YARN launch" >&2
          echo "      context. Acceptable for a lower-environment test; for production" >&2
          echo "      use a secret provider (cyberark / azure_keyvault / conjur) or the" >&2
          echo "      'file' provider with a --files-shipped secret." >&2
          ;;
      esac
      ENV_CONFS+=(--conf "spark.yarn.appMasterEnv.$NAME=${!NAME}")
    done
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
    ${ENV_CONFS[@]+"${ENV_CONFS[@]}"} \
    --files "$FILES" \
    "$JAR_FILE" \
    --entity "$ENTITY" \
    --conf-path "./$CONF_NAME" \
    "$@"
}
