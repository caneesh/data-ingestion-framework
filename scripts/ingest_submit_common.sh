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
#
# JDBC DRIVER: a jdbc feed needs its vendor driver on the driver AND every
# executor — the framework loads it reflectively in both places and fails
# fast with JDBC_001 if it is absent. Unless the cluster already ships one
# cluster-wide, point INGEST_JARS at it (comma-separated for several):
#
#   INGEST_JARS=/opt/jdbc/mssql-jdbc-12.4.2.jre11.jar \
#     scripts/run_ingest.sh /path/feed.conf my_feed INCR
set -euo pipefail

submit_ingest() {
  local CONF_FILE="$1"; local ENTITY="$2"; local JAR_FILE="$3"; shift 3
  local CONF_NAME; CONF_NAME="$(basename "$CONF_FILE")"
  local FILES="$CONF_FILE"
  if [[ -n "${INGEST_EXTRA_FILES:-}" ]]; then
    FILES="$FILES,$INGEST_EXTRA_FILES"
  fi

  # Vendor JDBC driver(s), distributed to the driver and every executor.
  local JAR_OPTS=()
  if [[ -n "${INGEST_JARS:-}" ]]; then
    JAR_OPTS=(--jars "$INGEST_JARS")
  fi

  # cluster (default, what Control-M runs) | client (driver = this shell).
  local DEPLOY_MODE="${INGEST_DEPLOY_MODE:-cluster}"
  if [[ "$DEPLOY_MODE" != "cluster" && "$DEPLOY_MODE" != "client" ]]; then
    echo "ERROR: INGEST_DEPLOY_MODE must be 'cluster' or 'client', got '$DEPLOY_MODE'." >&2
    return 2
  fi

  # Where the DRIVER finds the feed config. Cluster mode: --files localises
  # it into the container working directory, so the basename is correct.
  # Client mode: the driver is this shell and reads the ORIGINAL path, so
  # "./name" would only work when the cwd happens to hold it — use the
  # absolute path and let the HOCON include resolve beside it.
  local CONF_ARG="./$CONF_NAME"
  if [[ "$DEPLOY_MODE" == "client" ]]; then
    if [[ ! -f "$CONF_FILE" ]]; then
      echo "ERROR: client mode needs a readable config on this host: '$CONF_FILE'" >&2
      return 2
    fi
    CONF_ARG="$(cd "$(dirname "$CONF_FILE")" && pwd)/$CONF_NAME"
  fi

  # Forward the named variables into the driver container's environment.
  # CLIENT MODE ONLY INHERITS: the driver is this shell, so exported values
  # are already visible to it. Forwarding them there would put secrets on
  # the command line for no benefit — spark.yarn.appMasterEnv does not
  # reach a client-mode driver anyway.
  local ENV_CONFS=()
  if [[ -n "${INGEST_ENV_VARS:-}" && "$DEPLOY_MODE" == "client" ]]; then
    echo "INFO: client mode — the driver inherits this shell's environment;" >&2
    echo "      INGEST_ENV_VARS is not forwarded (and no secret is placed" >&2
    echo "      on the spark-submit command line)." >&2
  elif [[ -n "${INGEST_ENV_VARS:-}" ]]; then
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
    --deploy-mode "$DEPLOY_MODE" \
    --conf spark.sql.caseSensitive=false \
    --conf spark.speculation=false \
    --conf spark.sql.sources.partitionOverwriteMode=dynamic \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.sql.shuffle.partitions=200 \
    ${ENV_CONFS[@]+"${ENV_CONFS[@]}"} \
    ${JAR_OPTS[@]+"${JAR_OPTS[@]}"} \
    --files "$FILES" \
    "$JAR_FILE" \
    --entity "$ENTITY" \
    --conf-path "$CONF_ARG" \
    "$@"
}
