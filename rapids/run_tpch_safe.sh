#!/bin/bash
# Run NDS-H (TPC-H) queries 1-22 on the ramdisk dataset with RAPIDS, SAFELY:
#   * one spark-submit per query  -> scratch is reclaimed when each JVM exits,
#     so a heavy query can't poison the rest;
#   * a disk watchdog kills a query (and moves on) if free disk drops below a
#     threshold -> the box can never wedge on a full disk again.
#
#   run_tpch_safe.sh [QUERY_LIST]      e.g. "1 2 3"  (default: 1..22)
#
# Env: TPCH_PARQUET (dataset), TPCH_SF, SCRATCH (Spark local dirs go to
# $SCRATCH/rapids), plus the image's PY / JAVA_HOME / SPARK_HOME / RAPIDS_JAR.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

QUERIES="${1:-$(seq 1 22)}"
RAPIDS_DIR="${ROOT}/rapids"
RAM_PQ="${TPCH_PARQUET:?TPCH_PARQUET is not set}"
STREAM="${ROOT}/results/queries/stream_qualification.sql"
TPCH_SF="${TPCH_SF:?TPCH_SF is not set}"
# throwaway temp; folded into results/all_results.csv at the end (no per-run CSV kept)
OUT_CSV="${OUT_CSV:-/tmp/tpch_rapids_sf${TPCH_SF}.csv}"
SPARK_DIR="${SCRATCH:?SCRATCH is not set}/rapids"
LOG="${ROOT}/results/safe_run.log"
DRIVER_MEM="${DRIVER_MEM:-96g}"      # JVM heap (host spill store is off-heap, separate)
MIN_FREE_GB="${MIN_FREE_GB:-30}"     # kill a query if free disk drops below this
QUERY_TIMEOUT="${QUERY_TIMEOUT:-0}"  # kill a query after this many seconds (0 = never); row -> TIMEOUT

source "${RAPIDS_DIR}/activate.sh" >/dev/null 2>&1
rapids_run_args "${SPARK_DIR}"
mkdir -p "${SPARK_DIR}"
rm -f "${OUT_CSV}"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }

free_gb(){ df -k --output=avail / | tail -1 | awk '{print int($1/1024/1024)}'; }

log "Safe per-query RAPIDS run. queries=[${QUERIES}] min_free=${MIN_FREE_GB}GB driver=${DRIVER_MEM}"
log "free disk at start: $(free_gb)GB ; ramdisk: $(du -sh ${RAM_PQ%/parquet} 2>/dev/null | cut -f1)"

for q in ${QUERIES}; do
  rm -rf "${SPARK_DIR:?}"/* 2>/dev/null
  qlog="${ROOT}/results/q${q}.log"
  log "=== query ${q} starting (free $(free_gb)GB) ==="
  env -u CONTAINER_ID spark-submit "${RAPIDS_RUN_ARGS[@]}" \
    "${RAPIDS_DIR}/run_tpch_queries.py" "${RAM_PQ}" "${STREAM}" "${OUT_CSV}" "${q}" append \
    >"${qlog}" 2>&1 &
  pid=$!

  # watchdog: kill the query if free disk gets dangerously low, or if it
  # exceeds QUERY_TIMEOUT (only this query's JVM/python are killed).
  killed=0; timedout=0; tstart=$(date +%s)
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB during query ${q} -> killing to protect the box"
      pkill -9 -P "${pid}" 2>/dev/null; kill -9 "${pid}" 2>/dev/null; pkill -9 java 2>/dev/null
      killed=1; break
    fi
    if [ "${QUERY_TIMEOUT}" -gt 0 ] && [ $(( $(date +%s) - tstart )) -ge "${QUERY_TIMEOUT}" ]; then
      log "    !! query ${q} exceeded ${QUERY_TIMEOUT}s -> killing it"
      pkill -9 -f "run_tpch_queries.py ${RAM_PQ} " 2>/dev/null
      pkill -9 -P "${pid}" 2>/dev/null; kill -9 "${pid}" 2>/dev/null
      timedout=1; break
    fi
    sleep 3
  done
  wait "${pid}" 2>/dev/null; rc=$?

  if [ "${killed}" = 1 ]; then
    echo "query${q},KILLED_DISK,NA,0,exceeded_disk_scratch_on_single_GPU" >> "${OUT_CSV}"
    log "    query ${q}: KILLED (disk). result line recorded."
  elif [ "${timedout}" = 1 ]; then
    echo "query${q},TIMEOUT,$(( $(date +%s) - tstart )),0,exceeded_query_timeout_${QUERY_TIMEOUT}s" >> "${OUT_CSV}"
    log "    query ${q}: TIMEOUT. result line recorded."
  else
    res=$(grep -E "^query${q}[, ]" "${OUT_CSV}" | tail -1)
    log "    query ${q}: done rc=${rc} -> ${res:-<no csv row>}"
  fi
  rm -rf "${SPARK_DIR:?}"/* 2>/dev/null
done

log "ALL DONE. free disk: $(free_gb)GB"
log "results:"; cat "${OUT_CSV}" | tee -a "${LOG}"

if [ "${TPCH_MERGE:-1}" = 1 ]; then
  "${PY}" "${ROOT}/merge_results.py" rapids "${TPCH_SF}" "${OUT_CSV}" | tee -a "${LOG}" \
    && rm -f "${OUT_CSV}"
fi
