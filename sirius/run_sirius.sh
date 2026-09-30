#!/bin/bash
# Run TPC-H queries 1-22 through Sirius (GPU, gpu_execution) on the ramdisk
# parquet, SAFELY -- one duckdb process per query so each gets a clean GPU
# context and any spill scratch is reclaimed on exit, plus a disk watchdog that
# kills a query (and moves on) if free disk drops below a threshold so heavy
# spill can never wedge the box. Mirrors polars/run_polars_gpu.sh and
# rapids/run_tpch_safe.sh.
#
# One GPU; Sirius spills GPU -> pinned host RAM -> disk per sirius.yaml.
# Per-query time excludes engine startup (a warm-up query absorbs GPU/JIT init);
# results are appended incrementally.
#
#   run_sirius.sh [QUERY_LIST]   e.g. "1 2 3"   (default: 1..22)
#
# Env: SIRIUS_PARQUET (dataset), TPCH_SF, PY, and the image's SIRIUS_DUCKDB /
# SIRIUS_ENVLIB (sirius/env.sh).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

QUERIES="${1:-$(seq 1 22)}"
DIR="${ROOT}/sirius"
RAM_PQ="${SIRIUS_PARQUET:?SIRIUS_PARQUET is not set}"
STREAM="${ROOT}/results/queries/stream_qualification.sql"
LOGDIR="${ROOT}/results"
TPCH_SF="${TPCH_SF:?TPCH_SF is not set}"
# Per-query rows go to a throwaway temp CSV and are folded into the single
# canonical results/all_results.csv at the end (merge_results.py); no per-run
# CSV is kept. TAG only names the gitignored per-query log files.
TAG="${SIRIUS_TAG:-sirius_sf${TPCH_SF}}"
OUT_CSV="${OUT_CSV:-/tmp/tpch_sirius_sf${TPCH_SF}.csv}"
DETAIL_CSV="/tmp/tpch_sirius_sf${TPCH_SF}_detail.csv"
LOG="${LOGDIR}/${TAG}_run.log"
MIN_FREE_GB="${MIN_FREE_GB:-25}"

# shellcheck disable=SC1091
source "${DIR}/env.sh"
SPILL="${SIRIUS_SPILL}"
export SIRIUS_ITERS="${SIRIUS_ITERS:-2}"
export SIRIUS_TIMEOUT="${SIRIUS_TIMEOUT:-2400}"
export SIRIUS_DETAIL_CSV="${DETAIL_CSV}"
export SIRIUS_LOG_DIR="${SIRIUS_LOG_DIR:-${LOGDIR}/${TAG}_logs}"
mkdir -p "${SPILL}" "${SIRIUS_LOG_DIR}"
rm -f "${OUT_CSV}" "${DETAIL_CSV}"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
free_gb(){ df -k --output=avail / | tail -1 | awk '{print int($1/1024/1024)}'; }

if [ ! -x "${SIRIUS_DUCKDB}" ]; then
  log "ERROR: no Sirius duckdb binary at ${SIRIUS_DUCKDB}"; exit 1
fi

log "Sirius per-query run. queries=[${QUERIES}] iters=${SIRIUS_ITERS} timeout=${SIRIUS_TIMEOUT}s min_free=${MIN_FREE_GB}GB"
log "duckdb=${SIRIUS_DUCKDB}"
log "config=${SIRIUS_CONFIG_FILE} ; io=${SIRIUS_IO} ; parquet=${RAM_PQ}"
log "free disk at start: $(free_gb)GB ; ramdisk: $(du -sh ${RAM_PQ%/parquet} 2>/dev/null | cut -f1)"

for q in ${QUERIES}; do
  rm -rf "${SPILL:?}"/* 2>/dev/null
  qlog="${LOGDIR}/${TAG}_q${q}.log"
  log "=== query ${q} starting (free $(free_gb)GB) ==="
  "${PY:?PY is not set}" "${DIR}/run_tpch_sirius.py" "${RAM_PQ}" "${STREAM}" "${OUT_CSV}" "${q}" append \
      >"${qlog}" 2>&1 &
  pid=$!

  killed=0
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB during query ${q} -> killing to protect the box"
      pkill -9 -P "${pid}" 2>/dev/null; kill -9 "${pid}" 2>/dev/null
      pkill -9 -f "run_tpch_sirius.py" 2>/dev/null; pkill -9 -f "duckdb -f /tmp/sirius_q" 2>/dev/null
      killed=1; break
    fi
    sleep 3
  done
  wait "${pid}" 2>/dev/null; rc=$?

  if [ "${killed}" = 1 ]; then
    echo "query${q},KILLED_DISK,NA,exceeded_disk_scratch" >> "${OUT_CSV}"
    log "    query ${q}: KILLED (disk)."
  else
    res=$(grep -E "^query${q}[, ]" "${OUT_CSV}" | tail -1)
    log "    query ${q}: done rc=${rc} -> ${res:-<no csv row>}"
  fi
  rm -rf "${SPILL:?}"/* 2>/dev/null
done

log "ALL DONE. free disk: $(free_gb)GB"
log "results:"; cat "${OUT_CSV}" | tee -a "${LOG}"

# Fold this run into the single canonical results table, then drop the temp CSVs.
if [ "${TPCH_MERGE:-1}" = 1 ]; then
  "${PY}" "${ROOT}/merge_results.py" sirius "${TPCH_SF}" "${OUT_CSV}" | tee -a "${LOG}" \
    && rm -f "${OUT_CSV}" "${DETAIL_CSV}"
fi
