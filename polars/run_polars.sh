#!/bin/bash
# Run native-Polars TPC-H queries 1-22 on the ramdisk SF500 dataset, SAFELY:
#   * one python process per query  -> any spill scratch is reclaimed on exit;
#   * a disk watchdog kills a query (and moves on) if free disk drops below a
#     threshold, so heavy spill (q9/q18/q21) can never wedge the box.
# Polars CPU streaming engine. Per-query time excludes startup (negligible for
# Polars + a warm-up query); results are appended incrementally.
#
#   run_polars.sh [QUERY_LIST]   e.g. "1 2 3"   (default: 1..22)
set -uo pipefail

QUERIES="${1:-$(seq 1 22)}"
DIR="/workspace/baseline/polars"
RAM_PQ="${TPCH_PARQUET:-/dev/shm/tpch_sf500/parquet}"
TPCH_SF="${TPCH_SF:-500}"
# throwaway temp; folded into results/all_results.csv at the end (no per-run CSV kept)
OUT_CSV="${OUT_CSV:-/tmp/tpch_polars_cpu_sf${TPCH_SF}.csv}"
SCRATCH="/workspace/baseline/_polars_scratch"
LOG="/workspace/baseline/results/polars_run.log"
ENGINE="${ENGINE:-streaming}"
MIN_FREE_GB="${MIN_FREE_GB:-30}"

source "${DIR}/.venv/bin/activate"
export POLARS_TEMP_DIR="${SCRATCH}"
mkdir -p "${SCRATCH}"
rm -f "${OUT_CSV}"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
free_gb(){ df -k --output=avail / | tail -1 | awk '{print int($1/1024/1024)}'; }

log "Polars per-query run. queries=[${QUERIES}] engine=${ENGINE} min_free=${MIN_FREE_GB}GB"
log "free disk at start: $(free_gb)GB ; ramdisk: $(du -sh ${RAM_PQ%/parquet} 2>/dev/null | cut -f1)"

for q in ${QUERIES}; do
  rm -rf "${SCRATCH:?}"/* 2>/dev/null
  qlog="/workspace/baseline/results/polars_q${q}.log"
  log "=== query ${q} starting (free $(free_gb)GB) ==="
  python "${DIR}/run_tpch_polars.py" "${RAM_PQ}" "${OUT_CSV}" "${q}" append "${ENGINE}" \
    >"${qlog}" 2>&1 &
  pid=$!

  killed=0
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB during query ${q} -> killing to protect the box"
      kill -9 "${pid}" 2>/dev/null; pkill -9 -P "${pid}" 2>/dev/null
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
  rm -rf "${SCRATCH:?}"/* 2>/dev/null
done

log "ALL DONE. free disk: $(free_gb)GB"
log "results:"; cat "${OUT_CSV}" | tee -a "${LOG}"

if [ "${TPCH_MERGE:-1}" = 1 ]; then
  python3 /workspace/baseline/merge_results.py polars_cpu "${TPCH_SF}" "${OUT_CSV}" | tee -a "${LOG}" \
    && rm -f "${OUT_CSV}"
fi
