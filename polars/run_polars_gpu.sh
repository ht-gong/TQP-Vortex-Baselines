#!/bin/bash
# Run native-Polars TPC-H queries 1-22 on the ramdisk SF500 dataset on the GPU
# (cudf-polars / RAPIDS), one python process per query so each gets a clean GPU
# context. Single RTX 5090; streaming executor spills device->host via rapidsmpf
# (async pool) so SF500 fits 32 GB. Per-query time excludes startup (warm-up).
#
#   run_polars_gpu.sh [QUERY_LIST]   e.g. "1 2 3"   (default: 1..22)
set -uo pipefail

QUERIES="${1:-$(seq 1 22)}"
DIR="/workspace/baseline/polars"
RAM_PQ="${TPCH_PARQUET:-/dev/shm/tpch_sf500/parquet}"
TPCH_SF="${TPCH_SF:-500}"
# throwaway temp; folded into results/all_results.csv at the end (no per-run CSV kept)
OUT_CSV="${OUT_CSV:-/tmp/tpch_polars_gpu_sf${TPCH_SF}.csv}"
SCRATCH="/workspace/baseline/_polars_scratch_gpu"
LOG="/workspace/baseline/results/polars_gpu_run.log"
MIN_FREE_GB="${MIN_FREE_GB:-30}"

source "${DIR}/.venv-gpu/bin/activate"
export POLARS_TEMP_DIR="${SCRATCH}"
export GPU_MR="${GPU_MR:-async}"
export GPU_PART_MB="${GPU_PART_MB:-128}"
export RAPIDSMPF_SPILL_DEVICE_LIMIT="${RAPIDSMPF_SPILL_DEVICE_LIMIT:-$((22*1024*1024*1024))}"
mkdir -p "${SCRATCH}"
rm -f "${OUT_CSV}"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
free_gb(){ df -k --output=avail / | tail -1 | awk '{print int($1/1024/1024)}'; }

log "Polars GPU per-query run. queries=[${QUERIES}] mr=${GPU_MR} part=${GPU_PART_MB}MB min_free=${MIN_FREE_GB}GB"
log "free disk at start: $(free_gb)GB"

for q in ${QUERIES}; do
  rm -rf "${SCRATCH:?}"/* 2>/dev/null
  qlog="/workspace/baseline/results/polars_gpu_q${q}.log"
  log "=== query ${q} starting (free $(free_gb)GB) ==="
  python "${DIR}/run_tpch_polars.py" "${RAM_PQ}" "${OUT_CSV}" "${q}" append gpu \
    >"${qlog}" 2>&1 &
  pid=$!
  killed=0
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB during query ${q} -> killing"
      kill -9 "${pid}" 2>/dev/null; pkill -9 -P "${pid}" 2>/dev/null; killed=1; break
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
  python3 /workspace/baseline/merge_results.py polars_gpu "${TPCH_SF}" "${OUT_CSV}" | tee -a "${LOG}" \
    && rm -f "${OUT_CSV}"
fi
