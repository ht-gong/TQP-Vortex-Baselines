#!/bin/bash
# Run native-Polars TPC-H queries 1-22 on the ramdisk dataset on the GPU
# (cudf-polars / RAPIDS), one python process per query so each gets a clean GPU
# context. The streaming executor spills device->host via rapidsmpf (async
# pool). Each process first runs its query on the SF1 copy in WARM_PARQUET,
# untimed, then times it on TPCH_PARQUET (run.sh protocol).
#
#   run_polars_gpu.sh [QUERY_LIST]   e.g. "1 2 3"   (default: 1..22)
#
# Env: TPCH_PARQUET (dataset), TPCH_SF, WARM_PARQUET (SF1 warm copy), SCRATCH
# (spill goes to $SCRATCH/polars), POLARS_PY (the image's cudf-polars env).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

QUERIES="${1:-$(seq 1 22)}"
DIR="${ROOT}/polars"
RAM_PQ="${TPCH_PARQUET:?TPCH_PARQUET is not set}"
: "${WARM_PARQUET:?WARM_PARQUET is not set (the SF1 warm copy)}"
TPCH_SF="${TPCH_SF:?TPCH_SF is not set}"
# throwaway temp; folded into results/all_results.csv at the end (no per-run CSV kept)
OUT_CSV="${OUT_CSV:-/tmp/tpch_polars_gpu_sf${TPCH_SF}.csv}"
SPILL_DIR="${SCRATCH:?SCRATCH is not set}/polars"
LOG="${ROOT}/results/polars_gpu_run.log"
MIN_FREE_GB="${MIN_FREE_GB:-30}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-0}"  # kill a query after this many seconds (0 = never); row -> TIMEOUT

source "${DIR}/env.sh"
export POLARS_TEMP_DIR="${SPILL_DIR}"
mkdir -p "${SPILL_DIR}"
rm -f "${OUT_CSV}"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
free_gb(){ df -k --output=avail / | tail -1 | awk '{print int($1/1024/1024)}'; }

log "Polars GPU per-query run. queries=[${QUERIES}] mr=${GPU_MR} part=${GPU_PART_MB}MB min_free=${MIN_FREE_GB}GB"
log "free disk at start: $(free_gb)GB"

for q in ${QUERIES}; do
  rm -rf "${SPILL_DIR:?}"/* 2>/dev/null
  qlog="${ROOT}/results/polars_gpu_q${q}.log"
  log "=== query ${q} starting (free $(free_gb)GB) ==="
  "${POLARS_PY}" "${DIR}/run_tpch_polars.py" "${RAM_PQ}" "${OUT_CSV}" "${q}" append \
    >"${qlog}" 2>&1 &
  pid=$!
  killed=0; timedout=0; tstart=$(date +%s)
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB during query ${q} -> killing"
      kill -9 "${pid}" 2>/dev/null; pkill -9 -P "${pid}" 2>/dev/null; killed=1; break
    fi
    if [ "${QUERY_TIMEOUT}" -gt 0 ] && [ $(( $(date +%s) - tstart )) -ge "${QUERY_TIMEOUT}" ]; then
      log "    !! query ${q} exceeded ${QUERY_TIMEOUT}s -> killing it"
      pkill -9 -P "${pid}" 2>/dev/null; kill -9 "${pid}" 2>/dev/null; timedout=1; break
    fi
    sleep 3
  done
  wait "${pid}" 2>/dev/null; rc=$?
  if [ "${killed}" = 1 ]; then
    echo "query${q},KILLED_DISK,NA,exceeded_disk_scratch" >> "${OUT_CSV}"
    log "    query ${q}: KILLED (disk)."
  elif [ "${timedout}" = 1 ]; then
    echo "query${q},TIMEOUT,$(( $(date +%s) - tstart )),exceeded_query_timeout_${QUERY_TIMEOUT}s" >> "${OUT_CSV}"
    log "    query ${q}: TIMEOUT."
  else
    res=$(grep -E "^query${q}[, ]" "${OUT_CSV}" | tail -1)
    log "    query ${q}: done rc=${rc} -> ${res:-<no csv row>}"
  fi
  rm -rf "${SPILL_DIR:?}"/* 2>/dev/null
done

log "ALL DONE. free disk: $(free_gb)GB"
log "results:"; cat "${OUT_CSV}" | tee -a "${LOG}"

if [ "${TPCH_MERGE:-1}" = 1 ]; then
  "${POLARS_PY}" "${ROOT}/merge_results.py" polars_gpu "${TPCH_SF}" "${OUT_CSV}" | tee -a "${LOG}" \
    && rm -f "${OUT_CSV}"
fi
