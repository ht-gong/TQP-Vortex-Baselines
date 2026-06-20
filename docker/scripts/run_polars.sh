#!/bin/bash
# Run TPC-H q1-22 on the GPU with cudf-polars (RAPIDS). One process per query;
# the streaming executor spills device->host so the dataset fits a single GPU.
# Per-query time excludes startup (warm-up query). Two passes:
#   1) async pool + rapidsmpf device->host spill (fast)
#   2) any query that OOMs is retried on managed/UVM memory (slow but completes)
# so the final table always has all 22.
#
#   run_polars.sh [QUERY_LIST]      e.g. "1 2 3"   (default 1..22)
set -uo pipefail

QUERIES="${1:-$(seq 1 22)}"
SCALE="${SCALE:-10}"
DATA_DIR="${DATA_DIR:-/data}"
SRC="${DATA_DIR}/sf${SCALE}/parquet"
RESULTS="${DATA_DIR}/sf${SCALE}/results"; mkdir -p "${RESULTS}"
OUT_CSV="${RESULTS}/query_times_polars_gpu.csv"
SCRATCH="${POLARS_SCRATCH:-/tmp/polars_scratch}"; mkdir -p "${SCRATCH}"
APP="/opt/baseline/app/run_polars.py"
MIN_FREE_GB="${MIN_FREE_GB:-30}"

source /opt/venv-polars/bin/activate
export POLARS_TEMP_DIR="${SCRATCH}"
export GPU_PART_MB="${GPU_PART_MB:-128}"
export RAPIDSMPF_SPILL_DEVICE_LIMIT="${RAPIDSMPF_SPILL_DEVICE_LIMIT:-$((22*1024*1024*1024))}"

# Stage parquet into /dev/shm (ramdisk) for fast reads -- ON by default. Needs the
# container started with --shm-size >= dataset; reuses an existing stage and falls
# back to reading from disk if it will not fit (set STAGE_RAMDISK=0 to disable).
if [ "${STAGE_RAMDISK:-1}" = "1" ]; then
  RAM="/dev/shm/tpch_sf${SCALE}/parquet"
  need_kb=$(du -sk "${SRC}" | cut -f1); free_kb=$(df -k --output=avail /dev/shm | tail -1)
  if [ -d "${RAM}" ] && [ "$(du -sk "${RAM}" 2>/dev/null | cut -f1)" = "${need_kb}" ]; then
    echo "ramdisk: reusing ${RAM}"; SRC="${RAM}"
  elif [ "${need_kb}" -lt "${free_kb}" ]; then
    echo "ramdisk: staging $((need_kb/1024/1024))GB -> ${RAM}"
    mkdir -p "${RAM}"; ls "${SRC}" | xargs -P 8 -I{} cp -r "${SRC}/{}" "${RAM}/"; SRC="${RAM}"
  else
    echo "ramdisk: dataset $((need_kb/1024/1024))GB > /dev/shm free $((free_kb/1024/1024))GB -> using disk (raise --shm-size to enable)"
  fi
fi

free_gb(){ df -k --output=avail "${SCRATCH}" | tail -1 | awk '{print int($1/1024/1024)}'; }
log(){ echo "[$(date +%H:%M:%S)] $*"; }

# Run one query in a fresh process with the disk watchdog. $1=query $2=mr_mode
run_one(){
  local q="$1" mr="$2" csv="$3"
  rm -rf "${SCRATCH:?}"/* 2>/dev/null
  GPU_MR="${mr}" python3 "${APP}" "${SRC}" "${csv}" "${q}" append gpu \
      >"${RESULTS}/polars_q${q}.log" 2>&1 &
  local pid=$!
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB -> killing query ${q}"
      kill -9 "${pid}" 2>/dev/null; pkill -9 -P "${pid}" 2>/dev/null; break
    fi
    sleep 3
  done
  wait "${pid}" 2>/dev/null
}

rm -f "${OUT_CSV}"
log "Polars GPU run  scale=${SCALE}  src=${SRC}  (pass 1: async spill)"
for q in ${QUERIES}; do
  log "=== query ${q} (async, free $(free_gb)GB) ==="
  run_one "${q}" async "${OUT_CSV}"
  log "    -> $(grep -E "^query${q}[, ]" "${OUT_CSV}" | tail -1)"
done

# Pass 2: retry any FAIL on managed memory, replace the row in OUT_CSV.
fails=$(awk -F, '/,FAIL,/{gsub(/query/,"",$1); print $1}' "${OUT_CSV}")
if [ -n "${fails}" ]; then
  log "pass 2: retrying on managed memory -> ${fails}"
  for q in ${fails}; do
    tmp="${RESULTS}/.retry_q${q}.csv"; rm -f "${tmp}"
    log "=== query ${q} (managed/UVM) ==="
    run_one "${q}" managed "${tmp}"
    newrow=$(grep -E "^query${q}," "${tmp}" | tail -1)
    if [ -n "${newrow}" ]; then
      grep -vE "^query${q}," "${OUT_CSV}" > "${OUT_CSV}.t" && mv "${OUT_CSV}.t" "${OUT_CSV}"
      echo "${newrow}" >> "${OUT_CSV}"
    fi
    rm -f "${tmp}"
    log "    -> ${newrow}"
  done
fi
log "DONE -> ${OUT_CSV}"; cat "${OUT_CSV}"
