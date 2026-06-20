#!/bin/bash
# Run NDS-H (TPC-H) queries 1-22 on the ramdisk dataset with RAPIDS, using a
# SINGLE warm Spark session (1 local worker, 1 GPU, large host-memory spill):
#   * JVM + GPU start once -> excluded from per-query timing (plus a warm-up query);
#   * GPU partitions spill to a large HOST-MEMORY store first, disk only as last resort;
#   * a disk watchdog aborts the run (instead of wedging) if free disk ever gets low.
#
#   run_tpch_warm.sh [QUERY_SUBSET]     e.g. "9"  (default: all 22)
set -uo pipefail

SUBSET="${1:-}"
RAPIDS_DIR="/workspace/baseline/rapids"
RAM_PQ="/dev/shm/tpch_sf500/parquet"
STREAM="/workspace/baseline/tpch_sf500/queries/stream_qualification.sql"
OUT_CSV="/workspace/baseline/tpch_sf500/query_times_gpu.csv"
SCRATCH="/workspace/baseline/_spark_scratch"
LOG="/workspace/baseline/tpch_sf500/warm_run.log"

# --- tunables (iterate here) ---
DRIVER_MEM="${DRIVER_MEM:-96g}"            # JVM heap (on-heap)
HOST_SPILL="${HOST_SPILL:-200g}"           # off-heap host spill store (GPU spills here first)
PINNED="${PINNED:-8g}"
GPU_TASKS="${GPU_TASKS:-2}"
SHUFFLE_PARTS="${SHUFFLE_PARTS:-1024}"
MAXPART="${MAXPART:-1g}"
MIN_FREE_GB="${MIN_FREE_GB:-25}"

source "${RAPIDS_DIR}/activate.sh" >/dev/null 2>&1
mkdir -p "${SCRATCH}"; rm -rf "${SCRATCH:?}"/* 2>/dev/null
rm -f "${OUT_CSV}"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
free_gb(){ df -k --output=avail / | tail -1 | awk '{print int($1/1024/1024)}'; }

log "Warm single-session RAPIDS run | driver=${DRIVER_MEM} host_spill=${HOST_SPILL} pinned=${PINNED}"
log "gpu_tasks=${GPU_TASKS} shuffle_parts=${SHUFFLE_PARTS} maxpart=${MAXPART} min_free=${MIN_FREE_GB}GB"
log "free disk: $(free_gb)GB | ramdisk: $(du -sh ${RAM_PQ} 2>/dev/null | cut -f1)"

env -u CONTAINER_ID spark-submit \
  --master "local[*]" --driver-memory "${DRIVER_MEM}" \
  --jars "${RAPIDS_JAR}" \
  --conf spark.local.dir="${SCRATCH}" \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.concurrentGpuTasks="${GPU_TASKS}" \
  --conf spark.rapids.memory.pinnedPool.size="${PINNED}" \
  --conf spark.rapids.memory.host.spillStorageSize="${HOST_SPILL}" \
  --conf spark.rapids.memory.host.offHeapLimit.enabled=true \
  --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark358.RapidsShuffleManager \
  --conf spark.rapids.shuffle.mode=MULTITHREADED \
  --conf spark.sql.files.maxPartitionBytes="${MAXPART}" \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTS}" \
  --conf spark.sql.adaptive.enabled=true \
  "${RAPIDS_DIR}/run_tpch_queries.py" "${RAM_PQ}" "${STREAM}" "${OUT_CSV}" "${SUBSET}" \
  >>"${LOG}" 2>&1 &
pid=$!

# watchdog: protect the box. If free disk drops below threshold, abort the run.
killed=0
while kill -0 "${pid}" 2>/dev/null; do
  f=$(free_gb)
  if [ "${f}" -lt "${MIN_FREE_GB}" ]; then
    log "!! free disk ${f}GB < ${MIN_FREE_GB}GB -> aborting run to protect the box"
    kill -9 "${pid}" 2>/dev/null; pkill -9 java 2>/dev/null; killed=1; break
  fi
  sleep 4
done
wait "${pid}" 2>/dev/null; rc=$?
rm -rf "${SCRATCH:?}"/* 2>/dev/null

if [ "${killed}" = 1 ]; then
  log "RUN ABORTED by watchdog (disk). Lower per-query memory or skip the heaviest query."
else
  log "spark-submit exited rc=${rc}. free disk: $(free_gb)GB"
fi
log "results CSV:"; cat "${OUT_CSV}" 2>/dev/null | tee -a "${LOG}"
