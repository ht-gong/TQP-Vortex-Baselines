#!/bin/bash
# Run NDS-H (TPC-H) queries 1-22 on the ramdisk dataset with RAPIDS, SAFELY:
#   * one spark-submit per query  -> scratch is reclaimed when each JVM exits,
#     so a heavy query can't poison the rest;
#   * a disk watchdog kills a query (and moves on) if free disk drops below a
#     threshold -> the box can never wedge on a full disk again.
#
#   run_tpch_safe.sh [QUERY_LIST]      e.g. "1 2 3"  (default: 1..22)
set -uo pipefail

QUERIES="${1:-$(seq 1 22)}"
RAPIDS_DIR="/workspace/baseline/rapids"
RAM_PQ="/dev/shm/tpch_sf500/parquet"
STREAM="/workspace/baseline/tpch_sf500/queries/stream_qualification.sql"
OUT_CSV="/workspace/baseline/tpch_sf500/query_times_gpu.csv"
SCRATCH="/workspace/baseline/_spark_scratch"
LOG="/workspace/baseline/tpch_sf500/safe_run.log"
DRIVER_MEM="${DRIVER_MEM:-96g}"      # JVM heap (host spill store is off-heap, separate)
MIN_FREE_GB="${MIN_FREE_GB:-30}"     # kill a query if free disk drops below this

source "${RAPIDS_DIR}/activate.sh" >/dev/null 2>&1
mkdir -p "${SCRATCH}"
rm -f "${OUT_CSV}"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }

free_gb(){ df -k --output=avail / | tail -1 | awk '{print int($1/1024/1024)}'; }

log "Safe per-query RAPIDS run. queries=[${QUERIES}] min_free=${MIN_FREE_GB}GB driver=${DRIVER_MEM}"
log "free disk at start: $(free_gb)GB ; ramdisk: $(du -sh ${RAM_PQ%/parquet} 2>/dev/null | cut -f1)"

for q in ${QUERIES}; do
  rm -rf "${SCRATCH:?}"/* 2>/dev/null
  qlog="/workspace/baseline/tpch_sf500/q${q}.log"
  log "=== query ${q} starting (free $(free_gb)GB) ==="
  env -u CONTAINER_ID spark-submit \
    --master "local[*]" --driver-memory "${DRIVER_MEM}" \
    --jars "${RAPIDS_JAR}" \
    --conf spark.local.dir="${SCRATCH}" \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.rapids.memory.pinnedPool.size=8G \
    --conf spark.rapids.memory.host.spillStorageSize=200G \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark358.RapidsShuffleManager \
    --conf spark.rapids.shuffle.mode=MULTITHREADED \
    --conf spark.sql.files.maxPartitionBytes=1g \
    --conf spark.sql.shuffle.partitions=1024 \
    --conf spark.sql.adaptive.enabled=true \
    "${RAPIDS_DIR}/run_tpch_queries.py" "${RAM_PQ}" "${STREAM}" "${OUT_CSV}" "${q}" append \
    >"${qlog}" 2>&1 &
  pid=$!

  # watchdog: kill the query if free disk gets dangerously low
  killed=0
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB during query ${q} -> killing to protect the box"
      pkill -9 -P "${pid}" 2>/dev/null; kill -9 "${pid}" 2>/dev/null; pkill -9 java 2>/dev/null
      killed=1; break
    fi
    sleep 3
  done
  wait "${pid}" 2>/dev/null; rc=$?

  if [ "${killed}" = 1 ]; then
    echo "query${q},KILLED_DISK,NA,0,exceeded_disk_scratch_on_single_GPU" >> "${OUT_CSV}"
    log "    query ${q}: KILLED (disk). result line recorded."
  else
    res=$(grep -E "^query${q}[, ]" "${OUT_CSV}" | tail -1)
    log "    query ${q}: done rc=${rc} -> ${res:-<no csv row>}"
  fi
  rm -rf "${SCRATCH:?}"/* 2>/dev/null
done

log "ALL DONE. free disk: $(free_gb)GB"
log "results:"; cat "${OUT_CSV}" | tee -a "${LOG}"
