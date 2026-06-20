#!/bin/bash
# Run TPC-H q1-22 on the GPU with Spark + the RAPIDS Accelerator.
# One spark-submit per query (scratch reclaimed on JVM exit), GPU partitions
# spill to a large HOST-memory store first (so SF fits a single GPU), and a disk
# watchdog kills a query if free disk drops too low. Per-query time excludes
# JVM/GPU startup (a warm-up query absorbs first-touch kernel JIT).
#
#   run_spark_rapids.sh [QUERY_LIST]      e.g. "1 2 3"   (default 1..22)
set -uo pipefail

QUERIES="${1:-$(seq 1 22)}"
SCALE="${SCALE:-10}"
DATA_DIR="${DATA_DIR:-/data}"
SRC="${DATA_DIR}/sf${SCALE}/parquet"
STREAM="${DATA_DIR}/sf${SCALE}/queries/stream.sql"
RESULTS="${DATA_DIR}/sf${SCALE}/results"; mkdir -p "${RESULTS}"
OUT_CSV="${RESULTS}/query_times_spark_gpu.csv"
SCRATCH="${SPARK_SCRATCH:-/tmp/spark_scratch}"; mkdir -p "${SCRATCH}"
APP="/opt/baseline/app/run_spark_rapids.py"

# Tunables (env-overridable).
DRIVER_MEM="${DRIVER_MEM:-96g}"
HOST_SPILL="${HOST_SPILL:-200G}"
PINNED="${PINNED:-8G}"
GPU_TASKS="${GPU_TASKS:-2}"
SHUFFLE_PARTS="${SHUFFLE_PARTS:-1024}"
MAXPART="${MAXPART:-1g}"
MIN_FREE_GB="${MIN_FREE_GB:-30}"
RAPIDS_JAR="${RAPIDS_JAR:-/opt/rapids/rapids-4-spark.jar}"

source /opt/venv-spark/bin/activate

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

# Shim shuffle-manager class name to the running Spark version (3.5.8 -> spark358).
SHIM="spark$(echo "${SPARK_VERSION:-3.5.8}" | tr -d '.')"
free_gb(){ df -k --output=avail "${SCRATCH}" | tail -1 | awk '{print int($1/1024/1024)}'; }
log(){ echo "[$(date +%H:%M:%S)] $*"; }
rm -f "${OUT_CSV}"
log "Spark-RAPIDS GPU run  scale=${SCALE}  src=${SRC}  jar=${RAPIDS_JAR}"

for q in ${QUERIES}; do
  rm -rf "${SCRATCH:?}"/* 2>/dev/null
  log "=== query ${q} starting (free $(free_gb)GB) ==="
  env -u CONTAINER_ID spark-submit \
    --master "local[*]" --driver-memory "${DRIVER_MEM}" \
    --jars "${RAPIDS_JAR}" \
    --conf spark.local.dir="${SCRATCH}" \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.concurrentGpuTasks="${GPU_TASKS}" \
    --conf spark.rapids.memory.pinnedPool.size="${PINNED}" \
    --conf spark.rapids.memory.host.spillStorageSize="${HOST_SPILL}" \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.${SHIM}.RapidsShuffleManager \
    --conf spark.rapids.shuffle.mode=MULTITHREADED \
    --conf spark.sql.files.maxPartitionBytes="${MAXPART}" \
    --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTS}" \
    --conf spark.sql.adaptive.enabled=true \
    "${APP}" "${SRC}" "${STREAM}" "${OUT_CSV}" "${q}" append \
    >"${RESULTS}/spark_q${q}.log" 2>&1 &
  pid=$!
  killed=0
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB -> killing query ${q}"
      kill -9 "${pid}" 2>/dev/null; pkill -9 -P "${pid}" 2>/dev/null; pkill -9 java 2>/dev/null
      killed=1; break
    fi
    sleep 3
  done
  wait "${pid}" 2>/dev/null; rc=$?
  if [ "${killed}" = 1 ]; then
    echo "query${q},KILLED_DISK,NA,0,exceeded_disk_scratch" >> "${OUT_CSV}"
  else
    log "    query ${q}: $(grep -E "^query${q}[, ]" "${OUT_CSV}" | tail -1)"
  fi
  rm -rf "${SCRATCH:?}"/* 2>/dev/null
done
log "DONE -> ${OUT_CSV}"; cat "${OUT_CSV}"
