#!/bin/bash
# Stage the SF500 parquet dataset into the ramdisk (/dev/shm), then run NDS-H
# (TPC-H) queries 1-22 on it with the RAPIDS GPU accelerator.
#
#   run_tpch_on_ramdisk.sh [SRC_PARQUET] [STREAM_SQL]
#
# Defaults point at the SF500 dataset and the generated qualification stream.
set -euo pipefail

SRC="${1:-/workspace/baseline/tpch_sf500/parquet}"
STREAM="${2:-/workspace/baseline/tpch_sf500/queries/stream_qualification.sql}"
RAPIDS_DIR="/workspace/baseline/rapids"
RAM_BASE="/dev/shm/tpch_sf500"
RAM_PQ="${RAM_BASE}/parquet"
OUT_CSV="/workspace/baseline/tpch_sf500/query_times_gpu.csv"
DRIVER_MEM="${DRIVER_MEM:-160g}"

source "${RAPIDS_DIR}/activate.sh" >/dev/null 2>&1

echo "=== 1) Stage parquet -> ramdisk (${RAM_PQ}) ==="
need_kb=$(du -sk "${SRC}" | cut -f1)
free_kb=$(df -k --output=avail /dev/shm | tail -1)
echo "dataset: $((need_kb/1024/1024)) GB ; /dev/shm free: $((free_kb/1024/1024)) GB"
if [ "${need_kb}" -gt "${free_kb}" ]; then
  echo "ERROR: dataset does not fit in /dev/shm" >&2; exit 1
fi

mkdir -p "${RAM_PQ}"
# Copy each table dir in parallel; skip a table already fully staged.
ls "${SRC}" | xargs -P 8 -I{} bash -c '
  src="'"${SRC}"'/{}"; dst="'"${RAM_PQ}"'/{}"
  ssz=$(du -sk "$src" | cut -f1)
  if [ -d "$dst" ] && [ "$(du -sk "$dst" | cut -f1)" = "$ssz" ]; then
    echo "  {}: already staged"; else
    rm -rf "$dst"; cp -r "$src" "$dst"; echo "  {}: staged ($((ssz/1024)) MB)"; fi'
echo "staged total: $(du -sh "${RAM_PQ}" | cut -f1) ; /dev/shm now: $(df -h /dev/shm | awk 'NR==2{print $3" used / "$4" free"}')"

echo; echo "=== 2) Run queries 1-22 with RAPIDS (reading from ramdisk) ==="
echo "NOTE: local[*] uses a single GPU (GPU 0). Multi-GPU needs a standalone cluster w/ 2 executors."
env -u CONTAINER_ID spark-submit \
  --master "local[*]" --driver-memory "${DRIVER_MEM}" \
  --jars "${RAPIDS_JAR}" \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.concurrentGpuTasks=2 \
  --conf spark.rapids.memory.pinnedPool.size=4G \
  --conf spark.rapids.sql.explain=NOT_ON_GPU \
  --conf spark.sql.files.maxPartitionBytes=1g \
  --conf spark.sql.shuffle.partitions=512 \
  --conf spark.sql.adaptive.enabled=true \
  "${RAPIDS_DIR}/run_tpch_queries.py" "${RAM_PQ}" "${STREAM}" "${OUT_CSV}"

echo; echo "=== results ==="; cat "${OUT_CSV}"
