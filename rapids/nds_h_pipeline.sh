#!/bin/bash
# Disk-safe NDS-H (TPC-H) generate -> transcode-to-parquet pipeline.
#
# Generates raw TPC-H data in chunked batches via nds_h_gen_data.py, transcodes
# each batch to Parquet via nds_h_transcode.py, merges the part-files into a
# consolidated per-table Parquet dir, then DELETES the raw batch before the next
# one. This keeps peak disk ~= final_parquet + one_batch_raw + one_batch_tmp,
# instead of needing the full ~SCALE GB of raw text on disk at once.
#
# Usage: nds_h_pipeline.sh <SCALE> <PARALLEL> <BATCH> <OUT_DIR>
#   SCALE     scale factor (GB), e.g. 500
#   PARALLEL  number of dbgen chunks the table is split into, e.g. 1000
#   BATCH     chunks processed (generated+transcoded) per iteration, e.g. 100
#   OUT_DIR   output root; final parquet lands in $OUT_DIR/parquet/<table>/
set -euo pipefail

SCALE="${1:?scale}"; PARALLEL="${2:?parallel}"; BATCH="${3:?batch}"; OUT_DIR="${4:?out_dir}"

NDSH_DIR="/workspace/baseline/spark-rapids-benchmarks/nds-h"
RAPIDS_DIR="/workspace/baseline/rapids"
DRIVER_MEM="${DRIVER_MEM:-96g}"

RAW="${OUT_DIR}/_raw"
TMP="${OUT_DIR}/_tmp_pq"
FINAL="${OUT_DIR}/parquet"
WORK="${OUT_DIR}/_work"
LOG="${OUT_DIR}/pipeline.log"

ALL_TABLES="customer lineitem nation orders part partsupp region supplier"
SCALED_TABLES="customer,lineitem,orders,part,partsupp,supplier"   # nation/region only at chunk 1

# Activate the self-contained spark-rapids env (Java 17 + pyspark 3.5.8).
source "${RAPIDS_DIR}/activate.sh" >/dev/null 2>&1

mkdir -p "${OUT_DIR}" "${FINAL}" "${WORK}"
: > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }

log "NDS-H pipeline: SCALE=${SCALE} PARALLEL=${PARALLEL} BATCH=${BATCH} -> ${FINAL}"

start_chunk=1
while [ "${start_chunk}" -le "${PARALLEL}" ]; do
  end_chunk=$(( start_chunk + BATCH - 1 ))
  [ "${end_chunk}" -gt "${PARALLEL}" ] && end_chunk="${PARALLEL}"
  log "=== batch chunks ${start_chunk}-${end_chunk}/${PARALLEL} ==="

  # 1) generate raw for this chunk range
  rm -rf "${RAW}"; mkdir -p "${RAW}"
  python3 "${NDSH_DIR}/nds_h_gen_data.py" local "${SCALE}" "${PARALLEL}" "${RAW}" \
      --range "${start_chunk},${end_chunk}" --overwrite_output >>"${LOG}" 2>&1
  log "    generated raw: $(du -sh "${RAW}" | cut -f1)"

  # 2) which tables this batch has (nation/region only generated at chunk 1)
  if [ "${start_chunk}" -eq 1 ]; then TABLES_ARG=""; else TABLES_ARG="--tables ${SCALED_TABLES}"; fi

  # 3) transcode this batch -> TMP (fresh catalog each time)
  rm -rf "${TMP}" "${WORK}/spark-warehouse" "${WORK}/metastore_db" "${WORK}/derby.log"
  ( cd "${WORK}" && env -u CONTAINER_ID spark-submit --master "local[*]" --driver-memory "${DRIVER_MEM}" \
        --conf spark.sql.warehouse.dir="${WORK}/spark-warehouse" \
        --conf spark.sql.shuffle.partitions=256 \
        "${NDSH_DIR}/nds_h_transcode.py" "${RAW}" "${TMP}" "${WORK}/report.txt" \
        --output_mode overwrite --log_level WARN ${TABLES_ARG} ) >>"${LOG}" 2>&1
  log "    transcoded batch -> tmp parquet: $(du -sh "${TMP}" | cut -f1)"

  # 4) merge part-files into the consolidated final parquet dir
  for t in ${ALL_TABLES}; do
    if [ -d "${TMP}/${t}" ]; then
      mkdir -p "${FINAL}/${t}"
      find "${TMP}/${t}" -name 'part-*.parquet' -exec mv -t "${FINAL}/${t}/" {} +
    fi
  done

  # 5) free disk: drop raw + tmp for this batch
  rm -rf "${RAW}" "${TMP}"
  log "    merged; disk now: $(df -h "${OUT_DIR}" | awk 'NR==2{print $3" used / "$4" free"}')"

  start_chunk=$(( end_chunk + 1 ))
done

log "DONE. Final parquet: $(du -sh "${FINAL}" | cut -f1)"
for t in ${ALL_TABLES}; do
  printf '  %-10s %s\n' "${t}" "$(du -sh "${FINAL}/${t}" 2>/dev/null | cut -f1)" | tee -a "${LOG}"
done
