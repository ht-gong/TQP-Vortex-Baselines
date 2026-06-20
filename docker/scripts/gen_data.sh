#!/bin/bash
# Generate SF<SCALE> TPC-H parquet + the 22-query stream into $DATA_DIR, using
# the NDS-H (NVIDIA spark-rapids-benchmarks) generator built into the image.
# Disk-safe: generates raw .tbl in chunked batches, transcodes each to parquet,
# then deletes the raw batch before the next -- so peak disk stays bounded.
#
#   gen_data.sh [SCALE] [PARALLEL] [BATCH]
# Defaults: SCALE=$SCALE, PARALLEL=max(1,SCALE/0.5), BATCH=auto.
set -euo pipefail

SCALE="${1:-${SCALE:-10}}"
# chunks: ~0.5 GB raw each; BATCH chunks generated per iteration.
PARALLEL="${2:-$(awk -v s="${SCALE}" 'BEGIN{n=int(s/0.5); print (n<1?1:n)}')}"
BATCH="${3:-$(awk -v p="${PARALLEL}" 'BEGIN{b=int(p/5); print (b<1?1:b)}')}"

DATA_DIR="${DATA_DIR:-/data}"
NDSH_DIR="${NDSH_DIR:-/opt/spark-rapids-benchmarks/nds-h}"
OUT="${DATA_DIR}/sf${SCALE}"
FINAL="${OUT}/parquet"
QUERIES="${OUT}/queries"
DRIVER_MEM="${DRIVER_MEM:-$(awk -v s="${SCALE}" 'BEGIN{m=int(s/5)+8; print (m>96?96:m)"g"}')}"
RAW="${OUT}/_raw"; TMP="${OUT}/_tmp"; WORK="${OUT}/_work"
ALL_TABLES="customer lineitem nation orders part partsupp region supplier"
SCALED="customer,lineitem,orders,part,partsupp,supplier"

source /opt/venv-spark/bin/activate
mkdir -p "${FINAL}" "${QUERIES}" "${WORK}"
log(){ echo "[$(date +%H:%M:%S)] $*"; }
submit(){ env -u CONTAINER_ID spark-submit --master "local[*]" --driver-memory "${DRIVER_MEM}" "$@"; }

log "NDS-H gen: SCALE=${SCALE} PARALLEL=${PARALLEL} BATCH=${BATCH} -> ${FINAL}"

# ---- 1) data: batched generate -> transcode -> merge -> drop raw ------------
start=1
while [ "${start}" -le "${PARALLEL}" ]; do
  end=$(( start + BATCH - 1 )); [ "${end}" -gt "${PARALLEL}" ] && end="${PARALLEL}"
  log "=== chunks ${start}-${end}/${PARALLEL} ==="
  rm -rf "${RAW}"; mkdir -p "${RAW}"
  python3 "${NDSH_DIR}/nds_h_gen_data.py" local "${SCALE}" "${PARALLEL}" "${RAW}" \
      --range "${start},${end}" --overwrite_output
  [ "${start}" -eq 1 ] && TABLES_ARG="" || TABLES_ARG="--tables ${SCALED}"
  rm -rf "${TMP}" "${WORK}/spark-warehouse" "${WORK}/metastore_db" "${WORK}/derby.log"
  ( cd "${WORK}" && submit \
      --conf spark.sql.warehouse.dir="${WORK}/spark-warehouse" \
      --conf spark.sql.shuffle.partitions=256 \
      "${NDSH_DIR}/nds_h_transcode.py" "${RAW}" "${TMP}" "${WORK}/report.txt" \
      --output_mode overwrite --log_level WARN ${TABLES_ARG} )
  for t in ${ALL_TABLES}; do
    if [ -d "${TMP}/${t}" ]; then
      mkdir -p "${FINAL}/${t}"
      find "${TMP}/${t}" -name 'part-*.parquet' -exec mv -t "${FINAL}/${t}/" {} +
    fi
  done
  rm -rf "${RAW}" "${TMP}"
  log "    merged; parquet so far: $(du -sh "${FINAL}" 2>/dev/null | cut -f1)"
  start=$(( end + 1 ))
done

# ---- 2) queries: qgen the Spark-compatible 22-query stream ------------------
log "generating query stream -> ${QUERIES}/stream.sql"
python3 "${NDSH_DIR}/nds_h_gen_query_stream.py" "${SCALE}" 1 "${QUERIES}" || {
  # Fallback: call qgen directly if the wrapper signature differs.
  DBGEN="${NDSH_DIR}/tpch-gen/target/dbgen"
  ( cd "${DBGEN}" && for q in $(seq 1 22); do DSS_QUERY=queries ./qgen -d "${q}" -s "${SCALE}"; done ) \
      > "${QUERIES}/stream.sql"
}
# normalise to the single-stream filename the runners expect
[ -f "${QUERIES}/stream.sql" ] || find "${QUERIES}" -name '*.sql' -exec cat {} + > "${QUERIES}/stream.sql"

rm -rf "${WORK}"
log "DONE. parquet=$(du -sh "${FINAL}"|cut -f1)  tables=$(ls "${FINAL}"|wc -l)  stream=${QUERIES}/stream.sql"
