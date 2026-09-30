#!/bin/bash
# In-repo, disk-safe TPC-H generate -> parquet pipeline. Drop-in replacement for
# rapids/nds_h_pipeline.sh with the same CLI, batching and output layout, but
# with no dependency on the NVIDIA spark-rapids-benchmarks checkout: dbgen and
# the transcode live in datagen/ (see datagen/gen_tpch.py).
#
# For each batch of BATCH chunks: run dbgen for those chunks, transcode them to
# parquet with Spark, move the part-files into the consolidated per-table dirs,
# then delete the raw text before the next batch (peak disk ~= final parquet +
# one batch raw + one batch parquet). Finally validate the dataset.
#
#   datagen/gen_tpch.sh <SCALE> <PARALLEL> <BATCH> <OUT_DIR>
#     SCALE     scale factor (GB), e.g. 500
#     PARALLEL  number of dbgen chunks the tables are split into, e.g. 1000
#     BATCH     chunks generated+transcoded per iteration, e.g. 100
#     OUT_DIR   output root; final parquet lands in $OUT_DIR/parquet/<table>/
#
# Env: DRIVER_MEM (Spark driver heap, default 96g), DBGEN_DIR, SPARK_LOCAL_DIRS.
#   RAW_STORE=<dir>   keep dbgen's raw text per batch under <dir>/b<start>-<end>/
#                     instead of a throwaway _raw; batches already present there
#                     are reused (dbgen skipped). Lets several format variants
#                     be transcoded from ONE dbgen run (results/FORMAT_ABLATION.md).
#   TRANSCODE_OPTS    extra gen_tpch.py transcode flags, e.g.
#                     "--compression zstd --dictionary false --writer-version v2".
#                     Empty = the NDS-H reference format.
# Requires datagen/setup_datagen.sh to have been run once.
set -euo pipefail

SCALE="${1:?scale}"; PARALLEL="${2:?parallel}"; BATCH="${3:?batch}"; OUT_DIR="${4:?out_dir}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"
DRIVER_MEM="${DRIVER_MEM:-96g}"

RAW_STORE="${RAW_STORE:-}"
TRANSCODE_OPTS="${TRANSCODE_OPTS:-}"
RAW="${OUT_DIR}/_raw"
TMP="${OUT_DIR}/_tmp_pq"
FINAL="${OUT_DIR}/parquet"
WORK="${OUT_DIR}/_work"
LOG="${OUT_DIR}/pipeline.log"

ALL_TABLES="customer lineitem nation orders part partsupp region supplier"
SCALED_TABLES="customer,lineitem,orders,part,partsupp,supplier"   # nation/region only at chunk 1

# shellcheck disable=SC1091
source "${HERE}/env.sh" >/dev/null 2>&1

mkdir -p "${OUT_DIR}" "${FINAL}" "${WORK}"
: > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }

log "tpch datagen: SCALE=${SCALE} PARALLEL=${PARALLEL} BATCH=${BATCH} -> ${FINAL}"
log "  dbgen=${DBGEN_DIR}  spark=$(python -c 'import pyspark;print(pyspark.__version__)')  java=${JAVA_HOME}"
[ -z "${RAW_STORE}" ]      || log "  raw store: ${RAW_STORE} (raw text kept/reused per batch)"
[ -z "${TRANSCODE_OPTS}" ] || log "  transcode opts: ${TRANSCODE_OPTS}"

T_START=$(date +%s); T_DBGEN=0; T_TRANSCODE=0

start_chunk=1
while [ "${start_chunk}" -le "${PARALLEL}" ]; do
  end_chunk=$(( start_chunk + BATCH - 1 ))
  [ "${end_chunk}" -gt "${PARALLEL}" ] && end_chunk="${PARALLEL}"
  log "=== batch chunks ${start_chunk}-${end_chunk}/${PARALLEL} ==="

  # 1) generate raw text for this chunk range (or reuse it from RAW_STORE)
  if [ -n "${RAW_STORE}" ]; then RAW="${RAW_STORE}/b${start_chunk}-${end_chunk}"; fi
  if [ -n "${RAW_STORE}" ] && [ -f "${RAW}/.complete" ]; then
    log "    reusing raw from ${RAW}: $(du -sh "${RAW}" | cut -f1)"
  else
    t0=$(date +%s)
    rm -rf "${RAW}"; mkdir -p "${RAW}"
    python "${HERE}/gen_tpch.py" dbgen --scale "${SCALE}" --parallel "${PARALLEL}" \
        --range "${start_chunk},${end_chunk}" --out "${RAW}" >>"${LOG}" 2>&1
    [ -z "${RAW_STORE}" ] || touch "${RAW}/.complete"
    T_DBGEN=$(( T_DBGEN + $(date +%s) - t0 ))
    log "    generated raw: $(du -sh "${RAW}" | cut -f1)  ($(( $(date +%s) - t0 ))s)"
  fi

  # 2) nation/region are only produced with chunk 1
  if [ "${start_chunk}" -eq 1 ]; then TABLES_ARG=""; else TABLES_ARG="--tables ${SCALED_TABLES}"; fi

  # 3) transcode this batch -> TMP (fresh Spark session per batch, as upstream)
  rm -rf "${TMP}"
  t0=$(date +%s)
  ( cd "${WORK}" && spark-submit --master "local[*]" --driver-memory "${DRIVER_MEM}" \
        --conf spark.sql.shuffle.partitions=256 \
        "${HERE}/gen_tpch.py" transcode --input "${RAW}" --output "${TMP}" \
        --output-mode overwrite --log-level WARN ${TABLES_ARG} ${TRANSCODE_OPTS} ) >>"${LOG}" 2>&1
  T_TRANSCODE=$(( T_TRANSCODE + $(date +%s) - t0 ))
  log "    transcoded batch -> tmp parquet: $(du -sh "${TMP}" | cut -f1)  ($(( $(date +%s) - t0 ))s)"

  # 4) merge part-files into the consolidated final parquet dirs
  for t in ${ALL_TABLES}; do
    if [ -d "${TMP}/${t}" ]; then
      mkdir -p "${FINAL}/${t}"
      find "${TMP}/${t}" -name 'part-*.parquet' -exec mv -t "${FINAL}/${t}/" {} +
    fi
  done

  # 5) free disk: drop tmp (and raw unless it lives in RAW_STORE) for this batch
  rm -rf "${TMP}"; [ -n "${RAW_STORE}" ] || rm -rf "${RAW}"
  log "    merged; disk now: $(df -h "${OUT_DIR}" | awk 'NR==2{print $3" used / "$4" free"}')"

  start_chunk=$(( end_chunk + 1 ))
done
rm -rf "${WORK}"

log "DONE. Final parquet: $(du -sh "${FINAL}" | cut -f1)"
log "TIMING total=$(( $(date +%s) - T_START ))s dbgen=${T_DBGEN}s transcode=${T_TRANSCODE}s (SF${SCALE} PARALLEL=${PARALLEL} BATCH=${BATCH}${TRANSCODE_OPTS:+ opts: ${TRANSCODE_OPTS}})"
for t in ${ALL_TABLES}; do
  printf '  %-10s %s\n' "${t}" "$(du -sh "${FINAL}/${t}" 2>/dev/null | cut -f1)" | tee -a "${LOG}"
done

# One-generator invariant: the fresh parquet must validate as clean external
# NDS-H-equivalent data (writer, in-spec part.p_brand, row counts). set -e makes
# a failure fatal so a bad dataset can never silently feed results.
log "validating ${FINAL} as SF${SCALE} ..."
python "${ROOT}/results/validate_dataset.py" "${FINAL}" "${SCALE}" 2>&1 | tee -a "${LOG}"
