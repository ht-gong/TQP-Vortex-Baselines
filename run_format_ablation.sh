#!/bin/bash
# ==========================================================================
# run_format_ablation.sh -- parquet-format ablation driver (results/FORMAT_ABLATION.md).
#
# Runs every engine over every parquet-format VARIANT of the same SF dataset for
# ROUNDS independent cold rounds, staging each variant disk -> ramdisk like
# run.sh, and folds the rows into results/format_ablation.csv via
# merge_format_results.py. Cold protocol everywhere: one fresh process per
# query for the GPU engines (their runners' default), DuckDB in `views` mode
# with no prewarm (parquet scanned by every query), Sirius 1 iteration.
#
#   ./run_format_ablation.sh
#
# Env:
#   SF        scale factor (default 100)
#   DATA_DIR  variants live at $DATA_DIR/fmt_sf$SF/<variant>/parquet/ (default
#             /data/haotiang/parquet-ablation); generate them with
#             datagen/gen_format_variants.sh
#   VARIANTS  space-separated "<layout>-<encoding>-<compression>" names
#             (default: the 9 shuffle variants then the 3 keyorder ones)
#   ROUNDS    rounds to run (default 3); ROUND_START to resume (default 1)
#   ENGINES   default "rapids polars_gpu duckdb_cpu sirius"
#   QUERIES   default 1..22
#   GPU       CUDA_VISIBLE_DEVICES for every engine (default 5)
#   SHM       ramdisk root (default /dev/shm)
# ==========================================================================
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SF="${SF:-100}"
DATA_DIR="${DATA_DIR:-/data/haotiang/parquet-ablation}"
SHM="${SHM:-/dev/shm}"
ENGINES="${ENGINES:-rapids polars_gpu duckdb_cpu sirius}"
QUERIES="${QUERIES:-$(seq 1 22)}"
ROUNDS="${ROUNDS:-3}"; ROUND_START="${ROUND_START:-1}"
VARIANTS="${VARIANTS:-shuffle-plain-snappy shuffle-dict-snappy shuffle-delta-snappy \
shuffle-plain-zstd shuffle-dict-zstd shuffle-delta-zstd \
shuffle-plain-lz4raw shuffle-dict-lz4raw shuffle-delta-lz4raw \
keyorder-plain-snappy keyorder-dict-snappy keyorder-delta-snappy}"
export CUDA_VISIBLE_DEVICES="${GPU:-5}"
# Engine scratch on the NVMe, not the nearly-full root disk (rapids' disk
# watchdog kills a query when / drops below MIN_FREE_GB).
export SPARK_SCRATCH="${SPARK_SCRATCH:-/data/haotiang/parquet-ablation/_spark_scratch}"
# Per-query timeout (rapids/polars_gpu QUERY_TIMEOUT, sirius SIRIUS_TIMEOUT):
# a reader bug can spin a GPU forever (seen: spark-rapids on delta+zstd).
export QUERY_TIMEOUT="${QUERY_TIMEOUT:-600}" SIRIUS_TIMEOUT="${SIRIUS_TIMEOUT:-900}"
# spark-rapids 26.04.2 hangs deterministically on about half the queries of the
# delta+zstd variant (cudf decoder bug, see results/FORMAT_ABLATION.md); a
# shorter cap there keeps the known-bad cells from eating hours per round.
RAPIDS_DELTA_ZSTD_TIMEOUT="${RAPIDS_DELTA_ZSTD_TIMEOUT:-300}"

RESULTS="${REPO}/results"
ABL="${RESULTS}/format_ablation"          # per-run logs + raw per-run CSVs
LOG="${ABL}/run.log"
PY="${PYTHON:-${REPO}/datagen-venv/bin/python}"
SIRIUS_DUCKDB="${SIRIUS_DUCKDB:-${REPO}/sirius/sirius/build/release/duckdb}"
SIRIUS_ENVLIB="${SIRIUS_ENVLIB:-${REPO}/sirius/sirius/.pixi/envs/default/lib}"
export PATH="${HOME}/.pixi/bin:${PATH}"

mkdir -p "${ABL}"
log(){ echo "[$(date '+%F %T')] $*" | tee -a "${LOG}"; }
shm_free_gb(){ df -k --output=avail "${SHM}" | tail -1 | awk '{print int($1/1024/1024)}'; }

remove_empty_parquets(){   # same as run.sh: Sirius' reader rejects 0-row part-files
  local pq="$1" t d
  [ -x "${SIRIUS_DUCKDB}" ] || { log "    (skip empty-parquet cleanup: sirius duckdb not built)"; return; }
  for t in region nation supplier customer part partsupp orders lineitem; do
    d="${pq}/${t}"; [ -d "${d}" ] || continue
    LD_LIBRARY_PATH="${SIRIUS_ENVLIB}" SIRIUS_DISABLE=1 "${SIRIUS_DUCKDB}" -noheader -list -c \
      "SELECT file_name FROM parquet_file_metadata('${d}/*.parquet') WHERE num_rows=0;" 2>/dev/null \
      | grep -vE 'mbind|Operation|^$' | while read -r f; do [ -f "${f}" ] && rm -f "${f}"; done
  done
}

# Run one engine cold on a ramdisk variant; leaves its per-query CSV at $3.
run_engine(){   # $1=engine $2=ramdisk_parquet $3=out_csv
  local e="$1" pq="$2" out="$3"
  rm -f "${out}"
  case "${e}" in
    rapids)     local qt="${QUERY_TIMEOUT}"; [[ "${V}" == *delta-zstd ]] && qt="${RAPIDS_DELTA_ZSTD_TIMEOUT}"
                QUERY_TIMEOUT="${qt}" TPCH_MERGE=0 OUT_CSV="${out}" TPCH_PARQUET="${pq}" TPCH_SF="${SF}" bash "${REPO}/rapids/run_tpch_safe.sh"  "${QUERIES}" ;;
    polars_gpu) TPCH_MERGE=0 OUT_CSV="${out}" TPCH_PARQUET="${pq}" TPCH_SF="${SF}" bash "${REPO}/polars/run_polars_gpu.sh" "${QUERIES}" ;;
    polars_cpu) TPCH_MERGE=0 OUT_CSV="${out}" TPCH_PARQUET="${pq}" TPCH_SF="${SF}" bash "${REPO}/polars/run_polars.sh"     "${QUERIES}" ;;
    duckdb_cpu) TPCH_MERGE=0 OUT_CSV="${out}" TPCH_PARQUET="${pq}" TPCH_SF="${SF}" \
                DUCKDB_LOAD_MODE=views RUNS=1 WARMUPS=0 bash "${REPO}/duckdb/run_duckdb.sh" "${QUERIES}" ;;
    sirius)     TPCH_MERGE=0 OUT_CSV="${out}" SIRIUS_PARQUET="${pq}" TPCH_SF="${SF}" SIRIUS_ITERS=1 \
                SIRIUS_TAG="sirius_fmt" bash "${REPO}/sirius/run_sirius.sh" "${QUERIES}" ;;
    *) log "    unknown engine '${e}'"; return 1 ;;
  esac
}

# Per-query logs the runners write to fixed paths; keep a copy per run.
engine_logs(){
  case "$1" in
    rapids)     echo "${RESULTS}/safe_run.log ${RESULTS}/q*.log" ;;
    polars_gpu) echo "${RESULTS}/polars_gpu_run.log ${RESULTS}/polars_gpu_q*.log" ;;
    polars_cpu) echo "${RESULTS}/polars_run.log ${RESULTS}/polars_q*.log" ;;
    duckdb_cpu) echo "${RESULTS}/duckdb_run.log" ;;
    sirius)     echo "${RESULTS}/sirius_fmt_run.log ${RESULTS}/sirius_fmt_q*.log" ;;
  esac
}

log "FORMAT ABLATION SF${SF}: rounds ${ROUND_START}..${ROUNDS} engines=[${ENGINES}] gpu=${CUDA_VISIBLE_DEVICES}"
log "variants: ${VARIANTS}"

for R in $(seq "${ROUND_START}" "${ROUNDS}"); do
  for V in ${VARIANTS}; do
    DISK_PQ="${DATA_DIR}/fmt_sf${SF}/${V}/parquet"
    [ -d "${DISK_PQ}" ] || { log "!! round ${R}: variant ${V} missing at ${DISK_PQ} -> skipped"; continue; }
    log "=== round ${R} / ${V} ==="
    need=$(( SF * 36 * 15 / 1000 + 20 ))
    RAM_DIR="${SHM}/tpch_fmt_sf${SF}"; RAM_PQ="${RAM_DIR}/parquet"
    rm -rf "${RAM_DIR}"
    if [ "$(shm_free_gb)" -lt "${need}" ]; then
      log "    !! ramdisk short (need ~${need}GB, free $(shm_free_gb)GB) -> skipping ${V}"; continue
    fi
    t0=$(date +%s)
    mkdir -p "${RAM_DIR}"
    cp -r "${DISK_PQ}" "${RAM_PQ}" || { log "    !! stage failed"; rm -rf "${RAM_DIR}"; continue; }
    remove_empty_parquets "${RAM_PQ}"
    log "    staged $(du -sh "${RAM_PQ}" | cut -f1) in $(( $(date +%s) - t0 ))s"

    for E in ${ENGINES}; do
      RUN_DIR="${ABL}/sf${SF}/r${R}/${V}/${E}"; mkdir -p "${RUN_DIR}"
      out="${RUN_DIR}/run.csv"
      log "    --- round ${R} / ${V} / ${E} ---"
      t0=$(date +%s)
      run_engine "${E}" "${RAM_PQ}" "${out}" >"${RUN_DIR}/driver.log" 2>&1 || log "        ${E} returned nonzero"
      # shellcheck disable=SC2086
      cp $(engine_logs "${E}") "${RUN_DIR}/" 2>/dev/null
      if [ -s "${out}" ]; then
        "${PY}" "${REPO}/merge_format_results.py" "${E}" "${SF}" "${V}" "${R}" "${out}" "${RESULTS}" >>"${LOG}" 2>&1
        ok=$(awk -F, '$2=="OK"' "${out}" | wc -l)
        log "        ${E}: ${ok}/$(echo ${QUERIES} | wc -w) OK, $(( $(date +%s) - t0 ))s wall (merged -> format_ablation.csv)"
      else
        log "        ${E}: !! no results CSV"
      fi
    done
    rm -rf "${RAM_DIR}"
  done
done
log "=== done -> ${RESULTS}/format_ablation.csv ==="
