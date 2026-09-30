#!/bin/bash
# ==========================================================================
# run.sh — the single entry point for the TPC-H engine comparison.
#
# For each requested scale factor: ensure the dataset exists on disk (generate
# it with datagen/gen_tpch.sh if missing), validate it, stage it disk ->
# ramdisk, run each requested engine against the ramdisk copy, then free the
# ramdisk before the next scale factor. Each engine self-merges its 22 rows into
# results/all_results.csv.
#
#   ./run.sh [SF_SPEC ...]
#
#   SF_SPEC   scale factors to run. A bare number (e.g. 100) or an inclusive
#             range over the canonical set {30,50,100,300,500,700} (e.g. 30-300).
#             Default: 30 50 100 300 500 700
#
# Env:
#   ENGINES   space-separated engines, in run order.
#             Default: "rapids polars_gpu duckdb_cpu sirius"
#   DATA_DIR  where datasets live on disk, as $DATA_DIR/sf<SF>/parquet/<table>/
#             (required). Missing datasets are generated here.
#   SCRATCH   engine scratch root (required; runners use $SCRATCH/<engine>).
#   SHM       ramdisk root (default /dev/shm).
#   QUERIES   query subset (default "1 2 ... 22").
#   KEEP_RAMDISK=1   keep the staged ramdisk copy after a scale factor (default: delete).
#   GEN_PARALLEL / GEN_BATCH   override dbgen chunking for generation.
#   DRIVER_MEM, GPU_PART_MB, MIN_FREE_GB ... passed through to runners.
#   RUN_CSV_DIR=<dir>  smoke mode: nothing is merged; each engine's rows stay in
#             <dir>/<engine>_sf<SF>.csv, a query x engine table of status and row
#             counts is printed at the end, and the exit code is 1 unless every
#             query is OK (`make smoke`).
# The tool paths (PY, POLARS_PY, JAVA_HOME, SPARK_HOME, RAPIDS_JAR,
# SIRIUS_DUCKDB, SIRIUS_ENVLIB, DBGEN_DIR) come from the image.
#
# Examples:
#   ./run.sh                       # all four engines, SF30..SF700 (normally: make bench)
#   ./run.sh 30-300                # SF30,50,100,300
#   ENGINES="duckdb_cpu" ./run.sh 500 700
# ==========================================================================
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHM="${SHM:-/dev/shm}"
DATA_DIR="${DATA_DIR:?DATA_DIR is not set}"
: "${SCRATCH:?SCRATCH is not set}" "${PY:?PY is not set}"
ENGINES="${ENGINES:-rapids polars_gpu duckdb_cpu sirius}"
QUERIES="${QUERIES:-$(seq 1 22)}"
CANON_SFS="30 50 100 300 500 700"

RESULTS="${REPO}/results"
LOG="${RESULTS}/run.log"
RUN_CSV_DIR="${RUN_CSV_DIR:-}"

: > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
shm_free_gb(){ df -k --output=avail "${SHM}" | tail -1 | awk '{print int($1/1024/1024)}'; }

# Expand SF_SPEC tokens: a bare number is itself; "A-B" expands to the canonical
# scale factors within [A,B].
expand_sfs(){
  local tok lo hi sf out=""
  for tok in "$@"; do
    if [[ "${tok}" == *-* ]]; then
      lo="${tok%-*}"; hi="${tok#*-}"
      for sf in ${CANON_SFS}; do
        [ "${sf}" -ge "${lo}" ] && [ "${sf}" -le "${hi}" ] && out+=" ${sf}"
      done
    else
      out+=" ${tok}"
    fi
  done
  echo ${out}
}

# Drop empty (0-row) parquet part-files from the ramdisk copy so every engine
# reads identical files (Sirius' GPU reader errors on a file with no row groups;
# harmless to others).
remove_empty_parquets(){
  "${PY}" - "$1" <<'PY'
import glob, os, sys
import pyarrow.parquet as pq
for f in glob.glob(f"{sys.argv[1]}/*/*.parquet"):
    if pq.ParquetFile(f).metadata.num_rows == 0:
        os.remove(f)
PY
}

run_engine(){   # $1=engine  $2=ramdisk_parquet  $3=SF
  local e="$1" pq="$2" sf="$3" smoke=()
  [ -z "${RUN_CSV_DIR}" ] || smoke=(TPCH_MERGE=0 OUT_CSV="${RUN_CSV_DIR}/${e}_sf${sf}.csv")
  case "${e}" in
    rapids)     env "${smoke[@]}" TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/rapids/run_tpch_safe.sh"  "${QUERIES}" ;;
    polars_gpu) env "${smoke[@]}" TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/polars/run_polars_gpu.sh" "${QUERIES}" ;;
    duckdb_cpu) env "${smoke[@]}" TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/duckdb/run_duckdb.sh"      "${QUERIES}" ;;
    sirius)     env "${smoke[@]}" SIRIUS_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/sirius/run_sirius.sh"    "${QUERIES}" ;;
    *) log "    unknown engine '${e}'"; return 1 ;;
  esac
}

# Ensure $DATA_DIR/sf<SF>/parquet exists and validates (datagen/dataset.sh
# generates it if missing). Echoes the parquet dir on success, empty on failure.
ensure_dataset(){
  local sf="$1"
  if ! "${REPO}/datagen/dataset.sh" "${sf}" >>"${LOG}" 2>&1; then
    log "    !! SF${sf} dataset could not be generated or failed validation (see ${LOG}, results/GENERATOR.md)"; return 1
  fi
  echo "${DATA_DIR}/sf${sf}/parquet"
}

SFS="$(expand_sfs "${@:-30 50 100 300 500 700}")"
log "TPC-H run. SFs=[${SFS}] engines=[${ENGINES}] data=${DATA_DIR} ramdisk=${SHM}${RUN_CSV_DIR:+ smoke -> ${RUN_CSV_DIR}}"
[ -z "${RUN_CSV_DIR}" ] || { mkdir -p "${RUN_CSV_DIR}"; rm -f "${RUN_CSV_DIR}"/*_sf*.csv; }

for SF in ${SFS}; do
  log "=== SF${SF} ==="

  DISK_PQ="$(ensure_dataset "${SF}")"
  [ -n "${DISK_PQ}" ] || { log "    skipping SF${SF}"; continue; }

  # Stage disk -> ramdisk (need ~1x parquet size + headroom).
  need=$(( SF * 36 * 13 / 1000 + 20 ))
  RAM_DIR="${SHM}/tpch_sf${SF}"; RAM_PQ="${RAM_DIR}/parquet"
  rm -rf "${RAM_DIR}"
  if [ "$(shm_free_gb)" -lt "${need}" ]; then
    log "    !! not enough ramdisk (need ~${need}GB, free $(shm_free_gb)GB) -> skipping SF${SF}"; continue
  fi
  log "    staging ${DISK_PQ} -> ${RAM_PQ} ..."
  mkdir -p "${RAM_DIR}"
  cp -r "${DISK_PQ}" "${RAM_PQ}" || { log "    !! stage failed"; rm -rf "${RAM_DIR}"; continue; }
  remove_empty_parquets "${RAM_PQ}"
  log "    staged: $(du -sh "${RAM_PQ}" 2>/dev/null | cut -f1)"

  for E in ${ENGINES}; do
    log "    --- SF${SF} / ${E} ---"
    run_engine "${E}" "${RAM_PQ}" "${SF}" >>"${LOG}" 2>&1 || log "        ${E} returned nonzero"
    if [ -n "${RUN_CSV_DIR}" ]; then
      ok=$(awk -F, '$2=="OK"' "${RUN_CSV_DIR}/${E}_sf${SF}.csv" 2>/dev/null | wc -l)
      log "        ${E}: ${ok}/$(echo ${QUERIES} | wc -w) OK (kept in ${RUN_CSV_DIR})"
    else
      ok=$(awk -F, -v e="${E}" -v s="${SF}" '$1==e && $2==s && $4=="OK"' \
             "${RESULTS}/all_results.csv" 2>/dev/null | wc -l)
      log "        ${E}: ${ok}/22 OK (merged -> all_results.csv)"
    fi
  done

  [ "${KEEP_RAMDISK:-0}" = 1 ] || rm -rf "${RAM_DIR}"
done

if [ -z "${RUN_CSV_DIR}" ]; then
  log "=== done -> ${RESULTS}/all_results.csv ==="
  exit 0
fi
# Smoke summary: status:rows per query x engine; fail unless every query is OK.
"${PY}" - "${RUN_CSV_DIR}" "${ENGINES}" "${SFS}" "$(echo ${QUERIES})" <<'PY' | tee -a "${LOG}"
import csv, os, sys
d, engines, sfs, queries = sys.argv[1], sys.argv[2].split(), sys.argv[3].split(), sys.argv[4].split()
bad = 0
for sf in sfs:
    cell = {}
    for e in engines:
        p = os.path.join(d, f"{e}_sf{sf}.csv")
        for r in (csv.DictReader(open(p)) if os.path.exists(p) else []):
            v = r["result_rows_or_error"]
            cell[(r["query"], e)] = v if r["status"] == "OK" else f"{r['status']}:{v[:20]}"
    print(f"SF{sf}  " + "".join(f"{e:>14}" for e in engines))
    for q in queries:
        row = [cell.get((f"query{q}", e), "MISSING") for e in engines]
        bad += sum(not v.isdigit() for v in row)
        print(f"query{q:<4}" + "".join(f"{v:>14}" for v in row))
print("SMOKE OK" if bad == 0 else f"SMOKE FAILED: {bad} queries not OK")
sys.exit(1 if bad else 0)
PY
