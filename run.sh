#!/bin/bash
# ==========================================================================
# run.sh — the single entry point for the TPC-H engine comparison.
#
# For each requested scale factor: ensure the dataset exists on disk (generate
# with the NDS-H pipeline if missing), validate it is clean external NDS-H, stage
# it disk -> ramdisk, run each requested engine against the ramdisk copy, then
# free the ramdisk before the next scale factor. Each engine self-merges its 22
# rows into results/all_results.csv.
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
#             Also available: polars_cpu.
#   DATA_DIR  where datasets live on disk, as $DATA_DIR/sf<SF>/parquet/<table>/.
#             Default: $REPO/data. Missing datasets are generated here.
#   SHM       ramdisk root (default /dev/shm).
#   QUERIES   query subset (default "1 2 ... 22").
#   KEEP_RAMDISK=1   keep the staged ramdisk copy after a scale factor (default: delete).
#   GEN_PARALLEL / GEN_BATCH   override NDS-H generation chunking.
#   DRIVER_MEM, SPARK_SCRATCH, GPU_PART_MB, MIN_FREE_GB ... passed through to runners.
#
# Examples:
#   ./run.sh                       # all four engines, SF30..SF700
#   ./run.sh 30-300                # SF30,50,100,300
#   ENGINES="duckdb_cpu" ./run.sh 500 700
# ==========================================================================
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHM="${SHM:-/dev/shm}"
DATA_DIR="${DATA_DIR:-${REPO}/data}"
ENGINES="${ENGINES:-rapids polars_gpu duckdb_cpu sirius}"
QUERIES="${QUERIES:-$(seq 1 22)}"
CANON_SFS="30 50 100 300 500 700"

PIPELINE="${REPO}/rapids/nds_h_pipeline.sh"
VALIDATOR="${REPO}/results/validate_dataset.py"
RESULTS="${REPO}/results"
LOG="${RESULTS}/run.log"
# Sirius' DuckDB build (for dropping empty parquet part-files its GPU reader
# rejects). Optional: skipped with a warning if absent.
SIRIUS_DUCKDB="${SIRIUS_DUCKDB:-${REPO}/sirius/sirius/build/release/duckdb}"
SIRIUS_ENVLIB="${SIRIUS_ENVLIB:-${REPO}/sirius/sirius/.pixi/envs/default/lib}"
export PATH="${HOME}/.pixi/bin:${PATH}"

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

# Drop empty (0-row) parquet part-files so every engine reads identical files
# (Sirius' GPU reader errors on a file with no row groups; harmless to others).
remove_empty_parquets(){
  local pq="$1" t d
  [ -x "${SIRIUS_DUCKDB}" ] || { log "    (skip empty-parquet cleanup: sirius duckdb not built)"; return; }
  for t in region nation supplier customer part partsupp orders lineitem; do
    d="${pq}/${t}"; [ -d "${d}" ] || continue
    LD_LIBRARY_PATH="${SIRIUS_ENVLIB}" SIRIUS_DISABLE=1 "${SIRIUS_DUCKDB}" -noheader -list -c \
      "SELECT file_name FROM parquet_file_metadata('${d}/*.parquet') WHERE num_rows=0;" 2>/dev/null \
      | grep -vE 'mbind|Operation|^$' | while read -r f; do [ -f "${f}" ] && rm -f "${f}"; done
  done
}

run_engine(){   # $1=engine  $2=ramdisk_parquet  $3=SF
  local e="$1" pq="$2" sf="$3"
  case "${e}" in
    rapids)     TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/rapids/run_tpch_safe.sh"  "${QUERIES}" ;;
    polars_gpu) TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/polars/run_polars_gpu.sh" "${QUERIES}" ;;
    polars_cpu) TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/polars/run_polars.sh"     "${QUERIES}" ;;
    duckdb_cpu) TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/duckdb/run_duckdb.sh"      "${QUERIES}" ;;
    sirius)     SIRIUS_PARQUET="${pq}" TPCH_SF="${sf}" bash "${REPO}/sirius/run_sirius.sh"    "${QUERIES}" ;;
    *) log "    unknown engine '${e}'"; return 1 ;;
  esac
}

# Ensure $DATA_DIR/sf<SF>/parquet exists and is clean external NDS-H; generate it
# if missing. Echoes the parquet dir on success, empty on failure.
ensure_dataset(){
  local sf="$1" out="${DATA_DIR}/sf${sf}" pq="${DATA_DIR}/sf${sf}/parquet"
  if [ ! -d "${pq}" ]; then
    local par="${GEN_PARALLEL:-$(( sf * 2 ))}"; [ "${par}" -lt 20 ] && par=20
    local batch="${GEN_BATCH:-25}"
    log "    dataset absent -> generating SF${sf} into ${out} (PARALLEL=${par} BATCH=${batch})"
    mkdir -p "${DATA_DIR}"
    if ! "${PIPELINE}" "${sf}" "${par}" "${batch}" "${out}" >>"${LOG}" 2>&1; then
      log "    !! generation failed for SF${sf}"; return 1
    fi
  fi
  if ! python3 "${VALIDATOR}" "${pq}" "${sf}" >>"${LOG}" 2>&1; then
    log "    !! SF${sf} dataset failed validation (not clean external NDS-H — see results/GENERATOR.md)"; return 1
  fi
  echo "${pq}"
}

SFS="$(expand_sfs "${@:-30 50 100 300 500 700}")"
log "TPC-H run. SFs=[${SFS}] engines=[${ENGINES}] data=${DATA_DIR} ramdisk=${SHM}"

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
    ok=$(awk -F, -v e="${E}" -v s="${SF}" '$1==e && $2==s && $4=="OK"' \
           "${RESULTS}/all_results.csv" 2>/dev/null | wc -l)
    log "        ${E}: ${ok}/22 OK (merged -> all_results.csv)"
  done

  [ "${KEEP_RAMDISK:-0}" = 1 ] || rm -rf "${RAM_DIR}"
done

log "=== done -> ${RESULTS}/all_results.csv ==="
