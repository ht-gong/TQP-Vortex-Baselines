#!/bin/bash
# Multi-engine TPC-H scale-factor sweep: run q1-22 at several SFs for
# Spark-RAPIDS (GPU), Polars (CPU), and Polars (GPU) — the companion to Sirius's
# sirius/run_scale_sweep.sh, so every engine has a query-time-vs-scale curve.
#
#   run_scale_sweep_all.sh [SF_LIST]        default: "30 50 100 300"
#   ENGINES="rapids polars_cpu polars_gpu"  (override the engine set)
#
# Each SF is generated into the ramdisk ONCE, all engines run on it, then it is
# deleted before the next SF. The SF500 ramdisk parquet is cleared if a big SF
# needs the room (results are already saved; regenerate with nds_h_pipeline.sh).
set -uo pipefail

SFS="${1:-30 50 100 300}"
ENGINES="${ENGINES:-polars_cpu rapids polars_gpu}"
BASE="/workspace/baseline"
SHM="/dev/shm"
RESULTS="${BASE}/results"
PIPELINE="${BASE}/rapids/nds_h_pipeline.sh"
DUCKDB="${BASE}/sirius/sirius/build/release/duckdb"
ENVLIB="${BASE}/sirius/sirius/.pixi/envs/default/lib"
LOG="${RESULTS}/scale_sweep_all.log"
export PATH="${HOME}/.pixi/bin:${PATH}"

: > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
shm_free_gb(){ df -k --output=avail "${SHM}" | tail -1 | awk '{print int($1/1024/1024)}'; }

# Drop empty (0-row) parquet part-files (harmless for Spark/Polars; keeps the
# dataset identical to what Sirius needs, so all engines run on the same files).
remove_empty_parquets(){
  local pq="$1" t d
  for t in region nation supplier customer part partsupp orders lineitem; do
    d="${pq}/${t}"; [ -d "${d}" ] || continue
    LD_LIBRARY_PATH="${ENVLIB}" SIRIUS_DISABLE=1 "${DUCKDB}" -noheader -list -c \
      "SELECT file_name FROM parquet_file_metadata('${d}/*.parquet') WHERE num_rows=0;" 2>/dev/null \
      | grep -vE 'mbind|Operation|^$' | while read -r f; do [ -f "${f}" ] && rm -f "${f}"; done
  done
}

run_engine(){         # $1=engine  $2=parquet_dir  $3=SF
  local e="$1" pq="$2" sf="$3"
  case "${e}" in
    rapids)     TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${BASE}/rapids/run_tpch_safe.sh" ;;
    polars_cpu) TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${BASE}/polars/run_polars.sh" ;;
    polars_gpu) TPCH_PARQUET="${pq}" TPCH_SF="${sf}" bash "${BASE}/polars/run_polars_gpu.sh" ;;
    *) log "    unknown engine ${e}"; return 1 ;;
  esac
}

log "Multi-engine scale sweep. SFs=[${SFS}] engines=[${ENGINES}]"
[ -x "${DUCKDB}" ] || { log "WARN: sirius duckdb missing; empty-file cleanup will be skipped"; }

for SF in ${SFS}; do
  OUT_DIR="${SHM}/tpch_sf${SF}"; PQ="${OUT_DIR}/parquet"
  need=$(( SF * 36 * 13 / 1000 + 20 ))
  log "=== SF${SF}: need ~${need}GB ramdisk, free $(shm_free_gb)GB ==="

  rm -rf "${OUT_DIR}" 2>/dev/null
  if [ "$(shm_free_gb)" -lt "${need}" ] && [ -d "${SHM}/tpch_sf500/parquet" ]; then
    log "    clearing SF500 ramdisk parquet for space (results already saved)"; rm -rf "${SHM}/tpch_sf500/parquet"
  fi
  if [ "$(shm_free_gb)" -lt "${need}" ]; then
    log "    !! not enough ramdisk (need ${need}GB, free $(shm_free_gb)GB) -> skipping SF${SF}"; continue
  fi

  PARALLEL=$(( SF * 2 )); [ "${PARALLEL}" -lt 20 ] && PARALLEL=20
  log "    generating SF${SF} parquet (PARALLEL=${PARALLEL}, BATCH=25) ..."
  if ! DRIVER_MEM="${DRIVER_MEM:-96g}" "${PIPELINE}" "${SF}" "${PARALLEL}" 25 "${OUT_DIR}" >>"${LOG}" 2>&1; then
    log "    !! generation failed for SF${SF} -> skipping"; rm -rf "${OUT_DIR}"; continue
  fi
  [ -x "${DUCKDB}" ] && remove_empty_parquets "${PQ}"
  log "    generated: $(du -sh "${PQ}" 2>/dev/null | cut -f1)"

  for E in ${ENGINES}; do
    log "    --- SF${SF} / ${E} ---"
    run_engine "${E}" "${PQ}" "${SF}" >>"${LOG}" 2>&1 || log "        ${E} returned nonzero"
    # the runner self-merged into all_results.csv; count what landed there
    ok=$(awk -F, -v e="${E}" -v s="${SF}" '$1==e && $2==s && $4=="OK"' \
           "${RESULTS}/all_results.csv" 2>/dev/null | wc -l)
    log "        ${E}: ${ok}/22 OK (merged -> all_results.csv)"
  done

  rm -rf "${OUT_DIR}"
done

# Runners self-merge into results/all_results.csv (via merge_results.py) as they
# finish, so there is nothing to combine — no per-run/per-SF CSVs are produced.
log "=== multi-engine scale sweep complete -> ${RESULTS}/all_results.csv ==="
