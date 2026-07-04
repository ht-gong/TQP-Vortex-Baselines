#!/bin/bash
# TPC-H scale-factor sweep for Sirius (GPU): run q1-22 at several scale factors
# to show how query time scales with data size (and where the single-32 GB-GPU
# OOM boundary sits). Each SF is generated into the ramdisk, cleaned, run, then
# deleted before the next so only one SF occupies /dev/shm at a time.
#
#   run_scale_sweep.sh [SF_LIST]     default: "30 50 100 300"
#
# Adds to the existing SF500 run (results/query_times_sirius.csv). Because
# SF300 (~110 GB) will not fit in /dev/shm alongside the SF500 dataset (~180 GB),
# the sweep CLEARS the SF500 ramdisk parquet when it needs the space (the SF500
# *results* are already saved; regenerate the parquet with rapids/nds_h_pipeline.sh
# if you need it again).
set -uo pipefail

SFS="${1:-30 50 100 300}"
DIR="/workspace/baseline/sirius"
ENV="${DIR}/sirius/.pixi/envs/default"
DUCKDB="${DIR}/sirius/build/release/duckdb"
RESULTS="/workspace/baseline/results"
SHM="/dev/shm"
PIPELINE="/workspace/baseline/rapids/nds_h_pipeline.sh"
LOG="${RESULTS}/sirius_scale_sweep.log"
export PATH="${HOME}/.pixi/bin:${PATH}"

: > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
shm_free_gb(){ df -k --output=avail "${SHM}" | tail -1 | awk '{print int($1/1024/1024)}'; }

# Remove empty (0-row) parquet part-files: Sirius's GPU reader errors on a file
# with no row groups, and the NDS-H/Spark generator emits one for tiny tables.
remove_empty_parquets(){
  local pq="$1" t d
  for t in region nation supplier customer part partsupp orders lineitem; do
    d="${pq}/${t}"; [ -d "${d}" ] || continue
    LD_LIBRARY_PATH="${ENV}/lib" SIRIUS_DISABLE=1 "${DUCKDB}" -noheader -list -c \
      "SELECT file_name FROM parquet_file_metadata('${d}/*.parquet') WHERE num_rows=0;" 2>/dev/null \
      | grep -vE 'mbind|Operation|^$' | while read -r f; do [ -f "${f}" ] && rm -f "${f}"; done
  done
}

log "Sirius scale sweep. SFs=[${SFS}]  duckdb=${DUCKDB}"
[ -x "${DUCKDB}" ] || { log "ERROR: build Sirius first (setup_sirius.sh)"; exit 1; }

for SF in ${SFS}; do
  OUT_DIR="${SHM}/tpch_sf${SF}"
  PQ="${OUT_DIR}/parquet"
  # parquet is ~0.36 GB/SF; require parquet*1.3 + 20 GB headroom for gen batches.
  need=$(( SF * 36 * 13 / 1000 + 20 ))
  log "=== SF${SF}: need ~${need}GB ramdisk, free $(shm_free_gb)GB ==="

  # free space: drop any stale sweep dir, then the SF500 parquet if still short.
  rm -rf "${OUT_DIR}" 2>/dev/null
  if [ "$(shm_free_gb)" -lt "${need}" ] && [ -d "${SHM}/tpch_sf500/parquet" ]; then
    log "    clearing SF500 ramdisk parquet to make room (results already saved)"
    rm -rf "${SHM}/tpch_sf500/parquet"
  fi
  if [ "$(shm_free_gb)" -lt "${need}" ]; then
    log "    !! not enough ramdisk for SF${SF} (need ${need}GB, free $(shm_free_gb)GB) -> skipping"
    continue
  fi

  # 1) generate parquet into the ramdisk
  PARALLEL=$(( SF * 2 )); [ "${PARALLEL}" -lt 20 ] && PARALLEL=20
  log "    generating SF${SF} parquet (PARALLEL=${PARALLEL}, BATCH=25) ..."
  if ! DRIVER_MEM="${DRIVER_MEM:-96g}" "${PIPELINE}" "${SF}" "${PARALLEL}" 25 "${OUT_DIR}" >>"${LOG}" 2>&1; then
    log "    !! generation failed for SF${SF} -> skipping"; rm -rf "${OUT_DIR}"; continue
  fi
  log "    generated: $(du -sh "${PQ}" 2>/dev/null | cut -f1)"

  # 2) drop empty parquet part-files (Sirius GPU-reader requirement)
  remove_empty_parquets "${PQ}"

  # 3) run Sirius q1-22 on this SF (run_sirius.sh self-merges into all_results.csv)
  log "    running Sirius q1-22 on SF${SF} ..."
  SIRIUS_PARQUET="${PQ}" TPCH_SF="${SF}" \
    bash "${DIR}/run_sirius.sh" >>"${LOG}" 2>&1
  ok=$(awk -F, -v s="${SF}" '$1=="sirius" && $2==s && $4=="OK"' \
         "${RESULTS}/all_results.csv" 2>/dev/null | wc -l)
  log "    SF${SF} done: ${ok}/22 OK (merged -> all_results.csv)"

  # 4) free the ramdisk for the next SF
  rm -rf "${OUT_DIR}"
done

# run_sirius.sh already merged each SF into results/all_results.csv; nothing to combine.
log "=== scale sweep complete. results -> ${RESULTS}/all_results.csv ==="
