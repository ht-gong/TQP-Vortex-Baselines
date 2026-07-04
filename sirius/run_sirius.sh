#!/bin/bash
# Run TPC-H queries 1-22 through Sirius (GPU, gpu_execution) on the ramdisk
# SF500 parquet, SAFELY -- one duckdb process per query so each gets a clean GPU
# context and any spill scratch is reclaimed on exit, plus a disk watchdog that
# kills a query (and moves on) if free disk drops below a threshold so heavy
# spill can never wedge the box. Mirrors run_polars.sh / rapids/run_tpch_safe.sh.
#
# Single RTX 5090; Sirius spills GPU -> pinned host RAM -> disk per sirius.yaml.
# Per-query time excludes engine startup (a warm-up query absorbs GPU/JIT init);
# results are appended incrementally.
#
#   run_sirius.sh [QUERY_LIST]   e.g. "1 2 3"   (default: 1..22)
set -uo pipefail

QUERIES="${1:-$(seq 1 22)}"
DIR="/workspace/baseline/sirius"
RAM_PQ="${SIRIUS_PARQUET:-/dev/shm/tpch_sf500/parquet}"
STREAM="/workspace/baseline/results/queries/stream_qualification.sql"
LOGDIR="/workspace/baseline/results"
TPCH_SF="${TPCH_SF:-500}"
# Per-query rows go to a throwaway temp CSV and are folded into the single
# canonical results/all_results.csv at the end (merge_results.py); no per-run
# CSV is kept. TAG only names the gitignored per-query log files.
TAG="${SIRIUS_TAG:-sirius_sf${TPCH_SF}}"
OUT_CSV="${OUT_CSV:-/tmp/tpch_sirius_sf${TPCH_SF}.csv}"
DETAIL_CSV="/tmp/tpch_sirius_sf${TPCH_SF}_detail.csv"
SPILL="/dev/shm/_sirius_spill"   # matches downgrade_root_dirs in sirius.yaml
LOG="${LOGDIR}/${TAG}_run.log"
MIN_FREE_GB="${MIN_FREE_GB:-25}"

export SIRIUS_DUCKDB="${SIRIUS_DUCKDB:-${DIR}/sirius/build/release/duckdb}"
export SIRIUS_CONFIG_FILE="${SIRIUS_CONFIG_FILE:-${DIR}/sirius.yaml}"
export SIRIUS_ITERS="${SIRIUS_ITERS:-2}"
export SIRIUS_TIMEOUT="${SIRIUS_TIMEOUT:-2400}"
export SIRIUS_DETAIL_CSV="${DETAIL_CSV}"
export SIRIUS_LOG_DIR="${SIRIUS_LOG_DIR:-${LOGDIR}/${TAG}_logs}"
# kvikio POSIX/compat mode: no io_uring, no cuFile/GDS (nvidia-fs absent here).
export KVIKIO_COMPAT_MODE="${KVIKIO_COMPAT_MODE:-ON}"

# Activate the pixi runtime env so the duckdb binary finds its conda libs
# (libcudf, rmm, cudart, ...). We resolve LD paths via `pixi run` at call time.
# ~/.pixi/bin must be on PATH: Sirius's own pixi_activate.sh calls bare `pixi`
# during env activation, so invoking pixi by absolute path alone isn't enough.
export PATH="${HOME}/.pixi/bin:${PATH}"
PIXI="${HOME}/.pixi/bin/pixi"
mkdir -p "${SPILL}" "${SIRIUS_LOG_DIR}"
rm -f "${OUT_CSV}" "${DETAIL_CSV}"; : > "${LOG}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
free_gb(){ df -k --output=avail / | tail -1 | awk '{print int($1/1024/1024)}'; }

if [ ! -x "${SIRIUS_DUCKDB}" ]; then
  log "ERROR: sirius duckdb not built at ${SIRIUS_DUCKDB} -- run setup_sirius.sh first"; exit 1
fi

log "Sirius per-query run. queries=[${QUERIES}] iters=${SIRIUS_ITERS} timeout=${SIRIUS_TIMEOUT}s min_free=${MIN_FREE_GB}GB"
log "duckdb=${SIRIUS_DUCKDB}"
log "config=${SIRIUS_CONFIG_FILE} ; parquet=${RAM_PQ}"
log "free disk at start: $(free_gb)GB ; ramdisk: $(du -sh ${RAM_PQ%/parquet} 2>/dev/null | cut -f1)"

for q in ${QUERIES}; do
  rm -rf "${SPILL:?}"/* 2>/dev/null
  qlog="${LOGDIR}/${TAG}_q${q}.log"
  log "=== query ${q} starting (free $(free_gb)GB) ==="
  # Run inside the pixi env so the duckdb binary's conda libs are on the loader path.
  ( cd "${DIR}/sirius" && "${PIXI}" run --manifest-path "${DIR}/sirius/pixi.toml" \
      python "${DIR}/run_tpch_sirius.py" "${RAM_PQ}" "${STREAM}" "${OUT_CSV}" "${q}" append ) \
      >"${qlog}" 2>&1 &
  pid=$!

  killed=0
  while kill -0 "${pid}" 2>/dev/null; do
    if [ "$(free_gb)" -lt "${MIN_FREE_GB}" ]; then
      log "    !! free disk < ${MIN_FREE_GB}GB during query ${q} -> killing to protect the box"
      pkill -9 -P "${pid}" 2>/dev/null; kill -9 "${pid}" 2>/dev/null
      pkill -9 -f "run_tpch_sirius.py" 2>/dev/null; pkill -9 -f "duckdb -f /tmp/sirius_q" 2>/dev/null
      killed=1; break
    fi
    sleep 3
  done
  wait "${pid}" 2>/dev/null; rc=$?

  if [ "${killed}" = 1 ]; then
    echo "query${q},KILLED_DISK,NA,exceeded_disk_scratch" >> "${OUT_CSV}"
    log "    query ${q}: KILLED (disk)."
  else
    res=$(grep -E "^query${q}[, ]" "${OUT_CSV}" | tail -1)
    log "    query ${q}: done rc=${rc} -> ${res:-<no csv row>}"
  fi
  rm -rf "${SPILL:?}"/* 2>/dev/null
done

log "ALL DONE. free disk: $(free_gb)GB"
log "results:"; cat "${OUT_CSV}" | tee -a "${LOG}"

# Fold this run into the single canonical results table, then drop the temp CSVs.
if [ "${TPCH_MERGE:-1}" = 1 ]; then
  python3 /workspace/baseline/merge_results.py sirius "${TPCH_SF}" "${OUT_CSV}" | tee -a "${LOG}" \
    && rm -f "${OUT_CSV}" "${DETAIL_CSV}"
fi
