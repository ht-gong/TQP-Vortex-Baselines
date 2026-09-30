#!/bin/bash
# DuckDB CPU baseline: run TPC-H q1-22 over the shared parquet dataset and fold
# the result into results/all_results.csv (like every other engine's runner).
#
#   run_duckdb.sh [QUERY_LIST]        e.g. "1 6 9"   (default: 1..22)
#
# Env: TPCH_PARQUET (dataset), TPCH_SF, PY (the image's py env, which has
# duckdb 1.5.5), and optionally RUNS, WARMUPS, DUCKDB_LOAD_MODE, DUCKDB_PK,
# DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT, TPCH_MERGE=0 (keep temp CSV).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

QUERIES="${1:-$(seq 1 22)}"
TPCH_SF="${TPCH_SF:?TPCH_SF is not set}"
TPCH_PARQUET="${TPCH_PARQUET:?TPCH_PARQUET is not set}"
STREAM="${STREAM:-${ROOT}/results/queries/stream_qualification.sql}"
# throwaway temp; folded into results/all_results.csv at the end (no per-run CSV kept)
OUT_CSV="${OUT_CSV:-/tmp/tpch_duckdb_cpu_sf${TPCH_SF}.csv}"
LOG="${LOG:-${ROOT}/results/duckdb_run.log}"
# test.py workflow: 3 prewarm passes, then one measured run per query.
# NOTE: this is WARM, unlike the cold `seconds` contract the GPU engines follow
# (see AGENTS.md). RUNS=1 WARMUPS=0 gives the cold protocol instead.
RUNS="${RUNS:-1}"
WARMUPS="${WARMUPS:-3}"

[ -d "${TPCH_PARQUET}" ] || { echo "ERROR: no parquet dir at ${TPCH_PARQUET}" >&2; exit 1; }

mkdir -p "$(dirname "${OUT_CSV}")" "$(dirname "${LOG}")"
rm -f "${OUT_CSV}"

PY="${PY:?PY is not set}"

echo "[$(date +%H:%M:%S)] duckdb_cpu SF${TPCH_SF} queries=[${QUERIES}] parquet=${TPCH_PARQUET} python=${PY}" | tee "${LOG}"
"${PY}" "${ROOT}/duckdb/run_tpch_duckdb.py" \
  "${TPCH_PARQUET}" "${STREAM}" "${OUT_CSV}" "${QUERIES}" "${RUNS}" "${WARMUPS}" 2>&1 | tee -a "${LOG}"

[ -s "${OUT_CSV}" ] || { echo "ERROR: no results produced" >&2; exit 1; }

if [ "${TPCH_MERGE:-1}" = 1 ]; then
  "${PY}" "${ROOT}/merge_results.py" duckdb_cpu "${TPCH_SF}" "${OUT_CSV}" "${ROOT}/results" \
    | tee -a "${LOG}" && rm -f "${OUT_CSV}"
fi
