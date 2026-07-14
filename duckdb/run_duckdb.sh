#!/bin/bash
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# paper query set
QUERIES="${1:-1 3 5 6 13 16}"
TPCH_SF="${TPCH_SF:-500}"
TPCH_PARQUET="${TPCH_PARQUET:-}"
STREAM="${STREAM:-${ROOT}/results/queries/stream_qualification.sql}"
OUT_CSV="${OUT_CSV:-${ROOT}/duckdb/results/duckdb_runs.csv}"
RUNS="${RUNS:-5}"
WARMUPS="${WARMUPS:-1}"

# find local parquet
[ -n "${TPCH_PARQUET}" ] || for d in \
  "${ROOT}/duckdb/results/parquet" \
  "${ROOT}/results/parquet" \
  "/dev/shm/tpch_sf${TPCH_SF}/parquet" \
  "/workspace/baseline/results/parquet"; do
  [ -d "${d}" ] && TPCH_PARQUET="${d}" && break
done

[ -d "${TPCH_PARQUET}" ] || {
  echo "ERROR: parquet dir not found. Set TPCH_PARQUET=/path/to/parquet" >&2
  exit 1
}

# run benchmark
mkdir -p "$(dirname "${OUT_CSV}")"
python3 "${ROOT}/duckdb/run_tpch_duckdb.py" \
  "${TPCH_PARQUET}" "${STREAM}" "${OUT_CSV}" "${QUERIES}" "${RUNS}" "${WARMUPS}"


# TPCH_SF=100 TPCH_PARQUET=/path/to/parquet ./duckdb/run_duckdb.sh
