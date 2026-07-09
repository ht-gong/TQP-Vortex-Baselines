#!/bin/bash
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SF="${1:-100}"
OUT="${2:-${ROOT}/results/parquet}"

# convert dpfproto tbl data
python3 "${ROOT}/duckdb/make_parquet.py" "${SF}" "${OUT}"
