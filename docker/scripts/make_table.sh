#!/bin/bash
# Merge the two GPU result CSVs into the comparison table (markdown + csv).
set -euo pipefail
SCALE="${SCALE:-10}"
DATA_DIR="${DATA_DIR:-/data}"
RESULTS="${DATA_DIR}/sf${SCALE}/results"
source /opt/venv-spark/bin/activate
python3 /opt/baseline/app/make_table.py \
  "${RESULTS}/query_times_spark_gpu.csv" \
  "${RESULTS}/query_times_polars_gpu.csv" \
  "${RESULTS}/gpu_comparison.md" \
  "${RESULTS}/gpu_comparison.csv"
echo
echo "wrote ${RESULTS}/gpu_comparison.md and .csv"
