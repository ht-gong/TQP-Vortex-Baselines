#!/bin/bash
# Dispatcher for the TPC-H GPU benchmark image.
#
#   gen           generate SF<SCALE> parquet + the 22-query stream into $DATA_DIR
#   spark-rapids  run TPC-H q1-22 on the GPU with Spark + RAPIDS -> results CSV
#   polars        run TPC-H q1-22 on the GPU with cudf-polars     -> results CSV
#   table         merge both result CSVs into the comparison table (md + csv)
#   all           gen (if missing) -> spark-rapids -> polars -> table
#   smoke         tiny SF1 end-to-end self-test (gen + both engines + assert)
#   bash          drop into a shell
#
# Config via env: SCALE (default 10), DATA_DIR (default /data). STAGE_RAMDISK
# defaults to 1 (parquet staged into /dev/shm before running; needs --shm-size >=
# dataset, else auto-falls back to disk -- set 0 to force disk). Plus the
# per-engine knobs documented in each script.
set -euo pipefail
cmd="${1:-help}"; shift || true
S="/opt/baseline/scripts"

case "${cmd}" in
  gen)          exec "${S}/gen_data.sh" "$@" ;;
  spark-rapids) exec "${S}/run_spark_rapids.sh" "$@" ;;
  polars)       exec "${S}/run_polars.sh" "$@" ;;
  table)        exec "${S}/make_table.sh" "$@" ;;
  all)          exec "${S}/run_all.sh" "$@" ;;
  smoke)        exec "${S}/smoke.sh" "$@" ;;
  bash|sh)      exec /bin/bash "$@" ;;
  help|*)
    awk 'NR>1{ if(/^#/){sub(/^# ?/,""); print} else exit }' "$0"
    echo
    echo "Examples:"
    echo "  docker run --gpus all --shm-size=64g -v \$PWD/data:/data -e SCALE=100 tpch-gpu all"
    echo "  docker run --gpus all -v \$PWD/data:/data -e SCALE=100 tpch-gpu gen"
    echo "  docker run --gpus all -v \$PWD/data:/data tpch-gpu spark-rapids"
    ;;
esac
