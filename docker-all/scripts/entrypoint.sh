#!/bin/bash
# Unified dispatcher for the multi-tool benchmark image (TPC-H: Spark-RAPIDS +
# Polars; SSB: Lancelot). All three engines packaged in one image; run one at a
# time (each saturates the GPU). Results persist under $DATA_DIR (/data).
#
# TPC-H (Spark-RAPIDS + cudf-polars), scale via $SCALE:
#   tpch-gen        generate SF<SCALE> parquet + 22-query stream
#   tpch-spark      run TPC-H q1-22 on GPU with Spark+RAPIDS  -> results CSV
#   tpch-polars     run TPC-H q1-22 on GPU with cudf-polars   -> results CSV
#   tpch-table      merge both into the GPU comparison table
#   tpch-all        gen -> spark -> polars -> table
#   tpch-smoke      tiny SF1 TPC-H self-test
#
# SSB (Lancelot hybrid CPU/multi-GPU DBMS), scale via $SF (1|10|20|40):
#   ssb-gen         generate SSB columnar data
#   ssb-build       compile engine + minmax (SF/NUM_GPU/SM_ARCH), run minmax
#   ssb-run-gpu     run the multi-GPU SSB engine
#   ssb-run-cpu     build + run the CPU SSB query binaries
#   ssb-all         gen -> build -> run-gpu
#   ssb-smoke       tiny SF1 SSB self-test
#
#   smoke           run BOTH smokes (TPC-H SF1 + SSB SF1)
#   bash            drop into a shell
set -euo pipefail
cmd="${1:-help}"; shift || true
T=/opt/scripts/tpch
L=/opt/scripts/lancelot
case "${cmd}" in
  tpch-gen)     exec "${T}/gen_data.sh" "$@" ;;
  tpch-spark)   exec "${T}/run_spark_rapids.sh" "$@" ;;
  tpch-polars)  exec "${T}/run_polars.sh" "$@" ;;
  tpch-table)   exec "${T}/make_table.sh" "$@" ;;
  tpch-all)     exec "${T}/run_all.sh" "$@" ;;
  tpch-smoke)   exec "${T}/smoke.sh" "$@" ;;
  ssb-gen)      exec "${L}/gen_data.sh" "$@" ;;
  ssb-build)    exec "${L}/build_engine.sh" "$@" ;;
  ssb-run-gpu)  exec "${L}/run_gpu.sh" "$@" ;;
  ssb-run-cpu)  exec "${L}/run_cpu.sh" "$@" ;;
  ssb-all)      exec "${L}/run_all.sh" "$@" ;;
  ssb-smoke)    exec "${L}/smoke.sh" "$@" ;;
  smoke)        "${T}/smoke.sh" && "${L}/smoke.sh" ;;
  bash|sh)      exec /bin/bash "$@" ;;
  help|*)
    awk 'NR>1{ if(/^#/){sub(/^# ?/,""); print} else exit }' "$0"
    echo
    echo "Example:  docker run --gpus all --shm-size=64g -v \$PWD/data:/data \\"
    echo "            -e SCALE=100 tpch-multitool tpch-all"
    ;;
esac
