#!/bin/bash
# Tiny end-to-end smoke test: generate SF1, run a few queries on BOTH GPU engines,
# build the comparison table, and assert every query produced an OK row with
# matching row counts. Fast (SF1 ≈ 0.36 GB) -- proves the whole pipeline (gen +
# Spark-RAPIDS + cudf-polars + table) works on this host/GPU before a big run.
#
#   smoke.sh                 # default queries "1 3 6" at SF1
#   SMOKE_QUERIES="1 6" smoke.sh
set -uo pipefail

export SCALE=1
DATA_DIR="${DATA_DIR:-/data}"
QSET="${SMOKE_QUERIES:-1 3 6}"
S="/opt/scripts/tpch"
PARQUET="${DATA_DIR}/sf1/parquet"
RESULTS="${DATA_DIR}/sf1/results"
SPARK_CSV="${RESULTS}/query_times_spark_gpu.csv"
POLARS_CSV="${RESULTS}/query_times_polars_gpu.csv"

echo "### smoke: SF1, queries [${QSET}]"

# 1) generate SF1 if not already present
if [ ! -d "${PARQUET}" ] || [ "$(ls "${PARQUET}" 2>/dev/null | wc -l)" -lt 8 ]; then
  echo "### [1/4] generating SF1"
  "${S}/gen_data.sh" 1 1 1
else
  echo "### [1/4] SF1 data present (skipping gen)"
fi

echo "### [2/4] Spark-RAPIDS GPU (${QSET})"
"${S}/run_spark_rapids.sh" "${QSET}"
echo "### [3/4] Polars GPU (${QSET})"
"${S}/run_polars.sh" "${QSET}"
echo "### [4/4] table"
"${S}/make_table.sh"

# validate: every requested query is OK in both engines
status_of(){ awk -F, -v q="query$1" '$1==q{print $2; exit}' "$2"; }
fail=0
for q in ${QSET}; do
  s=$(status_of "${q}" "${SPARK_CSV}"); p=$(status_of "${q}" "${POLARS_CSV}")
  printf "  q%-3s spark=%-12s polars=%-12s\n" "${q}" "${s:-MISSING}" "${p:-MISSING}"
  [ "${s}" = "OK" ] && [ "${p}" = "OK" ] || fail=1
done

if [ "${fail}" = 0 ]; then
  echo "SMOKE TEST PASSED  (table: ${RESULTS}/gpu_comparison.md)"
  exit 0
else
  echo "SMOKE TEST FAILED  (see ${RESULTS}/*.log)"
  exit 1
fi
