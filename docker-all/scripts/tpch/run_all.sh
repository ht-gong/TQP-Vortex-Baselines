#!/bin/bash
# End-to-end: generate data (if missing) -> Spark-RAPIDS GPU run -> Polars GPU
# run -> comparison table. All artifacts land under $DATA_DIR/sf$SCALE/.
set -euo pipefail
SCALE="${SCALE:-10}"
DATA_DIR="${DATA_DIR:-/data}"
S="/opt/scripts/tpch"
PARQUET="${DATA_DIR}/sf${SCALE}/parquet"

if [ ! -d "${PARQUET}" ] || [ "$(ls "${PARQUET}" 2>/dev/null | wc -l)" -lt 8 ]; then
  echo "### [1/4] generating SF${SCALE} data"
  "${S}/gen_data.sh"
else
  echo "### [1/4] data present at ${PARQUET} (skipping gen)"
fi

echo "### [2/4] Spark-RAPIDS GPU run"
"${S}/run_spark_rapids.sh"

echo "### [3/4] Polars GPU run"
"${S}/run_polars.sh"

echo "### [4/4] comparison table"
"${S}/make_table.sh"
