#!/bin/bash
# Ensure the dataset of one scale factor (`make data`, run.sh): generate
# $DATA_DIR/sf<SF> with gen_tpch.sh if it has no parquet/ yet, then validate it.
#
#   datagen/dataset.sh <SF>
#
# Env: DATA_DIR, PY; GEN_PARALLEL (dbgen chunks, default 2*SF but at least 20),
# GEN_BATCH (chunks per batch, default 25), DRIVER_MEM.
set -euo pipefail
SF="${1:?scale factor}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"
OUT="${DATA_DIR:?DATA_DIR is not set}/sf${SF}"

if [ ! -d "${OUT}/parquet" ]; then
  par="${GEN_PARALLEL:-$(( SF * 2 < 20 ? 20 : SF * 2 ))}"
  batch="${GEN_BATCH:-25}"
  echo "[$(date +%T)] generating SF${SF} into ${OUT} (PARALLEL=${par} BATCH=${batch}; log ${OUT}/pipeline.log)"
  "${HERE}/gen_tpch.sh" "${SF}" "${par}" "${batch}" "${OUT}"
fi
"${PY:?PY is not set}" "${ROOT}/results/validate_dataset.py" "${OUT}/parquet" "${SF}"
