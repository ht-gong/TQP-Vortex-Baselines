#!/bin/bash
# Tiny SF1 end-to-end self-test: generate -> build -> run on GPU, and assert the
# engine produced output. Fast gate that the image + toolchain + GPU work.
#   smoke.sh
set -uo pipefail
export SF=1
DATA_DIR="${DATA_DIR:-/data}"
BASE_PATH="${BASE_PATH:-/data/ssb/}"; BASE_PATH="${BASE_PATH%/}/"
COL="${BASE_PATH}s1_columnar"
S=/opt/scripts/lancelot
echo "### lancelot smoke: SF1"

[ -d "${COL}" ] && [ -n "$(ls -A "${COL}" 2>/dev/null)" ] || "${S}/gen_data.sh" 1
"${S}/build_engine.sh" 1
"${S}/run_gpu.sh"

out="${DATA_DIR}/logs/run_gpu.out"
if [ -s "${out}" ] && [ -x /opt/lancelot/bin/gpudb/main_multi_gpu ]; then
  echo "SMOKE TEST PASSED  (engine ran; logs: ${DATA_DIR}/logs/)"
  exit 0
else
  echo "SMOKE TEST FAILED  (see ${DATA_DIR}/logs/)"
  exit 1
fi
