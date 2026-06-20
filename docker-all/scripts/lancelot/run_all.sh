#!/bin/bash
# End-to-end: generate SSB data (if missing) -> build engine -> run on GPU.
#   run_all.sh [SF]
set -euo pipefail
SF="${1:-${SF:-1}}"
BASE_PATH="${BASE_PATH:-/data/ssb/}"; BASE_PATH="${BASE_PATH%/}/"
COL="${BASE_PATH}s${SF}_columnar"
S=/opt/scripts/lancelot

if [ ! -d "${COL}" ] || [ -z "$(ls -A "${COL}" 2>/dev/null)" ]; then
  echo "### [1/3] generate SF${SF}"; "${S}/gen_data.sh" "${SF}"
else
  echo "### [1/3] data present at ${COL} (skip gen)"
fi
echo "### [2/3] build engine (SF${SF})"; "${S}/build_engine.sh" "${SF}"
echo "### [3/3] run on GPU";            "${S}/run_gpu.sh"
