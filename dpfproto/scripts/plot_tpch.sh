#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$(find "${SCRIPT_DIR}/../DPFProto/logs/tpch_run_all" -mindepth 1 -maxdepth 1 -type d | sort | tail -1)"

python3 "${SCRIPT_DIR}/plot_tpch.py" "${LOG_DIR}" --out "${LOG_DIR}/tpch_runtime.png"
echo "Wrote ${LOG_DIR}/tpch_runtime.png"
