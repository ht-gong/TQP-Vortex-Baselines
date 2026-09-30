#!/bin/bash
# Start one column-probe worker (ablation/probe_<engine>.py) in the same engine
# environment its TPC-H runner uses. Called by ablation/format_profile.py.
#
#   probe.sh <engine> <parquet_dir> <columns_file> <out_csv>
#
# Env: PROBE_SCRATCH (engine scratch dir; must not be on the nearly full /),
#      PROBE_LOG_DIR (Sirius log dir), CUDA_VISIBLE_DEVICES.
set -euo pipefail
ENGINE="${1:?engine}"; shift
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"
SCRATCH="${PROBE_SCRATCH:?PROBE_SCRATCH}"
mkdir -p "${SCRATCH}"

case "${ENGINE}" in
  rapids)
    # shellcheck disable=SC1091
    source "${ROOT}/rapids/activate.sh" >/dev/null
    rapids_run_args "${SCRATCH}"
    exec env -u CONTAINER_ID spark-submit "${RAPIDS_RUN_ARGS[@]}" "${HERE}/probe_rapids.py" "$@" ;;
  polars_gpu)
    # shellcheck disable=SC1091
    source "${ROOT}/polars/env.sh"
    export POLARS_TEMP_DIR="${SCRATCH}"
    exec python "${HERE}/probe_polars.py" "$@" ;;
  duckdb_cpu)
    PY="${PYTHON:-${ROOT}/datagen-venv/bin/python}"
    exec "${PY}" "${HERE}/probe_duckdb.py" "$@" ;;
  sirius)
    # shellcheck disable=SC1091
    source "${ROOT}/sirius/env.sh"
    export SIRIUS_LOG_DIR="${PROBE_LOG_DIR:-${SCRATCH}}"
    rm -rf "${SIRIUS_SPILL:?}"/* 2>/dev/null || true
    cd "${ROOT}/sirius/sirius"
    "${PIXI}" run --manifest-path "${ROOT}/sirius/sirius/pixi.toml" \
      python "${HERE}/probe_sirius.py" "$@" ;;
  *) echo "probe.sh: unknown engine '${ENGINE}'" >&2; exit 2 ;;
esac
