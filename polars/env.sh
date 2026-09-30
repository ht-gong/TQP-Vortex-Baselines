#!/bin/bash
# Source this for the polars_gpu engine environment, shared by the TPC-H runner
# (run_polars_gpu.sh) and the format probes (ablation/): the cudf-polars venv
# and the GPU memory / IO settings the benchmark runs use.
_PL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${_PL_DIR}/.venv-gpu/bin/activate"
export GPU_MR="${GPU_MR:-async}"
# kvikio POSIX/compat mode: cuFile/GDS is unavailable on these boxes; without it
# the parquet reader aborts in CUFileThreadPoolWorker.
export KVIKIO_COMPAT_MODE="${KVIKIO_COMPAT_MODE:-ON}"
export GPU_PART_MB="${GPU_PART_MB:-128}"
export RAPIDSMPF_SPILL_DEVICE_LIMIT="${RAPIDSMPF_SPILL_DEVICE_LIMIT:-$((22*1024*1024*1024))}"
