#!/bin/bash
# Patch the compile-time config (SF, BASE_PATH, NUM_GPU), then compile the
# multi-GPU engine + the minmax encoder for the chosen GPU arch, and run the
# minmax pass over the columnar data (required before the engine runs).
#   build_engine.sh [SF]
set -euo pipefail
SF="${1:-${SF:-1}}"
NUM_GPU="${NUM_GPU:-2}"
SM_ARCH="${SM_ARCH:-120}"
SM_DEF="${SM_DEF:--DSM700}"
BASE_PATH="${BASE_PATH:-/data/ssb/}"; BASE_PATH="${BASE_PATH%/}/"
COL="${BASE_PATH}s${SF}_columnar"
ROOT="${LANCELOT_ROOT:-/opt/lancelot}"
cd "${ROOT}"
log(){ echo "[lancelot:build] $*"; }

# --- patch compile-time constants --------------------------------------------
log "config: SF=${SF} NUM_GPU=${NUM_GPU} BASE_PATH=${BASE_PATH} arch=sm_${SM_ARCH}"
sed -i -E "s|^#define SF .*|#define SF ${SF}|" src/ssb/ssb_utils.h
sed -i -E "s|^#define BASE_PATH .*|#define BASE_PATH \"${BASE_PATH}\"|" src/ssb/ssb_utils.h
sed -i -E "s|^#define NUM_GPU .*|#define NUM_GPU ${NUM_GPU}|" src/gpudb/common.h
# Modern bundled CUB/CCCL (CUDA 12.x/13) requires C++17; upstream Makefile pins
# c++14 (it targeted CUDA 11.5 + old CUB 1.8). Bump it in place.
sed -i 's/--std=c++14/--std=c++17/g; s/-std=c++14/-std=c++17/g' Makefile

SMT="-gencode=arch=compute_${SM_ARCH},code=sm_${SM_ARCH}"
MK=(make NVCC_VER=11.5 SM_TARGETS="${SMT}" SM_DEF="${SM_DEF}" \
       GENCODE_FLAGS="-gencode arch=compute_${SM_ARCH},code=sm_${SM_ARCH}")

# --- compile -----------------------------------------------------------------
"${MK[@]}" setup
log "compiling minmax encoder"
# The upstream minmax rule compiles .cpp host-side; newer CUDA's CCCL headers
# (pulled via common.h) need the device pass, so fall back to `-x cu`.
if ! "${MK[@]}" minmax 2>/tmp/minmax.err; then
  log "  minmax host build failed -> retrying as CUDA (-x cu)"
  nvcc -x cu "${SMT}" --std=c++17 "${SM_DEF}" -Iincludes -I. \
       src/gpudb/minmax.cpp -lcurand -ltbb -o bin/gpudb/minmax
  nvcc -x cu "${SMT}" --std=c++17 "${SM_DEF}" -Iincludes -I. \
       src/gpudb/minmaxsort.cpp -lcurand -ltbb -o bin/gpudb/minmaxsort
fi
log "compiling multi-GPU engine (this can take several minutes)"
"${MK[@]}" bin/gpudb/main_multi_gpu

# --- minmax pass over the data (needs columnar data present) -----------------
if [ -d "${COL}" ] && [ -n "$(ls -A "${COL}" 2>/dev/null)" ]; then
  log "running minmax over ${COL}"
  bash minmax.sh
else
  log "WARNING: no columnar data at ${COL} -- run 'gen' first, then re-run 'build' (skipping minmax)"
fi
log "DONE. engine: ${ROOT}/bin/gpudb/main_multi_gpu"
