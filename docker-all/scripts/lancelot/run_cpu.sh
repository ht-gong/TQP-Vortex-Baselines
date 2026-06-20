#!/bin/bash
# Build + run the CPU SSB query binaries (q11..q43), logging timings under
# $DATA_DIR/logs/cpu. Compiled for the chosen arch (CPU kernels are still built
# with nvcc in this codebase).
#   run_cpu.sh [SF]
set -uo pipefail
SF="${1:-${SF:-1}}"
SM_ARCH="${SM_ARCH:-120}"
SM_DEF="${SM_DEF:--DSM700}"
DATA_DIR="${DATA_DIR:-/data}"
ROOT="${LANCELOT_ROOT:-/opt/lancelot}"
cd "${ROOT}"
log(){ echo "[lancelot:run-cpu] $*"; }

SMT="-gencode=arch=compute_${SM_ARCH},code=sm_${SM_ARCH}"
MK=(make -B NVCC_VER=11.5 SM_TARGETS="${SMT}" SM_DEF="${SM_DEF}")
"${MK[@]}" setup
QUERIES="q11 q12 q13 q21 q22 q23 q31 q32 q33 q34 q41 q42 q43"
OUT="${DATA_DIR}/logs/cpu/sf${SF}"; mkdir -p "${OUT}"

for q in ${QUERIES}; do
  log "build+run cpu ${q}"
  "${MK[@]}" "bin/cpu/ssb/${q}" >/dev/null 2>>"${OUT}/build.log" || { log "  build failed: ${q}"; continue; }
  "bin/cpu/ssb/${q}" --t=3 > "${OUT}/${q}cpu" 2>&1 && log "  ${q} -> ${OUT}/${q}cpu"
done
log "DONE. CPU logs -> ${OUT}/"
