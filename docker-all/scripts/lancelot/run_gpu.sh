#!/bin/bash
# Run the Lancelot multi-GPU SSB engine. Logs land under $DATA_DIR/logs (the
# repo's logs/ dir is symlinked there so results persist on the mounted volume).
#   run_gpu.sh [args...]   (extra args are passed through to the engine)
set -uo pipefail
ROOT="${LANCELOT_ROOT:-/opt/lancelot}"
DATA_DIR="${DATA_DIR:-/data}"
BIN="${ROOT}/bin/gpudb/main_multi_gpu"
cd "${ROOT}"
log(){ echo "[lancelot:run-gpu] $*"; }

if [ ! -x "${BIN}" ]; then
  log "engine not built -- run 'build' first."; exit 1
fi

# persist logs on the volume
mkdir -p "${DATA_DIR}/logs/runs" "${DATA_DIR}/logs/stats" "${DATA_DIR}/logs/traffic"
rm -rf "${ROOT}/logs"; ln -s "${DATA_DIR}/logs" "${ROOT}/logs"

log "GPUs visible:"; nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader 2>/dev/null || true
log "running ${BIN} $*"
"${BIN}" "$@" 2>&1 | tee "${DATA_DIR}/logs/run_gpu.out"
log "DONE. logs -> ${DATA_DIR}/logs/"
