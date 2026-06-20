#!/bin/bash
# Host helper for the multi-tool image. Run on a host with Docker + the NVIDIA
# Container Toolkit.
#
#   ./host.sh build                      # docker build -t tpch-multitool .
#   ./host.sh <cmd> [args]               # any entrypoint command, e.g.:
#       tpch-all | tpch-spark | tpch-polars | tpch-smoke
#       ssb-all  | ssb-build | ssb-run-gpu | ssb-smoke | smoke
#
# Env: SCALE (TPC-H scale, default 10), SF (SSB scale 1|10|20|40, default 1),
#      NUM_GPU (2), SM_ARCH (120), DATA (host data dir, default ./data),
#      SHM (--shm-size, default 64g), IMAGE (tpch-multitool).
set -euo pipefail
cd "$(dirname "$0")"
IMAGE="${IMAGE:-tpch-multitool}"
SCALE="${SCALE:-10}"; SF="${SF:-1}"; NUM_GPU="${NUM_GPU:-2}"; SM_ARCH="${SM_ARCH:-120}"
DATA="${DATA:-$PWD/data}"; SHM="${SHM:-64g}"
cmd="${1:-help}"; shift || true

if [ "${cmd}" = "build" ]; then
  exec docker build -t "${IMAGE}" .
fi
mkdir -p "${DATA}"
exec docker run --rm -it --gpus all --shm-size="${SHM}" \
  -e SCALE="${SCALE}" -e SF="${SF}" -e NUM_GPU="${NUM_GPU}" -e SM_ARCH="${SM_ARCH}" \
  ${SM_DEF:+-e SM_DEF="${SM_DEF}"} \
  ${STAGE_RAMDISK:+-e STAGE_RAMDISK="${STAGE_RAMDISK}"} \
  ${DRIVER_MEM:+-e DRIVER_MEM="${DRIVER_MEM}"} \
  ${HOST_SPILL:+-e HOST_SPILL="${HOST_SPILL}"} \
  -v "${DATA}:/data" \
  "${IMAGE}" "${cmd}" "$@"
