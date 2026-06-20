#!/bin/bash
# Host-side helper: build the image and run a benchmark command with the GPU
# wired in. Run this on a GPU host that has Docker + the NVIDIA Container
# Toolkit installed (this is NOT runnable inside an unprivileged container).
#
#   ./run.sh build                     # docker build -t tpch-gpu .
#   ./run.sh <cmd> [args]              # e.g. all | gen | spark-rapids | polars | table
#
# Env: SCALE (default 10), DATA (host dir for dataset+results, default ./data),
#      SHM   (--shm-size, default 64g; raise toward the dataset size if you set
#             STAGE_RAMDISK=1), IMAGE (default tpch-gpu).
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-tpch-gpu}"
SCALE="${SCALE:-10}"
DATA="${DATA:-$PWD/data}"
SHM="${SHM:-64g}"
cmd="${1:-help}"; shift || true

if [ "${cmd}" = "build" ]; then
  exec docker build -t "${IMAGE}" .
fi

mkdir -p "${DATA}"
exec docker run --rm -it \
  --gpus all \
  --shm-size="${SHM}" \
  -e SCALE="${SCALE}" \
  ${STAGE_RAMDISK:+-e STAGE_RAMDISK="${STAGE_RAMDISK}"} \
  ${DRIVER_MEM:+-e DRIVER_MEM="${DRIVER_MEM}"} \
  ${HOST_SPILL:+-e HOST_SPILL="${HOST_SPILL}"} \
  -v "${DATA}:/data" \
  "${IMAGE}" "${cmd}" "$@"
