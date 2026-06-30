#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DPF_ROOT="$ROOT/DPFProto"
cd "$DPF_ROOT"

# Fixed experiment settings.
SF=1
THREADS="$(nproc)"
TRIALS=3
QUERIES="q1 q3 q5 q6 q13 q16"
DEVICE_SIZE=16G
DATA_BASE="$ROOT/data/tpch"
DEV_BASE="$ROOT/data/golap_filedev/sf${SF}"
LOG_DIR="$ROOT/logs/golap_filedev/$(date +%Y%m%d_%H%M%S)"

mkdir -p "$DEV_BASE" "$LOG_DIR"

# cuFile compat mode is needed when nvidia-fs is not loaded.
CUFILE_COMPAT_JSON="$LOG_DIR/cufile.compat.json"
sed 's/"allow_compat_mode": false/"allow_compat_mode": true/' \
  "$DPF_ROOT/config/cufile.json" > "$CUFILE_COMPAT_JSON"
export CUFILE_ENV_PATH_JSON="$CUFILE_COMPAT_JSON"

# Regular files stand in for NVMe device images.
for i in 0 1 2 3; do
  truncate -s "$DEVICE_SIZE" "$DEV_BASE/dev${i}.img"
done
DEVICES_NVME="$DEV_BASE/dev0.img,$DEV_BASE/dev1.img,$DEV_BASE/dev2.img,$DEV_BASE/dev3.img"

echo "DATA_BASE=$DATA_BASE"
echo "DEVICES_NVME=$DEVICES_NVME"
echo "LOG_DIR=$LOG_DIR"

# Load GOLAP-style compressed pages.
./build/tpchloader \
  -i "$DATA_BASE/sideways/sf${SF}" \
  -d "$DEVICES_NVME" \
  -x gidp \
  -A \
  -c | tee "$LOG_DIR/load.log"

# Run the partial TPC-H suite.
for q in $QUERIES; do
  scripts/bench.sh -n "$TRIALS" -w -t 60 \
    -o "$LOG_DIR/${q}.txt" \
    -- ./build/tpchdb \
      -q "$q" \
      -x gidp \
      -w "$THREADS" \
      -Z \
      "$DEVICES_NVME"
done

echo "Done: $LOG_DIR"
