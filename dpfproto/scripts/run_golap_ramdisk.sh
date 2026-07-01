#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DPF_ROOT="$ROOT/DPFProto"
PATCH="$ROOT/patches/0001-tmpfs-odirect-fallback.patch"
ARCH_PATCHES=(
  "$ROOT/patches/0002-q5-sm86-cuda-arch.patch"
  "$ROOT/patches/0003-global-sm86-cuda-arch.patch"
)
cd "$DPF_ROOT"

# Fixed experiment settings.
SF=1
THREADS="$(nproc)"
TRIALS=3
QUERIES="q1 q3 q5 q6 q13 q16"
DEVICE_SIZE=16G
DATA_BASE="$ROOT/data/tpch"
DEV_BASE="/dev/shm/dpfproto_golap_filedev/sf${SF}"
LOG_DIR="$ROOT/logs/golap_ramdisk/$(date +%Y%m%d_%H%M%S)"

NEED_CLEAN=0

# Apply the tmpfs/O_DIRECT fallback patch if needed.
if git apply --reverse --check "$PATCH" >/dev/null 2>&1; then
  echo "Patch already applied: $PATCH"
else
  git apply "$PATCH"
  NEED_CLEAN=1
  echo "Applied patch: $PATCH"
fi

# Build for the RTX 3090 sm86 target.
for patch in "${ARCH_PATCHES[@]}"; do
  if git apply --reverse --check "$patch" >/dev/null 2>&1; then
    echo "Patch already applied: $patch"
  else
    git apply "$patch"
    NEED_CLEAN=1
    echo "Applied patch: $patch"
  fi
done

# Rebuild the touched binaries.
export DPF_DEPS=$HOME/.local/dpfproto-deps
export CUDA_HOME=/usr/local/cuda-13.2
export CUDA_PATH=$CUDA_HOME
export PATH=$CUDA_HOME/bin:$CUDA_HOME/gds/tools:$DPF_DEPS/bin:$PATH
export PKG_CONFIG_PATH=$DPF_DEPS/lib/pkgconfig:$DPF_DEPS/lib64/pkgconfig:${PKG_CONFIG_PATH:-}
export LD_LIBRARY_PATH=$DPF_DEPS/lib:$DPF_DEPS/lib64:$CUDA_HOME/lib64:${LD_LIBRARY_PATH:-}
export LIBRARY_PATH=$DPF_DEPS/lib:$DPF_DEPS/lib64:${LIBRARY_PATH:-}
export CMAKE_PREFIX_PATH=$DPF_DEPS:${CMAKE_PREFIX_PATH:-}
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CUDA_ARCHITECTURES=86
CLEAN_MARKER="build/.sm86_clean_done"
if [[ "$NEED_CLEAN" == 1 || ! -f "$CLEAN_MARKER" ]]; then
  cmake --build build --target clean
  touch "$CLEAN_MARKER"
fi
make -C build -j"$(nproc)" tpchdb tpchloader

mkdir -p "$DEV_BASE" "$LOG_DIR"

# cuFile compat mode is needed when nvidia-fs is not loaded.
CUFILE_COMPAT_JSON="$LOG_DIR/cufile.compat.json"
sed 's/"allow_compat_mode": false/"allow_compat_mode": true/' \
  "$DPF_ROOT/config/cufile.json" > "$CUFILE_COMPAT_JSON"
export CUFILE_ENV_PATH_JSON="$CUFILE_COMPAT_JSON"

# tmpfs files stand in for NVMe device images.
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
      "$DEVICES_NVME"
done

echo "Done: $LOG_DIR"
