#!/bin/bash
# Set up the Sirius GPU SQL engine baseline: install pixi, clone the repo with
# submodules, and build the Sirius-enabled duckdb binary. Idempotent -- safe to
# re-run; skips steps already done.
#
# Sirius (github.com/sirius-db/sirius) is a GPU-native SQL engine that loads as a
# DuckDB extension and transparently runs supported SQL on the GPU (cuDF/RMM/
# cuCascade) with out-of-core tiered spilling. Requirements it needs and this box
# satisfies: NVIDIA compute capability >= 7.5 (RTX 5090 = 12.0), CUDA 13.x with
# driver >= 580.65.06 (580.82.09 here), glibc >= 2.28 (2.39), io_uring enabled,
# and O_DIRECT-capable storage for the parquet (both / and /dev/shm qualify).
#
#   setup_sirius.sh
set -euo pipefail

DIR="/workspace/baseline/sirius"
REPO="${DIR}/sirius"
# Build only for this box's GPU arch (Blackwell sm_120) to cut CUDA codegen time;
# the conda libcudf is already multi-arch. Override with CUDAARCHS=... to widen.
ARCH="${CUDAARCHS:-120a-real;120}"
JOBS="${CMAKE_BUILD_PARALLEL_LEVEL:-64}"

echo "=== 1) install pixi (if missing) ==="
if ! command -v pixi >/dev/null 2>&1 && [ ! -x "${HOME}/.pixi/bin/pixi" ]; then
  curl -fsSL https://pixi.sh/install.sh | bash
fi
export PATH="${HOME}/.pixi/bin:${PATH}"
pixi --version

echo "=== 2) clone sirius (with submodules) ==="
mkdir -p "${DIR}"
if [ ! -d "${REPO}/.git" ]; then
  git clone --recurse-submodules https://github.com/sirius-db/sirius.git "${REPO}"
fi
cd "${REPO}"
# The experimental starrocks integration is not needed to build Sirius and is
# large; drop it to save disk if it got pulled by --recurse-submodules.
git submodule deinit -f experimental/starrocks/starrocks experimental/starrocks/brpc 2>/dev/null || true
rm -rf experimental/starrocks/starrocks experimental/starrocks/brpc .git/modules/experimental 2>/dev/null || true
echo "sirius @ $(git rev-parse --short HEAD)"

echo "=== 3) build (default pixi env = dev-libs + cuda13, arch=${ARCH}, -j${JOBS}) ==="
# `pixi run make` runs the repo Makefile inside the activated conda env (nvcc,
# cmake, clang, libcudf 26.06, rmm, ...). Produces build/release/duckdb with the
# sirius extension statically linked and auto-loading.
pixi run bash -c "export CUDAARCHS='${ARCH}'; CMAKE_BUILD_PARALLEL_LEVEL=${JOBS} make"

BIN="${REPO}/build/release/duckdb"
if [ -x "${BIN}" ]; then
  echo "=== done: ${BIN} ==="
  "${BIN}" -c "select 'sirius duckdb ok' as status;" || true
else
  echo "ERROR: build did not produce ${BIN}" >&2; exit 1
fi
