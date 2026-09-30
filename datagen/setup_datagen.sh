#!/bin/bash
# One-time setup for the in-repo TPC-H data generator (datagen/gen_tpch.sh) and
# for the upstream NDS-H reference pipeline it is verified against
# (rapids/nds_h_pipeline.sh). Idempotent; re-run freely.
#
# Installs, all inside the repo (every path is gitignored):
#   tpch-kit/                   pinned clone: the TPC-H dbgen sources
#   spark-rapids-benchmarks/    pinned clone: NVIDIA NDS-H scripts (reference only)
#   datagen/dbgen/              dbgen + qgen built from tpch-kit, the way the NDS-H
#                               tpch-gen Makefile builds them (SPARK query profile)
#   datagen-venv/               python venv: pyspark==3.5.8, pyarrow, duckdb; plus a
#                               Temurin JDK 17 under datagen-venv/jdk (Spark 3.5
#                               does not support the system Java 21)
#
#   datagen/setup_datagen.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HERE="${ROOT}/datagen"

# Pinned upstreams. tpch-kit master = TPC-H tools 2.17.3 (release.h); the
# spark-rapids-benchmarks ref is what the reference pipeline was verified against.
TPCH_KIT_URL="https://github.com/gregrahn/tpch-kit.git"
TPCH_KIT_REF="${TPCH_KIT_REF:-852ad0a5ee31ebefeed884cea4188781dd9613a3}"
SRB_URL="https://github.com/NVIDIA/spark-rapids-benchmarks.git"
SRB_REF="${SRB_REF:-efcfa3f5d5065f07e3834731c17ed042b2b39b21}"

PYSPARK_VERSION="${PYSPARK_VERSION:-3.5.8}"
VENV="${ROOT}/datagen-venv"
JDK="${VENV}/jdk"

log(){ echo "[setup_datagen] $*"; }

clone_pinned(){   # $1=url $2=ref $3=dir
  local url="$1" ref="$2" dir="$3"
  if [ -d "${dir}/.git" ]; then
    local have; have="$(git -C "${dir}" rev-parse HEAD)"
    if [ "${have}" != "${ref}" ]; then
      log "${dir} is at ${have:0:12}, expected ${ref:0:12} -> checking out pinned ref"
      git -C "${dir}" fetch -q origin "${ref}" && git -C "${dir}" checkout -q "${ref}"
    fi
  else
    log "cloning ${url} -> ${dir} @ ${ref:0:12}"
    git clone -q "${url}" "${dir}"
    git -C "${dir}" checkout -q "${ref}"
  fi
}

# ---------------------------------------------------------------- 1) upstreams
clone_pinned "${TPCH_KIT_URL}" "${TPCH_KIT_REF}" "${ROOT}/tpch-kit"
clone_pinned "${SRB_URL}"      "${SRB_REF}"      "${ROOT}/spark-rapids-benchmarks"

# ---------------------------------------------------------------- 2) dbgen
# Reproduce nds-h/tpch-gen/Makefile without its dos2unix/maven dependencies:
#   * apply patches/template.patch to the query templates (qgen only),
#   * add the SPARK query-profile defines to tpcd.h and the
#     "-- Template file: N" marker to qgen.c,
#   * build with MACHINE=LINUX DATABASE=SPARK WORKLOAD=TPCH.
# None of this touches dbgen's data generation; it is reproduced so `qgen` can
# regenerate results/queries/stream_qualification.sql identically if needed.
DBGEN_DIR="${HERE}/dbgen"
if [ ! -x "${DBGEN_DIR}/dbgen" ]; then
  log "building dbgen/qgen -> ${DBGEN_DIR}"
  rm -rf "${DBGEN_DIR}"; cp -r "${ROOT}/tpch-kit/dbgen" "${DBGEN_DIR}"
  (
    cd "${DBGEN_DIR}"
    cp "${ROOT}/spark-rapids-benchmarks/nds-h/tpch-gen/patches/template.patch" queries/
    sed -i 's/\r$//' queries/*.sql queries/*.patch
    (cd queries && cat *.patch | patch -p1 -s)
    sed -i '172i fprintf(ofp, "\\n-- Template file: %s\\n", qtag);' qgen.c
    # The upstream Makefile inserts this block by line number (115a) into the
    # official toolkit's tpcd.h; tpch-kit's tpcd.h is numbered differently, so
    # anchor on the line that follows the per-database blocks instead.
    sed -i '/^#define MAX_VARS/i\
#ifdef SPARK\
#define GEN_QUERY_PLAN  ""\
#define START_TRAN      ""\
#define END_TRAN        ""\
#define SET_OUTPUT      ""\
#define SET_ROWCOUNT    "LIMIT %d"\
#define SET_DBASE       ""\
#endif\
' tpcd.h
    make -s clean >/dev/null 2>&1 || true
    make -s MACHINE=LINUX DATABASE=SPARK WORKLOAD=TPCH >/dev/null 2>&1 || make MACHINE=LINUX DATABASE=SPARK WORKLOAD=TPCH
    test -x dbgen && test -x qgen
  )
fi
log "dbgen: $("${DBGEN_DIR}/dbgen" -h 2>&1 | head -1 | tr -d '\r')"

# The upstream reference scripts look for their dbgen under
# nds-h/tpch-gen/target/dbgen and (even in local mode) for a tpch-gen-*.jar
# there; mirror our build and drop a placeholder jar so nds_h_gen_data.py runs.
TARGET="${ROOT}/spark-rapids-benchmarks/nds-h/tpch-gen/target"
if [ ! -x "${TARGET}/dbgen/dbgen" ]; then
  log "mirroring dbgen into upstream ${TARGET}/dbgen (reference pipeline)"
  mkdir -p "${TARGET}"; rm -rf "${TARGET}/dbgen"; cp -r "${DBGEN_DIR}" "${TARGET}/dbgen"
fi
[ -f "${TARGET}/tpch-gen-local-placeholder.jar" ] || : > "${TARGET}/tpch-gen-local-placeholder.jar"

# ---------------------------------------------------------------- 3) venv
if [ ! -x "${VENV}/bin/python" ]; then
  log "creating venv ${VENV}"
  python3 -m venv "${VENV}"
fi
if ! "${VENV}/bin/python" -c "import pyspark,pyarrow,duckdb; assert pyspark.__version__=='${PYSPARK_VERSION}'" 2>/dev/null; then
  log "installing pyspark==${PYSPARK_VERSION} pyarrow duckdb"
  PIP_NO_CACHE_DIR=1 "${VENV}/bin/pip" install -q --disable-pip-version-check \
    "pyspark==${PYSPARK_VERSION}" pyarrow duckdb
fi

# ---------------------------------------------------------------- 4) JDK 17
if [ ! -x "${JDK}/bin/java" ]; then
  log "fetching Temurin JDK 17 -> ${JDK}"
  mkdir -p "${JDK}"
  curl -sSL "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse" \
    | tar xz -C "${JDK}" --strip-components=1
fi
log "java: $("${JDK}/bin/java" -version 2>&1 | head -1)"

log "done. Use:  source ${HERE}/env.sh"
