#!/bin/bash
# One-time host setup for the in-repo TPC-H data generator (datagen/gen_tpch.sh).
# Idempotent; re-run freely. (docker/Dockerfile does the same steps, pinned by
# versions.env and docker/py.lock.)
#
# Installs, all inside the repo (every path is gitignored):
#   tpch-kit/                   pinned clone: the TPC-H dbgen sources (2.17.3)
#   datagen/dbgen/              dbgen built from tpch-kit with the NDS-H tpch-gen
#                               flags (MACHINE=LINUX DATABASE=SPARK WORKLOAD=TPCH)
#   datagen-venv/               python venv: pyspark==3.5.8, pyarrow, duckdb; plus a
#                               Temurin JDK 17 under datagen-venv/jdk (Spark 3.5
#                               does not support the system Java 21)
#
#   datagen/setup_datagen.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HERE="${ROOT}/datagen"

# Pinned upstream: tpch-kit master = TPC-H tools 2.17.3 (release.h).
TPCH_KIT_URL="https://github.com/gregrahn/tpch-kit.git"
TPCH_KIT_REF="${TPCH_KIT_REF:-852ad0a5ee31ebefeed884cea4188781dd9613a3}"

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

# ---------------------------------------------------------------- 2) dbgen
# Build dbgen the way the NDS-H tpch-gen Makefile does: MACHINE=LINUX
# DATABASE=SPARK WORKLOAD=TPCH, with the SPARK profile defines added to tpcd.h
# (the upstream Makefile inserts them by line number into the official toolkit's
# tpcd.h; tpch-kit's is numbered differently, so anchor on the line after the
# per-database blocks). The defines only shape qgen output; the data is
# unaffected, but keeping them keeps the build identical to the one the
# results were generated with.
DBGEN_DIR="${HERE}/dbgen"
if [ ! -x "${DBGEN_DIR}/dbgen" ]; then
  log "building dbgen -> ${DBGEN_DIR}"
  rm -rf "${DBGEN_DIR}"; cp -r "${ROOT}/tpch-kit/dbgen" "${DBGEN_DIR}"
  (
    cd "${DBGEN_DIR}"
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
    make -s MACHINE=LINUX DATABASE=SPARK WORKLOAD=TPCH dbgen
    test -x dbgen
  )
fi
log "dbgen: $("${DBGEN_DIR}/dbgen" -h 2>&1 | head -1 | tr -d '\r')"

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
