#!/bin/bash
# Source this to get the datagen toolchain on PATH: the datagen-venv python
# (pyspark 3.5.8, pyarrow, duckdb), its spark-submit, and the bundled JDK 17.
#   source datagen/env.sh
_DG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export JAVA_HOME="${_DG_ROOT}/datagen-venv/jdk"
export DATAGEN_VENV="${_DG_ROOT}/datagen-venv"
export DBGEN_DIR="${DBGEN_DIR:-${_DG_ROOT}/datagen/dbgen}"
# shellcheck disable=SC1091
source "${DATAGEN_VENV}/bin/activate"
export SPARK_HOME="$(python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')"
export PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"
# Spark mistakes a Vast-style CONTAINER_ID for a YARN container and dies with
# "Yarn Local dirs can't be empty"; the runners already `env -u CONTAINER_ID`.
unset CONTAINER_ID
# Local-mode Spark only; in containers whose hostname does not resolve to a
# bindable address the driver fails with "Cannot assign requested address".
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
