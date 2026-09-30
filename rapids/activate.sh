#!/bin/bash
# Source this to get a ready-to-use spark-rapids shell:
#   source rapids/activate.sh
# It activates the Spark env, sets JAVA_HOME / SPARK_HOME, and exports
# $RAPIDS_JAR and $RAPIDS_CONF plus a `rapids-submit` helper.
#
# Two interchangeable envs provide pyspark 3.5.8 + JDK 17: the original conda
# env at rapids/env (see README), or the repo's datagen-venv (bundled Temurin 17;
# created by datagen/setup_datagen.sh). Whichever exists is used.

RAPIDS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -d "${RAPIDS_DIR}/env" ]; then
  # Self-contained conda env (Python 3.11 + OpenJDK 17 + pyspark 3.5.8)
  source /opt/miniforge3/etc/profile.d/conda.sh
  conda activate "${RAPIDS_DIR}/env"
  export JAVA_HOME="${CONDA_PREFIX}"
else
  source "${RAPIDS_DIR}/../datagen/env.sh"
fi
export SPARK_HOME="$(python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')"
export RAPIDS_JAR="${RAPIDS_DIR}/jars/rapids-4-spark_2.12-26.04.2-cuda12.jar"
export RAPIDS_CONF="${RAPIDS_DIR}/conf/spark-rapids.conf"
export PATH="${SPARK_HOME}/bin:${PATH}"

# Convenience: spark-submit with the GPU plugin already wired in.
# `env -u CONTAINER_ID` stops Spark from mistaking this Vast container for a
# YARN container (which would fail with "Yarn Local dirs can't be empty").
rapids-submit() {
  env -u CONTAINER_ID \
  spark-submit \
    --master "local[*]" \
    --jars "${RAPIDS_JAR}" \
    --properties-file "${RAPIDS_CONF}" \
    "$@"
}
export -f rapids-submit

echo "spark-rapids env ready:"
echo "  JAVA_HOME = ${JAVA_HOME}"
echo "  SPARK_HOME= ${SPARK_HOME}"
echo "  RAPIDS_JAR= ${RAPIDS_JAR}"
echo "  helper    : rapids-submit <script.py>   (local[*] + plugin + conf)"
