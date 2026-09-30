#!/bin/bash
# Source this to get a ready-to-use spark-rapids shell:
#   source rapids/activate.sh
# It activates the Spark env (the datagen toolchain: pyspark 3.5.8 + JDK 17),
# sets JAVA_HOME / SPARK_HOME, and exports $RAPIDS_JAR and $RAPIDS_CONF plus a
# `rapids-submit` helper and `rapids_run_args` (the flags of the benchmark runs).

RAPIDS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${RAPIDS_DIR}/../datagen/env.sh"
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

# spark-submit flags of every benchmark run (TPC-H runner and format probes):
# local mode on one GPU, host spill, RAPIDS shuffle. $1 = spark.local.dir.
# Sets the RAPIDS_RUN_ARGS array.
rapids_run_args() {
  RAPIDS_RUN_ARGS=(
    --master "local[*]" --driver-memory "${DRIVER_MEM:-96g}"
    --jars "${RAPIDS_JAR}"
    --conf spark.local.dir="$1"
    --conf spark.plugins=com.nvidia.spark.SQLPlugin
    --conf spark.rapids.sql.enabled=true
    --conf spark.rapids.sql.concurrentGpuTasks=2
    --conf spark.rapids.memory.pinnedPool.size=8G
    --conf spark.rapids.memory.host.spillStorageSize=200G
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark358.RapidsShuffleManager
    --conf spark.rapids.shuffle.mode=MULTITHREADED
    --conf spark.sql.files.maxPartitionBytes=1g
    --conf spark.sql.shuffle.partitions=1024
    --conf spark.sql.adaptive.enabled=true
  )
}

echo "spark-rapids env ready:"
echo "  JAVA_HOME = ${JAVA_HOME}"
echo "  SPARK_HOME= ${SPARK_HOME}"
echo "  RAPIDS_JAR= ${RAPIDS_JAR}"
echo "  helper    : rapids-submit <script.py>   (local[*] + plugin + conf)"
