# syntax=docker/dockerfile:1
##############################################################################
# TPC-H GPU benchmark image: Spark + RAPIDS Accelerator, GPU Polars (cudf-
# polars), and the NDS-H (TPC-H) data + query generator -- all in one image.
#
# Reproduces the versions validated on RTX 5090 (Blackwell sm_120):
#   Spark 3.5.8 | rapids-4-spark 26.04.2 (cuda12) | cudf-polars-cu12 26.6
#   OpenJDK 17  | tpch-kit + NVIDIA spark-rapids-benchmarks (NDS-H)
#
# Build:  docker build -t tpch-gpu .
# Run:    needs the NVIDIA Container Toolkit on the host (`--gpus all`).
#         See README.md / run.sh.
##############################################################################

# ----------------------------------------------------------------------------
# Stage 1 -- builder: compile tpch-kit dbgen/qgen with the SPARK profile via
# the NVIDIA NDS-H tpch-gen Makefile (adds LIMIT + "-- Template file:" markers).
# ----------------------------------------------------------------------------
FROM ubuntu:22.04 AS gen-builder

ARG TPCH_KIT_REF=master
ARG BENCH_REF=main
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential gcc make git dos2unix ca-certificates \
        openjdk-17-jdk-headless maven \
    && rm -rf /var/lib/apt/lists/*

# tpch-kit provides the canonical dbgen/qgen sources (TPCH_HOME).
RUN git clone --depth 1 -b ${TPCH_KIT_REF} https://github.com/gregrahn/tpch-kit.git /opt/tpch-kit
# NVIDIA spark-rapids-benchmarks ships the NDS-H wrappers + the tpch-gen build
# that patches dbgen/qgen for Spark-compatible SQL.
RUN git clone --depth 1 -b ${BENCH_REF} https://github.com/NVIDIA/spark-rapids-benchmarks.git /opt/spark-rapids-benchmarks

# Build dbgen + qgen (SPARK profile) and the helper jar.
ENV TPCH_HOME=/opt/tpch-kit
RUN cd /opt/spark-rapids-benchmarks/nds-h/tpch-gen && make \
    && test -x target/dbgen/dbgen && test -x target/dbgen/qgen \
    && echo "dbgen/qgen built OK"

# ----------------------------------------------------------------------------
# Stage 2 -- runtime: CUDA runtime base (driver injected at run time by the
# NVIDIA Container Toolkit). Both engines bring their own CUDA libs (the rapids
# jar bundles cuDF; cudf-polars wheels bundle libcudf), so only libcuda is
# needed from the host.
# ----------------------------------------------------------------------------
FROM nvidia/cuda:12.9.1-runtime-ubuntu22.04 AS runtime

ARG SPARK_VERSION=3.5.8
ARG RAPIDS_VERSION=26.04.2
ARG CUDF_POLARS_VERSION=26.6.0
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless python3 python3-venv python3-pip \
        curl ca-certificates procps libgomp1 tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# --- Apache Spark --------------------------------------------------------------
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      -o /tmp/spark.tgz \
    && mkdir -p /opt/spark \
    && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
    && rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}

# --- RAPIDS Accelerator for Spark jar -----------------------------------------
RUN mkdir -p /opt/rapids \
    && curl -fsSL "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/${RAPIDS_VERSION}/rapids-4-spark_2.12-${RAPIDS_VERSION}-cuda12.jar" \
       -o /opt/rapids/rapids-4-spark.jar \
    && test -s /opt/rapids/rapids-4-spark.jar
ENV RAPIDS_JAR=/opt/rapids/rapids-4-spark.jar

# --- Python venvs -------------------------------------------------------------
# venv-spark: pyspark (Spark-RAPIDS driver + NDS-H transcode).
RUN python3 -m venv /opt/venv-spark \
    && /opt/venv-spark/bin/pip install --no-cache-dir \
        "pyspark==${SPARK_VERSION}" pyarrow pandas numpy
# venv-polars: GPU Polars via cudf-polars (pulls its own polars + libcudf).
RUN python3 -m venv /opt/venv-polars \
    && /opt/venv-polars/bin/pip install --no-cache-dir \
        --extra-index-url https://pypi.nvidia.com \
        "cudf-polars-cu12==${CUDF_POLARS_VERSION}" pyarrow

# --- Generator artifacts (dbgen/qgen + NDS-H wrappers) ------------------------
COPY --from=gen-builder /opt/spark-rapids-benchmarks /opt/spark-rapids-benchmarks
ENV NDSH_DIR=/opt/spark-rapids-benchmarks/nds-h

# --- App + orchestration scripts ----------------------------------------------
COPY app/    /opt/baseline/app/
COPY scripts/ /opt/baseline/scripts/
RUN chmod +x /opt/baseline/scripts/*.sh

# Defaults; override at run time. /data is the mount point for the dataset +
# results so they persist outside the container.
ENV DATA_DIR=/data \
    SCALE=10 \
    PYTHONUNBUFFERED=1
WORKDIR /opt/baseline
VOLUME ["/data"]

ENTRYPOINT ["/opt/baseline/scripts/entrypoint.sh"]
CMD ["help"]
