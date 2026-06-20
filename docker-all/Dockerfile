# syntax=docker/dockerfile:1
##############################################################################
# Multi-tool GPU benchmark image: TPC-H (Spark-RAPIDS + cudf-polars) AND
# SSB (Lancelot hybrid CPU/multi-GPU DBMS), in one image.
#
#   Spark 3.5.8 + RAPIDS 26.04.2 | cudf-polars 26.6 | OpenJDK 17
#   NDS-H (TPC-H) generator | Lancelot + SSB generator | CUDA 12.9 devel (nvcc)
#
# All three engines share one CUDA-12 base. They are run ONE AT A TIME (each
# saturates the GPU); the image just unifies build + invocation.
#
# Build:  docker build -t tpch-multitool .
# Run:    needs the NVIDIA Container Toolkit on the host (`--gpus all`).
##############################################################################

# ---- Stage 1: build TPC-H dbgen/qgen (NDS-H Spark profile; needs maven) ------
FROM ubuntu:22.04 AS tpch-gen-builder
ARG TPCH_KIT_REF=master
ARG BENCH_REF=main
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential gcc make git dos2unix ca-certificates \
        openjdk-17-jdk-headless maven \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 -b ${TPCH_KIT_REF} https://github.com/gregrahn/tpch-kit.git /opt/tpch-kit
RUN git clone --depth 1 -b ${BENCH_REF} https://github.com/NVIDIA/spark-rapids-benchmarks.git /opt/spark-rapids-benchmarks
ENV TPCH_HOME=/opt/tpch-kit
RUN cd /opt/spark-rapids-benchmarks/nds-h/tpch-gen && make \
    && test -x target/dbgen/dbgen && test -x target/dbgen/qgen

# ---- Stage 2: final multi-tool image (devel base for nvcc / Lancelot) --------
FROM nvidia/cuda:12.9.1-devel-ubuntu22.04
ARG SPARK_VERSION=3.5.8
ARG RAPIDS_VERSION=26.04.2
ARG CUDF_POLARS_VERSION=26.6.0
ARG LANCELOT_URL=https://gitfront.io/r/bwyogatama/X3t8gGuDH7fQ/Lancelot/
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless python3 python3-venv python3-pip \
        build-essential gcc g++ make git dos2unix \
        libtbb-dev libnccl2 libnccl-dev libpapi-dev \
        curl ca-certificates procps libgomp1 tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    CUDA_PATH=/usr/local/cuda \
    LIBRARY_PATH=/usr/local/cuda/lib64/stubs:/usr/local/cuda/extras/CUPTI/lib64 \
    LD_LIBRARY_PATH=/usr/local/cuda/extras/CUPTI/lib64

# --- Apache Spark + RAPIDS jar ------------------------------------------------
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      -o /tmp/spark.tgz \
    && mkdir -p /opt/spark && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}
RUN mkdir -p /opt/rapids \
    && curl -fsSL "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/${RAPIDS_VERSION}/rapids-4-spark_2.12-${RAPIDS_VERSION}-cuda12.jar" \
       -o /opt/rapids/rapids-4-spark.jar && test -s /opt/rapids/rapids-4-spark.jar
ENV RAPIDS_JAR=/opt/rapids/rapids-4-spark.jar

# --- Python venvs -------------------------------------------------------------
RUN python3 -m venv /opt/venv-spark \
    && /opt/venv-spark/bin/pip install --no-cache-dir "pyspark==${SPARK_VERSION}" pyarrow pandas numpy
RUN python3 -m venv /opt/venv-polars \
    && /opt/venv-polars/bin/pip install --no-cache-dir \
        --extra-index-url https://pypi.nvidia.com "cudf-polars-cu12==${CUDF_POLARS_VERSION}" pyarrow

# --- TPC-H generator (from builder) -------------------------------------------
COPY --from=tpch-gen-builder /opt/spark-rapids-benchmarks /opt/spark-rapids-benchmarks
ENV NDSH_DIR=/opt/spark-rapids-benchmarks/nds-h

# --- Lancelot source + prebuilt SSB data-gen tools ----------------------------
RUN git clone "${LANCELOT_URL}" /opt/lancelot \
    && cd /opt/lancelot \
    && make setup NVCC_VER=11.5 \
    && make -C test/ssb/dbgen 2>&1 | tail -3 \
    && make -C test/ssb/loader loader 2>&1 | tail -3 \
    && test -x test/ssb/dbgen/dbgen && test -x test/ssb/loader/loader

# --- App + unified scripts ----------------------------------------------------
COPY app/    /opt/app/
COPY scripts/ /opt/scripts/
RUN chmod +x /opt/scripts/entrypoint.sh /opt/scripts/tpch/*.sh /opt/scripts/lancelot/*.sh

# Defaults; override at run. SCALE = TPC-H scale; SF = SSB scale (1|10|20|40).
ENV DATA_DIR=/data \
    SCALE=10 \
    SF=1 \
    NUM_GPU=2 \
    SM_ARCH=120 \
    SM_DEF=-DSM700 \
    BASE_PATH=/data/ssb/ \
    LANCELOT_ROOT=/opt/lancelot \
    PYTHONUNBUFFERED=1
WORKDIR /opt
VOLUME ["/data"]
ENTRYPOINT ["/opt/scripts/entrypoint.sh"]
CMD ["help"]
