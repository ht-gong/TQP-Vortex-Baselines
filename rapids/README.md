# spark-rapids (RAPIDS Accelerator for Apache Spark)

GPU-accelerated Apache Spark via the [NVIDIA RAPIDS Accelerator](https://github.com/NVIDIA/spark-rapids),
set up as a self-contained conda environment (no Docker, no system-wide installs).

## What's here

| Path | Purpose |
|------|---------|
| `env/` | Conda env — Python 3.11, **OpenJDK 17**, **pyspark 3.5.8** |
| `jars/rapids-4-spark_2.12-26.04.2-cuda12.jar` | The RAPIDS Accelerator plugin (Scala 2.12, CUDA 12 build, SHA1-verified) |
| `conf/spark-rapids.conf` | Local-mode Spark properties enabling the GPU plugin |
| `activate.sh` | `source` it to get a ready shell + `rapids-submit` helper |
| `test_gpu.py` | Smoke test that proves a query runs on the GPU |

## Versions / hardware

- **spark-rapids 26.04.2** (latest), Scala 2.12 → pairs with **Spark 3.3.x–3.5.x** (using 3.5.8).
- **cuda12** classifier: the jar bundles its own cuDF native library (incl. `sm_120` kernels), so
  it needs only an NVIDIA driver — runs on this box's CUDA 13 driver via backward compatibility.
- Verified on **2× RTX 5090** (Blackwell, compute capability 12.0), driver 580.82.09.

## Usage

```bash
source /workspace/baseline/rapids/activate.sh   # activates env, sets JAVA_HOME/SPARK_HOME/RAPIDS_JAR

# Run a job with the GPU plugin pre-wired (local[*] + jar + conf):
rapids-submit your_job.py

# Or the smoke test:
rapids-submit test_gpu.py
```

In your own `SparkSession` the three settings that matter are:

```python
.config("spark.jars", os.environ["RAPIDS_JAR"])
.config("spark.plugins", "com.nvidia.spark.SQLPlugin")
.config("spark.rapids.sql.enabled", "true")
```

Confirm GPU execution with `df.explain()` — operators should be prefixed `Gpu*`
(e.g. `GpuHashAggregate`). Set `spark.rapids.sql.explain=ALL` to see *why* any
operator falls back to CPU.

## Notes / gotchas

- **Vast `CONTAINER_ID`:** Spark otherwise thinks it's in a YARN container and dies with
  "Yarn Local dirs can't be empty". `activate.sh`'s `rapids-submit` runs Spark with
  `env -u CONTAINER_ID`; `test_gpu.py` pops it from `os.environ`. Do the same in your own jobs.
- **Persistence:** this lives under `/workspace`, which only survives recycle/destroy if the
  instance has a host volume (`vast-capabilities | jq '.instance.workspace_is_volume'`).
  The env + jar are reproducible from this README if not.
- **Upgrading the jar:** newer versions are at
  `https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/` — keep the Scala 2.12 /
  Spark-version pairing in mind, and a `-cuda13` classifier jar is also published if preferred.
