# TPC-H GPU benchmark in Docker — Polars + Spark-RAPIDS + NDS-H generator

A single image that encapsulates everything needed to generate TPC-H data and
produce the **GPU Polars** and **GPU Spark-RAPIDS** 22-query runtime table:

- **Apache Spark 3.5.8** + **RAPIDS Accelerator 26.04.2** (cuda12)
- **GPU Polars** via **cudf-polars-cu12 26.6** (RAPIDS)
- **NDS-H** data + query generator (NVIDIA `spark-rapids-benchmarks` + `tpch-kit`,
  built with the Spark profile so qgen emits `LIMIT` + `-- Template file:` markers)
- **OpenJDK 17**

Validated on 2× RTX 5090 (Blackwell, sm_120, CUDA 13 host driver).

## Requirements (host)

This image runs GPU work, so it must be built/run on a host with **Docker** and
the **NVIDIA Container Toolkit** (`--gpus all`). It cannot run inside an
unprivileged container (no Docker-in-Docker). The container brings its own CUDA
runtime; only the host **driver** is injected at run time.

## Quick start

```bash
cd docker
./run.sh build                      # docker build -t tpch-gpu .

# tiny self-test first: SF1, a few queries on both GPU engines, asserts OK
./run.sh smoke                      # ~1-2 min; exits non-zero if anything breaks

# end-to-end at scale factor 100: generate -> spark-rapids -> polars -> table
SCALE=100 ./run.sh all

# or step by step
SCALE=100 ./run.sh gen               # SF100 parquet + 22-query stream
SCALE=100 ./run.sh spark-rapids      # GPU Spark-RAPIDS run
SCALE=100 ./run.sh polars            # GPU Polars run
SCALE=100 ./run.sh table             # merge into the comparison table
```

Plain Docker (what `run.sh` wraps):

```bash
docker build -t tpch-gpu docker/
docker run --rm -it --gpus all --shm-size=64g -v "$PWD/data:/data" tpch-gpu smoke
docker run --rm -it --gpus all --shm-size=64g \
  -e SCALE=100 -v "$PWD/data:/data" tpch-gpu all
```

Or compose: `docker compose run --rm tpch smoke` · `SCALE=100 docker compose run --rm tpch all`.

**Ramdisk is the default:** the query runs stage the parquet into `/dev/shm` first,
so set `--shm-size` ≥ the dataset (~`SCALE`×0.36 GB; SF100 ≈ 36 GB, SF500 ≈ 180 GB,
e.g. `--shm-size=192g`). If it won't fit, the run prints a notice and reads from
disk instead. Disable with `-e STAGE_RAMDISK=0`.

The `smoke` target generates SF1, runs `SMOKE_QUERIES` (default `1 3 6`) on both
GPU engines, builds the table, and **exits non-zero unless every query is OK on
both** — a fast gate that the image, GPU, and both engines actually work before
committing to a large `SCALE`.

## Outputs

Everything lands under the mounted volume at `data/sf<SCALE>/`:

```
data/sf100/
  parquet/                         # 8 TPC-H tables
  queries/stream.sql               # Spark-compatible 22-query stream
  results/
    query_times_spark_gpu.csv      # per-query seconds, Spark-RAPIDS GPU
    query_times_polars_gpu.csv     # per-query seconds, Polars GPU
    gpu_comparison.md / .csv       # the merged comparison table
```

`gpu_comparison.md` is the deliverable table: per-query Polars-GPU vs
Spark-RAPIDS-GPU seconds (startup excluded), row counts, and a match flag.

## How the runs work

- **Startup excluded from timing.** Each query runs in a fresh process (one
  spark-submit / one python per query); a warm-up query absorbs first-touch GPU
  kernel JIT; only `collect()` is timed.
- **Single-GPU spill.** SF data far exceeds 32 GB, so both engines spill GPU
  partitions to **host memory** first:
  - Spark-RAPIDS: `spark.rapids.memory.host.spillStorageSize` (default 200G) +
    the RAPIDS shuffle manager.
  - Polars: cudf-polars **streaming executor**, async pool + rapidsmpf
    device→host spill. Any query that still OOMs (a single oversized groupby
    task) is **auto-retried on managed/UVM memory** so the table is always
    complete.
- **Disk watchdog.** A query is killed (and recorded) if free scratch disk drops
  below `MIN_FREE_GB` (default 30), so a runaway spill can't wedge the host.

## Configuration (env)

| var | default | meaning |
|-----|---------|---------|
| `SCALE` | 10 | TPC-H scale factor (GB) |
| `STAGE_RAMDISK` | **1** | stage parquet into `/dev/shm` (ramdisk) before running — **on by default**; needs `--shm-size` ≥ dataset (~`SCALE`×0.36 GB), else auto-falls back to disk. Set `0` to force disk. |
| `DRIVER_MEM` | 96g | Spark driver heap |
| `HOST_SPILL` | 200G | Spark-RAPIDS host spill store |
| `GPU_PART_MB` | 128 | Polars GPU IO partition size |
| `RAPIDSMPF_SPILL_DEVICE_LIMIT` | 22 GiB | Polars GPU device→host spill threshold |
| `MIN_FREE_GB` | 30 | disk watchdog floor |

Pin different versions at build time with `--build-arg SPARK_VERSION=…
RAPIDS_VERSION=… CUDF_POLARS_VERSION=…`.

## Notes / expectations

- A **single 32 GB GPU is the bottleneck at large SF**: queries that fit the GPU
  are very fast, but big joins that spill device→host over PCIe are slow on both
  engines (and on this box CPU Polars beats both — see `../results/POLARS_RESULTS.md`).
- Choose `SCALE` to fit your disk: raw text is generated in deletable batches,
  but the final parquet is roughly `SCALE × 0.36 GB` (e.g. SF500 ≈ 180 GB).
- To use **both** GPUs you'd run Spark standalone with 2 GPU executors, or
  cudf-polars `Cluster.SPMD` — not covered by this single-GPU image.

## Files

```
Dockerfile            multi-stage: gen-builder (dbgen/qgen) + CUDA runtime
app/run_spark_rapids.py   Spark-RAPIDS query runner (per-query, GPU-op count)
app/run_polars.py         Polars runner (CPU/GPU engines, GPU spill config)
app/tpch_queries.py       native-Polars TPC-H q1-22
app/make_table.py         merge the two result CSVs -> comparison table
scripts/                  entrypoint + gen/run/table orchestration
run.sh, docker-compose.yml host helpers (need --gpus all)
```
