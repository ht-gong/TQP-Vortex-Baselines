# TPC-H engine comparison — Spark-RAPIDS · Polars-GPU · DuckDB · Sirius

A harness that runs TPC-H q1–q22 across several query engines on the **same**
parquet dataset and collects per-query runtimes into one results file. Every
workflow — data generation, the benchmark, the parquet-format profiling pass,
the reports — runs in one pinned Docker image through one entry point, `make`.

## Setup

```bash
cp local.env.example local.env   # set DATA_DIR, SCRATCH, SHM and the GPU to use
make image                       # build the image for the pins in versions.env (~10 min, needs network)
make doctor                      # versions, GPU, io_uring, free space
```

Needs Docker with the NVIDIA Container Toolkit and an NVIDIA driver for CUDA 13.
The image holds the environments only; the repo is bind-mounted, so code
changes need no rebuild. The image tag is a hash of `versions.env`,
`docker/*.lock` and `docker/Dockerfile`. A container with a GPU runs on that
GPU's NUMA node only (its CPUs and memory; `NUMA=off` to unbind).

## Targets

| target | does |
|--------|------|
| `make image` / `make shell` | build the image / a shell in the container |
| `make doctor` | check versions against the pins, that the GPU is idle and not exclusive-mode, that the container is bound to the GPU's NUMA node, that io_uring works, free space |
| `make data SF=100` | generate `$DATA_DIR/sf100` if missing, then validate it |
| `make validate SF=100` | validate `$DATA_DIR/sf100` |
| `make bench SF=100 [ENGINES="…"]` | TPC-H q1–22 on every engine → `results/all_results.csv` |
| `make smoke` | SF1, all engines, nothing merged; prints a query × engine table, fails unless every query is OK |
| `make ablation [SF=5 ENCODINGS=… COMPRESSIONS=… ROUNDS=3 ENGINES=… TOL=0.05 OUT=results]` | parquet-format profiling pass → `format_profile.csv` (times) + `format_map.json` (decision); prints the results table |
| `make pth SF=1` | export `$DATA_DIR/sf1` as TQP-Vortex `.pth` column files in `$DATA_DIR/pth` |
| `make summary` | pivots of `results/all_results.csv` |

`SF` for `bench` is a list of bare numbers (`100 300`) or ranges over the
canonical set `{30,50,100,300,500,700}` (`30-300`). Runner settings pass through
the environment: `QUERIES`, `KEEP_RAMDISK=1`, `GEN_PARALLEL`/`GEN_BATCH`,
`DRIVER_MEM`, `GPU_PART_MB`, `MIN_FREE_GB`, `QUERY_TIMEOUT`, `SIRIUS_TIMEOUT`,
`PROBE_TIMEOUT`, … (the list is in `docker/run.sh`).

`make bench` (`run.sh`), for each scale factor: ensures the dataset exists and
validates, stages it disk → ramdisk, runs each engine against the ramdisk copy,
then frees the ramdisk. Each engine folds its 22 rows into
`results/all_results.csv`.

## Engines

| key | engine | dir |
|-----|--------|-----|
| `rapids` | Spark 3.5.8 + RAPIDS Accelerator 26.04.2 (GPU) | `rapids/` |
| `polars_gpu` | cudf-polars 26.6 / Polars 1.39.3 (GPU) | `polars/` |
| `duckdb_cpu` | DuckDB 1.5.5 (CPU) | `duckdb/` |
| `sirius` | Sirius (GPU-native SQL, DuckDB 1.5.5 extension, libcudf 26.08) | `sirius/` |

`duckdb_cpu` loads the dataset into memory once and times warm runs by default;
its `seconds` are a lower bound next to the other engines' cold runs
(`duckdb/README.md`).

## Data — one generator for every engine

All engines read byte-identical parquet from the repo's **NDS-H-equivalent**
generator in `datagen/` (TPC-H dbgen 2.17.3 → Spark 3.5.8 transcode, writer
`parquet-mr`; a re-implementation of NVIDIA's NDS-H pipeline). Never point a
runner at self-written parquet (DuckDB's own `CALL dbgen` export, cudf exports);
it is not comparable to the external standard. `make data` / `make bench`
generate and validate; full rules and rationale in **`results/GENERATOR.md`**.

The same data is the ground truth for TQP-Vortex: `make pth SF=…` writes it as
TQP-Vortex's `.pth` column files (`datagen/README.md`); point TQP-Vortex at them
with `TQP_DATA_DIR=$DATA_DIR/pth`.

## Results — `results/all_results.csv`

The single canonical TPC-H results file (the only TPC-H results CSV kept —
everything else is a throwaway; the format profiling pass keeps its own two
files, below). One row per `(engine, scale_factor, query)`. Each runner writes a
temp CSV and upserts its slice via `merge_results.py <engine> <sf> <run_csv>`;
every other view (summary, matrices, per-engine scaling) is a pivot of this file
(`make summary`). Do **not** add parallel summary CSVs; pivot this instead.

| column | type | values / meaning |
|--------|------|------------------|
| `engine` | string | `rapids` · `polars_gpu` · `duckdb_cpu` · `sirius` (legacy: `polars_cpu`, the retired Polars CPU engine, SF500 rows only) |
| `scale_factor` | int | TPC-H scale factor (≈ GB of raw data) |
| `query` | string | `query1` … `query22` |
| `status` | string | `OK` · `FAIL` (engine error; GPU out-of-memory shows here with an "OOM retry limit" message in `rows_or_error`) · `KILLED_DISK` (disk-watchdog kill) · `TIMEOUT` (per-query timeout) |
| `seconds` | float | per-query wall-clock, engine startup excluded; time-to-failure on error; `NA` on a watchdog kill |
| `rows_or_error` | int / string | result **row count** when `OK`, else a short error message |

Result row counts must **match across engines at the same scale factor** — a
mismatch means a correctness bug.

```csv
engine,scale_factor,query,status,seconds,rows_or_error
rapids,100,query1,OK,2.308,4
```

## Parquet-format profiling pass

`make ablation [SF=5 ENCODINGS=… COMPRESSIONS=…]` answers which parquet format
each engine scans and decodes fastest, per column: one uniform variant dataset
per encoding × compression (same rows,
`$DATA_DIR/fmt_sf<SF>/shuffle-<encoding>-<compression>`), one
`SELECT min(c), max(c)` probe per (engine, column, format, round), no TPC-H
queries. It writes two files to `OUT`: `format_profile.csv` (per engine, column
and encoding × compression: status, median seconds over rounds, compressed
bytes) and `format_map.json` (per engine and column: the chosen encoding and
compression). Schemas in `AGENTS.md` and `ablation/format_profile.py`. `SF`
defaults to 5.

## Versions

Every pin is in `versions.env` (base image digest, apt snapshot, JDK, dbgen,
RAPIDS jar, Sirius commit and build arch, pixi) and the hashed Python locks
`docker/py.lock` and `docker/polars.lock`. A version bump is its own change and
means re-running the affected results.

## Layout

```
Makefile       the entry point; every target runs through docker/run.sh
docker/        Dockerfile, run.sh (launcher), seccomp profile, doctor, locks
versions.env   every toolchain pin
run.sh         the benchmark driver (stage -> run engines -> free, per SF)
datagen/       the one TPC-H generator (dbgen -> Spark -> parquet) + .pth export
rapids/ polars/ duckdb/ sirius/   one runner per engine
ablation/      parquet-format profiling pass
results/       all_results.csv, query stream (queries/), validator, write-ups
dpfproto/      GOLAP / DPFProto baseline notes (separate; not wired into make)
```

Not committed (regenerate): the parquet datasets and the image.
