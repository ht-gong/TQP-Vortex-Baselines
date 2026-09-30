# AGENTS.md — TPC-H engine comparison

Harness that runs TPC-H q1–q22 across GPU/CPU query engines on the **same**
parquet dataset and collects per-query runtimes into `results/all_results.csv`.
Start with `README.md`; this file is the quick agent orientation and the
results-data contract.

## Running — one entry point: `make`

Every workflow runs in one pinned Docker image through `make`; each target calls
`docker/run.sh`, which mounts the repo, `DATA_DIR` and `SCRATCH` at their host
paths and runs the target's script inside. Box settings (`DATA_DIR`, `SCRATCH`,
`SHM`, `GPU`) come from the environment or `local.env` (copy
`local.env.example`).

```bash
make image && make doctor            # once per pin change; doctor checks GPU, io_uring, versions
make data SF=100                     # generate (if missing) + validate $DATA_DIR/sf100
make bench SF="100 300" [ENGINES="rapids sirius"] [QUERIES="1 6"]
make smoke                           # SF1, all engines, nothing merged; fails unless 22/22 everywhere
make ablation SF=100 ENCODINGS="plain dict delta" CODECS="snappy zstd lz4raw" [ROUNDS=3] [OUT=dir]
make format-map SF=100 [TOL=0.05]
make pth SF=1                        # TQP-Vortex .pth export
make summary                         # pivots of all_results.csv
make shell                           # interactive shell in the container
```

`make bench` runs `run.sh`: per scale factor it ensures the dataset
(`datagen/dataset.sh`), stages it disk → ramdisk, runs each engine against the
ramdisk copy, then frees the ramdisk. Engines self-merge into
`results/all_results.csv`. `SF` takes bare numbers or ranges over the canonical
set `{30,50,100,300,500,700}` (`30-300`); `ENGINES` defaults to
`rapids polars_gpu duckdb_cpu sirius`. Pass-through settings (`QUERIES`,
`KEEP_RAMDISK`, `GEN_PARALLEL`, `GEN_BATCH`, `DRIVER_MEM`, `GPU_PART_MB`,
`MIN_FREE_GB`, timeouts, …) are listed in `docker/run.sh`.

Runners read the tool paths the image sets — `PY`, `POLARS_PY`, `JAVA_HOME`,
`SPARK_HOME`, `RAPIDS_JAR`, `SIRIUS_DUCKDB`, `SIRIUS_ENVLIB`, `DBGEN_DIR` — and
`SCRATCH` (engine scratch: `$SCRATCH/rapids`, `$SCRATCH/polars`, `$SCRATCH/probe`).
Nothing in the scripts names a box-specific path.

## Engines & runners

| engine key | dir | runner |
|------------|-----|--------|
| `rapids` (Spark + RAPIDS, GPU) | `rapids/` | `run_tpch_safe.sh` |
| `polars_gpu` (cudf-polars, GPU) | `polars/` | `run_polars_gpu.sh` |
| `duckdb_cpu` (DuckDB, CPU) | `duckdb/` | `run_duckdb.sh` |
| `sirius` (Sirius GPU, DuckDB ext.) | `sirius/` | `run_sirius.sh` |

Engine settings shared by a runner and the format probes live in
`rapids/activate.sh` (`rapids_run_args`), `polars/env.sh`, `sirius/env.sh`.

`duckdb_cpu` is the **odd one out on protocol**: a single process loads the
dataset into in-memory tables once (load excluded from timing), then prewarms and
measures — not one cold process per query. Its `seconds` are therefore **warm**,
unlike every other engine's, and are a lower bound in any cross-engine comparison.
`RUNS=1 WARMUPS=0` gives cold, comparable numbers. Details in `duckdb/README.md`.

Runners read the same `results/queries/stream_qualification.sql`, take
`TPCH_PARQUET` / `TPCH_SF`, write a throwaway temp CSV, then fold their 22 rows
into `results/all_results.csv` via `merge_results.py <engine> <sf> <run_csv>` (an
idempotent upsert of the `(engine, scale_factor)` slice). No per-run CSVs are
kept; `TPCH_MERGE=0` keeps the temp instead of merging.

## Data — one generator for every engine

Every engine must run byte-identical parquet, from the in-repo generator
`datagen/gen_tpch.sh` (TPC-H dbgen → Spark transcode, writer `parquet-mr`). It is
a re-implementation of NVIDIA's NDS-H pipeline (provenance: `datagen/README.md`).
**Never** point a runner at self-written parquet — DuckDB's own `CALL dbgen`
export (16-col, no trailing `ignore`) or a cudf export — it is not comparable to
the external standard and gives different query answers.

`make data` / `make bench` generate missing datasets and validate
(`results/validate_dataset.py`: writer, `part.p_brand`, row counts); the pipeline
publishes `parquet/` only after it validates. Full rules and rationale:
`results/GENERATOR.md`.

The same data is TQP-Vortex's ground truth: `make pth SF=…` writes
`$DATA_DIR/pth/SF<SF>-tensor-<COL>.pth` (54 columns, TorchScript archives read by
TQP-Vortex's unmodified loader, rows in parquet order, plus a manifest); TQP-Vortex
uses them with `TQP_DATA_DIR=$DATA_DIR/pth`. Details in `datagen/README.md`.

## Results data contract — `results/all_results.csv`

**The single source of truth.** One row per `(engine, scale_factor, query)`.
Summary / matrices / per-engine scaling are all pivots of this — do NOT add
parallel summary CSVs; pivot this instead (`make summary`).

| column | type | values / meaning |
|--------|------|------------------|
| `engine` | string | `rapids` · `polars_gpu` · `duckdb_cpu` · `sirius`; legacy `polars_cpu` (retired Polars CPU engine; its SF500 rows remain) |
| `scale_factor` | int | TPC-H scale factor (≈ GB raw) |
| `query` | string | `query1` … `query22` |
| `status` | string | `OK` · `FAIL` (engine error; **GPU OOM** shows here with an "OOM retry limit" message in `rows_or_error`) · `KILLED_DISK` (disk-watchdog kill) · `TIMEOUT` (per-query timeout) |
| `seconds` | float | per-query wall-clock, engine startup excluded; time-to-failure on error; `NA` on a watchdog kill |
| `rows_or_error` | int / string | result **row count** when `OK`, else a short error string |

```csv
engine,scale_factor,query,status,seconds,rows_or_error
rapids,100,query1,OK,2.308,4
sirius,500,query9,FAIL,259.488,INTERNAL Error: ... GPU pipeline task exceeded maximum OOM retry limit (100) for
```

## Parquet-format profiling pass

A second experiment, separate from the engine comparison: which parquet format
(encoding × codec) each engine scans and decodes fastest, per column.
`make ablation` (`ablation/format_profile.py run`) builds one uniform variant
dataset per format (`$DATA_DIR/fmt_sf<SF>/shuffle-<enc>-<codec>/`, same rows,
from one dbgen run via `gen_tpch.sh`'s `RAW_STORE` / `TRANSCODE_OPTS`), then
times one `SELECT min(c), max(c)` probe per (engine, column, format, round), no
TPC-H: one worker per (engine, variant, round) probes every column in shuffled
order after untimed priming, engine caches off, CPU fallback recorded as
`FALLBACK`, hangs as `TIMEOUT` (worker restarted). It writes
`format_ablation_colsizes.csv` (keyed by `scale_factor, variant`),
`format_ablation_profile.csv`, `format_map.json` (per SF: the fastest eligible
format per engine and column; ties within `TOL` go to the smaller format) and
`format_map_sf<SF>.md`. `make format-map` re-runs only the selection.

The earlier 22-query SF100 ablation (12 variants incl. key-ordered rows) is
retired; its data and write-ups stay as results: `results/format_ablation.csv`
(keyed by `(engine, scale_factor, variant, round, query)`; do not fold it into
`all_results.csv`), `results/FORMAT_ABLATION.md`, `results/FORMAT_DECODING.md`,
`results/format_ablation_{report,columns}.md`,
`results/format_ablation_{colsizes,pagestats}.csv`. The scripts those write-ups
cite were removed.

## Notes for agents

- Result row counts (`rows_or_error` when `OK`) vary with scale factor but must
  **match across engines at the same SF** — a mismatch means a correctness bug.
  SF1 reference: q1–q22 = 4, 100, 10, 5, 5, 1, 4, 2, 175, 20, 29531, 2, 42, 1, 1,
  18314, 1, 57, 1, 186, 100, 7.
- Validate any dataset you did not just generate: `make validate SF=…`.
- Pins: `versions.env` + `docker/*.lock`. A bump is its own change and triggers
  re-runs; the image tag changes with it.
- Not committed (regenerate): parquet datasets, the image, `local.env`.
- GPUs are shared with other users: set `GPU` to an idle one; `make doctor`
  warns on a busy GPU and fails on an exclusive-mode one (Sirius fails at
  startup if one is visible). Inside the container the GPU is device 0.
- Container quirks, all handled by `docker/run.sh`: Docker's default seccomp
  profile blocks io_uring, and without it Sirius silently falls back to kvikio
  (`docker/seccomp-iouring.json`, `--ulimit memlock=-1`; `sirius/env.sh` probes
  io_uring and falls back to kvikio only where it is blocked). The container user
  gets a passwd entry (Java/Hadoop need a user name). `--ipc=host` makes
  `/dev/shm` the host's ramdisk. `KVIKIO_COMPAT_MODE=ON` stays set for
  polars_gpu/sirius since cuFile/GDS is unavailable. `run.sh` drops empty 0-row
  parquet part-files from the ramdisk copy (Sirius's GPU reader errors on them).
- On this box: `/` is nearly full, so datasets and scratch live on the NVMe
  (`DATA_DIR=/data/haotiang/parquet-ablation`); Docker's data root is on the NVMe too.
- `dpfproto/` (GOLAP/DPFProto notes, a git submodule) is separate and not wired
  into `make`.
