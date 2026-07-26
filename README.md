# TPC-H engine comparison — Spark-RAPIDS · Polars-GPU · DuckDB · Sirius

A harness that runs TPC-H q1–q22 across several query engines on the **same**
parquet dataset and collects per-query runtimes into one results file. Everything
runs from a single entry point, `./run.sh`.

## Engines

| key | engine | dir |
|-----|--------|-----|
| `rapids` | Spark + RAPIDS Accelerator (GPU) | `rapids/` |
| `polars_gpu` | cudf-polars (GPU) | `polars/` |
| `duckdb_cpu` | DuckDB (CPU) | `duckdb/` |
| `sirius` | Sirius — GPU-native SQL, DuckDB extension | `sirius/` |
| `polars_cpu` | Polars streaming (CPU) — optional | `polars/` |

Default engine set: `rapids polars_gpu duckdb_cpu sirius`.

## Quick start

```bash
./run.sh                       # default engines, SF30..SF700
./run.sh 30-300                # SF30,50,100,300
ENGINES="duckdb_cpu" ./run.sh 500 700
```

`run.sh` is the one entry point. For each requested scale factor it: ensures the
dataset exists on disk (generating it with the NDS-H pipeline if missing),
validates it is clean external NDS-H, stages it disk → ramdisk, runs each
requested engine against the ramdisk copy, then frees the ramdisk. Each engine
folds its 22 rows into `results/all_results.csv`.

**Arguments** — `SF_SPEC`: scale factors to run, as bare numbers (`100`) or an
inclusive range over the canonical set `{30,50,100,300,500,700}` (`30-300`).
Default: `30 50 100 300 500 700`.

**Environment** —

| var | meaning |
|-----|---------|
| `ENGINES` | space-separated engines, in run order (default above) |
| `DATA_DIR` | on-disk datasets at `$DATA_DIR/sf<SF>/parquet/<table>/` (default `./data`) |
| `SHM` | ramdisk root (default `/dev/shm`) |
| `QUERIES` | query subset (default `1..22`) |
| `KEEP_RAMDISK=1` | keep the staged ramdisk copy after each SF |
| `GEN_PARALLEL` / `GEN_BATCH` | override NDS-H generation chunking |

`DRIVER_MEM`, `SPARK_SCRATCH`, `GPU_PART_MB`, `MIN_FREE_GB`, etc. pass through to
the individual runners.

## Data — one generator for every engine

All engines must read byte-identical parquet, and it must come from the **NDS-H**
generator (official TPC-H dbgen → Spark transcode, writer `parquet-mr`). Never
point a runner at self-written parquet (DuckDB's own `CALL dbgen` export, cudf
exports); it is not comparable to the external standard. Generate a dataset with:

```bash
rapids/nds_h_pipeline.sh <SF> <PARALLEL> <BATCH> <out_dir>   # -> <out_dir>/parquet/<table>/
python3 results/validate_dataset.py <parquet_dir> <SF>       # must print VALID
```

`run.sh` generates missing datasets automatically and validates before every run.
Full rules and rationale: **`results/GENERATOR.md`**.

## Layout

```
run.sh         single entry point (stage -> run engines -> free, per SF)
rapids/        Spark-RAPIDS runner + NDS-H generate/transcode pipeline
polars/        Polars CPU/GPU runners + native TPC-H q1-22
sirius/        Sirius runner + gpu_execution config + setup
duckdb/        DuckDB CPU runner
results/       all_results.csv, query stream (queries/), validator, GENERATOR.md
docker/        self-contained image: engines + NDS-H generator (see docker/README.md)
```

Not committed (regenerate / re-download): the parquet datasets, python venvs, the
RAPIDS jar, conda envs, and the upstream generator clones. See `docker/README.md`
to rebuild reproducibly.

## Results — `results/all_results.csv`

The single canonical results file (the only results CSV kept — everything else is
a throwaway). One row per `(engine, scale_factor, query)`. Each runner writes a
temp CSV and upserts its slice via `merge_results.py <engine> <sf> <run_csv>`;
every other view (summary, matrices, per-engine scaling) is a one-line pivot of
this file. Do **not** add parallel summary CSVs; pivot this instead.

| column | type | values / meaning |
|--------|------|------------------|
| `engine` | string | `rapids` · `polars_gpu` · `duckdb_cpu` · `sirius` · `polars_cpu` |
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

Handy pivots:
```bash
# per (engine, SF): completed count + total OK seconds
duckdb -c "SELECT engine,scale_factor,count(*) FILTER(status='OK') ok,
  round(sum(seconds) FILTER(status='OK'),1) total_s
  FROM 'results/all_results.csv' GROUP BY 1,2 ORDER BY 1,2"
# query × engine_sf seconds matrix
duckdb -c "PIVOT 'results/all_results.csv'
  ON engine||'_sf'||scale_factor USING first(seconds) GROUP BY query"
```
