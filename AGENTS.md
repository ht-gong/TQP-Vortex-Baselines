# AGENTS.md — TPC-H engine comparison

Harness that runs TPC-H q1–q22 across GPU/CPU query engines on the **same**
parquet dataset and collects per-query runtimes into `results/all_results.csv`.
Start with `README.md`; this file is the quick agent orientation and the
results-data contract.

## Running — one entry point

```bash
./run.sh [SF_SPEC ...]
```

For each scale factor, `run.sh` ensures the dataset is on disk (generating it if
missing), validates it, stages it disk → ramdisk, runs each engine against the
ramdisk copy, then frees the ramdisk. Engines self-merge into
`results/all_results.csv`.

- `SF_SPEC` — scale factors: bare numbers (`100`) or an inclusive range over the
  canonical set `{30,50,100,300,500,700}` (`30-300`). Default `30 50 100 300 500 700`.
- `ENGINES` — space-separated, in run order. Default `rapids polars_gpu duckdb_cpu sirius`
  (also available: `polars_cpu`).
- `DATA_DIR` — on-disk datasets at `$DATA_DIR/sf<SF>/parquet/<table>/` (default `/data/haotiang/parquet-ablation`).
- `QUERIES`, `SHM`, `KEEP_RAMDISK`, `GEN_PARALLEL`, `GEN_BATCH`, and the usual
  runner passthroughs (`DRIVER_MEM`, `SPARK_SCRATCH`, `GPU_PART_MB`, `MIN_FREE_GB`).

Each engine's runner can also be invoked directly on a ramdisk dataset — see the
`run_engine` cases in `run.sh`.

## Engines & runners

| engine key | dir | runner |
|------------|-----|--------|
| `rapids` (Spark + RAPIDS, GPU) | `rapids/` | `run_tpch_safe.sh` |
| `polars_gpu` (cudf-polars, GPU) | `polars/` | `run_polars_gpu.sh` |
| `duckdb_cpu` (DuckDB, CPU) | `duckdb/` | `run_duckdb.sh` |
| `sirius` (Sirius GPU, DuckDB ext.) | `sirius/` | `run_sirius.sh` |
| `polars_cpu` (Polars streaming, CPU) | `polars/` | `run_polars.sh` |

`duckdb_cpu` is the **odd one out on protocol**: a single process loads the
dataset into in-memory tables once (load excluded from timing), then prewarms and
measures — not one cold process per query. Its `seconds` are therefore **warm**,
unlike every other engine's, and are a lower bound in any cross-engine comparison.
`RUNS=1 WARMUPS=0 ./duckdb/run_duckdb.sh` gives cold, comparable numbers. Details
in `duckdb/README.md`.

Runners read the same `results/queries/stream_qualification.sql`, take
`TPCH_PARQUET` / `TPCH_SF`, write a throwaway temp CSV, then fold their 22 rows
into `results/all_results.csv` via `merge_results.py <engine> <sf> <run_csv>` (an
idempotent upsert of the `(engine, scale_factor)` slice). No per-run CSVs are
kept; `TPCH_MERGE=0` keeps the temp instead of merging.

## Data — one generator for every engine

Every engine must run byte-identical parquet, from the in-repo generator
`datagen/gen_tpch.sh` (TPC-H dbgen → Spark transcode, writer `parquet-mr`). It is
a re-implementation of NVIDIA's NDS-H pipeline, verified equivalent to it
(`datagen/README.md`); `rapids/nds_h_pipeline.sh` remains only as the upstream
reference for that verification. **Never** point a runner at self-written
parquet — DuckDB's own `CALL dbgen` export (16-col, no trailing `ignore`) or a
cudf export — it is not comparable to the external standard and gives different
query answers.

```bash
datagen/setup_datagen.sh                                   # once: dbgen + pyspark/JDK venv
datagen/gen_tpch.sh <SF> <PARALLEL> <BATCH> <out_dir>      # -> <out_dir>/parquet/<table>/
python3 results/validate_dataset.py <parquet_dir> <SF>     # writer, part.p_brand, row counts
```

`run.sh` generates missing datasets and validates before every run; the pipeline
validates immediately after generation. Full rules and rationale:
`results/GENERATOR.md`. To re-verify the generator against upstream after
changing it: `datagen/verify_equivalence.py` (data) and `datagen/verify_engines.sh`
(engines), protocol in `datagen/README.md`.

## Results data contract — `results/all_results.csv`

**The single source of truth.** One row per `(engine, scale_factor, query)`.
Summary / matrices / per-engine scaling are all pivots of this — do NOT add
parallel summary CSVs; pivot this instead.

| column | type | values / meaning |
|--------|------|------------------|
| `engine` | string | `rapids` · `polars_gpu` · `duckdb_cpu` · `sirius` · `polars_cpu` |
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

Pivots:
```bash
duckdb -c "SELECT engine,scale_factor,count(*) FILTER(status='OK') ok,
  round(sum(seconds) FILTER(status='OK'),1) total_s
  FROM 'results/all_results.csv' GROUP BY 1,2 ORDER BY 1,2"          -- summary
duckdb -c "PIVOT 'results/all_results.csv'
  ON engine||'_sf'||scale_factor USING first(seconds) GROUP BY query"   -- matrix
```

## Parquet-format ablation (SF100)

A second experiment, separate from the engine comparison: the **same rows**
written in 12 parquet formats (encoding x codec, plus key-ordered rows) and
timed cold on every engine. `results/FORMAT_ABLATION.md` has the design and
the numbers; `results/format_ablation.csv` is its data (own table, keyed by
`(engine, scale_factor, variant, round, query)`; do not fold it into
`all_results.csv`). Tooling: `datagen/gen_format_variants.sh` (variants from one
dbgen run via `gen_tpch.sh` env `RAW_STORE` / `TRANSCODE_OPTS`),
`run_format_ablation.sh` (rounds; cold protocol incl. DuckDB `views` mode),
`merge_format_results.py`, `results/format_ablation_report.py`; per-column size
analysis: `results/format_ablation_colsizes.{py,csv}`, `results/format_ablation_columns.{py,md}`.
`results/FORMAT_DECODING.md` answers the follow-up "read less vs decode less"
questions (dictionary/delta page anatomy, predicate hit rates, zone maps, reader
microbenchmarks, what Sirius/Simpatico, DuckDB and cuDF do); its tooling is
`results/format_ablation_pagestats.{py,csv}` and `results/format_ablation_decode.py`.

## Notes for agents

- Result row counts (`rows_or_error` when `OK`) vary with scale factor but must
  **match across engines at the same SF** — a mismatch means a correctness bug.
- Validate any dataset you did not just generate: `results/validate_dataset.py`.
- Not committed (regenerate): the `sirius/sirius/` clone + `.pixi/` env, python
  venvs, the RAPIDS jar/conda env, and the parquet datasets (all gitignored).
  On this box the big engine installs live on the NVMe and are symlinked in:
  `sirius/sirius -> /data/haotiang/envs/sirius` (built with
  `SIRIUS_REPO=/data/haotiang/envs/sirius ./sirius/setup_sirius.sh`) and
  `polars/.venv-gpu -> /data/haotiang/envs/polars-gpu`; datasets are under
  `/data/haotiang/parquet-ablation`. `/` is nearly full — keep large things off it
  (`spark-rapids-benchmarks` and `_polars_scratch_gpu` are symlinks onto the NVMe
  for that reason; dbgen writes via `DSS_PATH` straight into the raw dir, and
  `SPARK_LOCAL_DIRS` / `SPARK_SCRATCH` should point at the NVMe for big runs).
- GPUs are shared with other users: pin a free one, e.g. `CUDA_VISIBLE_DEVICES=3`
  (GPUs 0–2 busy, 6–7 exclusive-mode at the time of writing). Sirius fails at
  startup if an exclusive-mode GPU is visible.
- Container quirks: `run_sirius.sh` probes `io_uring` at start-up and uses
  Sirius's native io_uring datasource where allowed (this box), falling back to
  the kvikio POSIX backend where the container blocks `io_uring_setup` (the old
  Vast box; `SIRIUS_IO=kvikio` forces it). `KVIKIO_COMPAT_MODE=ON` stays set for
  polars_gpu/sirius since cuFile/GDS is unavailable. `run.sh` drops empty 0-row
  parquet part-files before running (Sirius's GPU reader errors on them).
