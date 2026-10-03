# DuckDB CPU Baseline

`duckdb_cpu` — DuckDB as the CPU reference point against the GPU engines
(Sirius, Polars-GPU, Spark-RAPIDS). Same TPC-H parquet dataset,
same `results/queries/stream_qualification.sql`, same results contract as every
other engine in this repo (see `AGENTS.md`).

## Running

```bash
# all 22 queries at SF500, merged into results/all_results.csv
TPCH_SF=500 TPCH_PARQUET=/root/tpc-h/sf500_parquet ./duckdb/run_duckdb.sh

# a query subset
TPCH_SF=100 TPCH_PARQUET=/dev/shm/tpch_sf100/parquet ./duckdb/run_duckdb.sh "1 6 9"

# keep the temp CSV instead of merging
TPCH_MERGE=0 OUT_CSV=/tmp/duckdb.csv ./duckdb/run_duckdb.sh
```

The runner self-merges into `results/all_results.csv` via `merge_results.py` and
deletes its temp CSV — no per-run/per-SF CSVs, same as the other engines.

## Measurement protocol

Mirrors the local `test.py` flow this baseline came from.

- **Data:** every table is a view over `read_parquet()` of the ramdisk copy, and
  DuckDB's external file cache is off, so every run reads and decodes its parquet
  from the ramdisk, like the GPU engines. Nothing is loaded into memory ahead of
  a query (no in-memory tables; that mode was removed).
- **Timing** (default `WARMUPS=3 RUNS=1`): three prewarm passes over the query
  set, then one measured run each, all in one process — the `test.py` protocol.
  `RUNS>1` reports the median of the measured runs.

⚠️ **The process is warm.** The GPU engines start one process per query;
`duckdb_cpu` runs every query in one process after three prewarm passes (which
read the ramdisk too; no data stays cached). `RUNS=1 WARMUPS=0` gives a single
cold pass.

Rows in `all_results.csv` from before this change were measured on in-memory
tables (`tables` mode) and are not comparable to new runs.

Other env knobs: `DUCKDB_THREADS` (defaults to the process CPU affinity, not the
whole box), `DUCKDB_MEMORY_LIMIT`, `DUCKDB_TEMP_DIR` (spill location), `STREAM`.

## Output

One row per query, folded into `results/all_results.csv`:

```csv
engine,scale_factor,query,status,seconds,rows_or_error
duckdb_cpu,500,query1,OK,10.226,4
```

## Getting parquet

Only from the repo's generator (`datagen/`, see `results/GENERATOR.md`);
`run.sh` generates and validates missing datasets.
